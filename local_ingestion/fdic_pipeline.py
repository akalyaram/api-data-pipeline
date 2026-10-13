"""Load FDIC BankFind institution records into a local SQLite database."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from sqlalchemy import Date, DateTime, Integer, String, Text, create_engine, func, select, text
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

DEFAULT_API_URL = "https://api.fdic.gov/banks/institutions"
DEFAULT_DATABASE_URL = "sqlite:///fdic.sqlite3"
DEFAULT_PAGE_SIZE = 1000
REQUEST_TIMEOUT_SECONDS = 30
LOGGER = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class FdicInstitution(Base):
    __tablename__ = "fdic_institutions"

    record_id: Mapped[str] = mapped_column(String, primary_key=True)
    cert: Mapped[int | None] = mapped_column(Integer, index=True)
    name: Mapped[str | None] = mapped_column(String, index=True)
    active: Mapped[int | None] = mapped_column(Integer)
    date_updated: Mapped[date | None] = mapped_column(Date, index=True)
    source_record: Mapped[str] = mapped_column(Text, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    mode: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    records_fetched: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    error_message: Mapped[str | None] = mapped_column(Text)


def _parse_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError(f"Expected a date string, got {type(value).__name__}")
    for date_format in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, date_format).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported FDIC date format: {value!r}")


def create_database(database_url: str) -> Engine:
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    return engine


def _get_last_updated_date(session: Session) -> date | None:
    return session.scalar(select(func.max(FdicInstitution.date_updated)))


def _fetch_records(
    http: requests.Session,
    api_url: str,
    page_size: int,
    updated_since: date | None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    offset = 0
    total: int | None = None

    while total is None or offset < total:
        params: dict[str, str | int] = {
            "format": "json",
            "limit": page_size,
            "offset": offset,
        }
        if updated_since is not None:
            params["filters"] = f"DATEUPDT:[{updated_since.isoformat()} TO *]"

        response = http.get(api_url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            raise ValueError("FDIC response must contain a data array")

        totals = payload.get("totals")
        if not isinstance(totals, dict) or not isinstance(totals.get("count"), int):
            raise ValueError("FDIC response must contain an integer totals.count")
        total = totals["count"]

        page = payload["data"]
        if not page and offset < total:
            raise ValueError(
                f"FDIC returned an empty page at offset {offset} with {total} records remaining"
            )
        for item in page:
            if not isinstance(item, dict) or not isinstance(item.get("data"), dict):
                raise ValueError("Each FDIC result must contain a data object")
            records.append(item["data"])
        offset += len(page)

    return records


def _upsert_records(session: Session, records: list[dict[str, Any]]) -> None:
    fetched_at = datetime.now(timezone.utc)
    for record in records:
        record_id = record.get("ID")
        if record_id is None or str(record_id) == "":
            raise ValueError("FDIC institution record is missing its ID")

        values = {
            "record_id": str(record_id),
            "cert": int(record["CERT"]) if record.get("CERT") not in (None, "") else None,
            "name": record.get("NAME"),
            "active": int(record["ACTIVE"]) if record.get("ACTIVE") not in (None, "") else None,
            "date_updated": _parse_date(record.get("DATEUPDT")),
            "source_record": json.dumps(record, separators=(",", ":"), sort_keys=True),
            "fetched_at": fetched_at,
        }
        statement = insert(FdicInstitution).values(**values)
        session.execute(
            statement.on_conflict_do_update(
                index_elements=[FdicInstitution.record_id],
                set_={key: value for key, value in values.items() if key != "record_id"},
            )
        )


def run_pipeline(
    mode: str,
    database_url: str = DEFAULT_DATABASE_URL,
    api_url: str = DEFAULT_API_URL,
    page_size: int = DEFAULT_PAGE_SIZE,
    overlap_days: int = 7,
) -> int:
    if mode not in {"historical", "incremental"}:
        raise ValueError("mode must be 'historical' or 'incremental'")
    if page_size < 1:
        raise ValueError("page_size must be a positive integer")
    if overlap_days < 0:
        raise ValueError("overlap_days cannot be negative")

    engine = create_database(database_url)
    with Session(engine) as session:
        updated_since = None
        if mode == "incremental":
            last_updated = _get_last_updated_date(session)
            if last_updated is not None:
                updated_since = last_updated - timedelta(days=overlap_days)

        run = PipelineRun(
            mode=mode,
            status="running",
            started_at=datetime.now(timezone.utc),
            records_fetched=0,
        )
        session.add(run)
        session.commit()

        try:
            with requests.Session() as http:
                records = _fetch_records(http, api_url, page_size, updated_since)
            _upsert_records(session, records)
            run.status = "succeeded"
            run.records_fetched = len(records)
            run.finished_at = datetime.now(timezone.utc)
            session.commit()
            LOGGER.info("%s run loaded %d FDIC records", mode, len(records))
            return len(records)
        except Exception as error:
            session.rollback()
            run = session.get(PipelineRun, run.run_id)
            if run is not None:
                run.status = "failed"
                run.finished_at = datetime.now(timezone.utc)
                run.error_message = str(error)
                session.commit()
            LOGGER.exception("%s FDIC pipeline run failed", mode)
            raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Load FDIC institution data into SQLite")
    parser.add_argument("mode", choices=("historical", "incremental"))
    parser.add_argument(
        "--database-url",
        default=os.environ.get("FDIC_DATABASE_URL", DEFAULT_DATABASE_URL),
        help=f"SQLAlchemy database URL (default: {DEFAULT_DATABASE_URL})",
    )
    parser.add_argument(
        "--api-url",
        default=os.environ.get("FDIC_API_URL", DEFAULT_API_URL),
        help=f"FDIC institutions endpoint (default: {DEFAULT_API_URL})",
    )
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--overlap-days", type=int, default=7)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_pipeline(
        args.mode,
        database_url=args.database_url,
        api_url=args.api_url,
        page_size=args.page_size,
        overlap_days=args.overlap_days,
    )


if __name__ == "__main__":
    main()
