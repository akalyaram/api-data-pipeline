"""Load FDIC BankFind institution records into a configured SQLAlchemy database."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from sqlalchemy import Date, DateTime, Integer, String, Text, create_engine, func, select, text
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

DEFAULT_API_URL = "https://api.fdic.gov/banks/institutions"
DEFAULT_PAGE_SIZE = 1000
UPSERT_BATCH_SIZE = 500
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
    page_number = 0

    while total is None or offset < total:
        page_number += 1
        params: dict[str, str | int] = {
            "format": "json",
            "limit": page_size,
            "offset": offset,
        }
        if updated_since is not None:
            params["filters"] = f"DATEUPDT:[{updated_since.isoformat()} TO *]"

        LOGGER.info(
            "Fetching FDIC page %d (offset=%d, page_size=%d)",
            page_number,
            offset,
            page_size,
        )
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
        LOGGER.info(
            "Fetched FDIC page %d: %d records; total reported=%d; accumulated=%d",
            page_number,
            len(page),
            total,
            len(records),
        )

    LOGGER.info("FDIC fetch complete: %d institution records fetched", len(records))
    return records


def _upsert_records(session: Session, records: list[dict[str, Any]]) -> None:
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        insert_record = postgresql_insert
    elif dialect == "sqlite":
        insert_record = sqlite_insert
    else:
        raise ValueError(f"Unsupported database dialect for upsert: {dialect}")

    fetched_at = datetime.now(timezone.utc)
    rows: dict[str, dict[str, Any]] = {}
    for record in records:
        record_id = record.get("ID")
        if record_id is None or str(record_id) == "":
            raise ValueError("FDIC institution record is missing its ID")

        normalized_id = str(record_id)
        rows[normalized_id] = {
            "record_id": normalized_id,
            "cert": int(record["CERT"]) if record.get("CERT") not in (None, "") else None,
            "name": record.get("NAME"),
            "active": int(record["ACTIVE"]) if record.get("ACTIVE") not in (None, "") else None,
            "date_updated": _parse_date(record.get("DATEUPDT")),
            "source_record": json.dumps(record, separators=(",", ":"), sort_keys=True),
            "fetched_at": fetched_at,
        }

    row_values = list(rows.values())
    LOGGER.info(
        "Prepared %d unique institution records for batched upsert",
        len(row_values),
    )
    for offset in range(0, len(row_values), UPSERT_BATCH_SIZE):
        batch = row_values[offset : offset + UPSERT_BATCH_SIZE]
        LOGGER.info(
            "Writing institution batch %d (%d-%d of %d)",
            offset // UPSERT_BATCH_SIZE + 1,
            offset + 1,
            offset + len(batch),
            len(row_values),
        )
        statement = insert_record(FdicInstitution).values(batch)
        session.execute(
            statement.on_conflict_do_update(
                index_elements=[FdicInstitution.record_id],
                set_={
                    "cert": statement.excluded.cert,
                    "name": statement.excluded.name,
                    "active": statement.excluded.active,
                    "date_updated": statement.excluded.date_updated,
                    "source_record": statement.excluded.source_record,
                    "fetched_at": statement.excluded.fetched_at,
                },
            )
        )
        LOGGER.info(
            "Institution batch executed: %d of %d records (pending transaction commit)",
            min(offset + len(batch), len(row_values)),
            len(row_values),
        )


def run_pipeline(
    mode: str,
    engine: Engine,
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

    LOGGER.info("%s run: initializing database schema", mode)
    Base.metadata.create_all(engine)
    LOGGER.info("%s run: database schema ready", mode)
    with Session(engine) as session:
        updated_since = None
        if mode == "incremental":
            last_updated = _get_last_updated_date(session)
            if last_updated is not None:
                updated_since = last_updated - timedelta(days=overlap_days)
                LOGGER.info(
                    "Incremental checkpoint=%s; querying from %s (overlap=%d days)",
                    last_updated.isoformat(),
                    updated_since.isoformat(),
                    overlap_days,
                )
            else:
                LOGGER.info("No stored update date; incremental mode will fetch all records")
        else:
            LOGGER.info("Historical mode will fetch all currently published records")

        run = PipelineRun(
            mode=mode,
            status="running",
            started_at=datetime.now(timezone.utc),
            records_fetched=0,
        )
        session.add(run)
        session.commit()
        LOGGER.info("Created pipeline run %d with status=running", run.run_id)

        phase = "fetch"
        try:
            with requests.Session() as http:
                records = _fetch_records(http, api_url, page_size, updated_since)
            LOGGER.info(
                "%s run: fetch phase complete (%d records); starting database upsert",
                mode,
                len(records),
            )
            phase = "upsert"
            _upsert_records(session, records)
            LOGGER.info(
                "%s run: all upsert batches executed; committing transaction",
                mode,
            )
            phase = "commit"
            run.status = "succeeded"
            run.records_fetched = len(records)
            run.finished_at = datetime.now(timezone.utc)
            session.commit()
            LOGGER.info(
                "%s run %d completed successfully; %d records committed",
                mode,
                run.run_id,
                len(records),
            )
            return len(records)
        except Exception as error:
            LOGGER.exception(
                "%s run %d failed during %s phase",
                mode,
                run.run_id,
                phase,
            )
            session.rollback()
            run = session.get(PipelineRun, run.run_id)
            if run is not None:
                run.status = "failed"
                run.finished_at = datetime.now(timezone.utc)
                run.error_message = str(error)
                session.commit()
                LOGGER.error(
                    "%s run %d marked failed; error recorded in pipeline_runs",
                    mode,
                    run.run_id,
                )
            raise
