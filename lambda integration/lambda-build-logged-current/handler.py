"""AWS Lambda entry point for loading FDIC institutions into RDS PostgreSQL."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from sqlalchemy import URL, create_engine, event
from sqlalchemy.engine import Engine

from fdic_pipeline import DEFAULT_API_URL, run_pipeline

LOGGER = logging.getLogger(__name__)
logging.getLogger().setLevel(logging.INFO)
_engine: Engine | None = None
_engine_key: tuple[str, str] | None = None


def _read_secret(secret_arn: str, region: str) -> dict[str, Any]:
    import boto3

    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_arn)
    secret_string = response.get("SecretString")
    if not isinstance(secret_string, str):
        raise ValueError("RDS secret must contain a SecretString with JSON credentials")

    try:
        secret = json.loads(secret_string)
    except json.JSONDecodeError as error:
        raise ValueError("RDS SecretString must be valid JSON") from error
    if not isinstance(secret, dict):
        raise ValueError("RDS SecretString JSON must be an object")

    required = ("host", "username", "dbname")
    missing = [key for key in required if not isinstance(secret.get(key), str) or not secret[key]]
    if missing:
        raise ValueError(f"RDS secret is missing required field(s): {', '.join(missing)}")
    raw_port = secret.get("port", 5432)
    if isinstance(raw_port, str):
        try:
            port = int(raw_port.strip())
        except ValueError as error:
            raise ValueError(
                "RDS secret port must be an integer between 1 and 65535"
            ) from error
    elif isinstance(raw_port, int) and not isinstance(raw_port, bool):
        port = raw_port
    else:
        raise ValueError("RDS secret port must be an integer between 1 and 65535")
    if not 1 <= port <= 65535:
        raise ValueError("RDS secret port must be an integer between 1 and 65535")
    return {
        "host": secret["host"],
        "port": port,
        "username": secret["username"],
        "dbname": secret["dbname"],
    }


def _get_engine(secret_arn: str, region: str) -> Engine:
    global _engine, _engine_key

    engine_key = (secret_arn, region)
    if _engine is not None and _engine_key == engine_key:
        return _engine

    secret = _read_secret(secret_arn, region)
    import boto3

    rds_client = boto3.client("rds", region_name=region)
    url = URL.create(
        "postgresql+psycopg2",
        username=secret["username"],
        host=secret["host"],
        port=secret["port"],
        database=secret["dbname"],
    )
    _engine = create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=840,
        pool_size=1,
        max_overflow=0,
        connect_args={"sslmode": "require", "connect_timeout": 10},
    )

    def add_iam_auth_token(
        dialect: Any,
        connection_record: Any,
        cargs: list[Any],
        cparams: dict[str, Any],
    ) -> None:
        del dialect, connection_record, cargs
        cparams["password"] = rds_client.generate_db_auth_token(
            DBHostname=secret["host"],
            Port=secret["port"],
            DBUsername=secret["username"],
        )

    event.listen(_engine, "do_connect", add_iam_auth_token)
    _engine_key = engine_key
    return _engine


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Run one historical or incremental load; incremental is the default."""
    del context
    if not isinstance(event, dict):
        raise ValueError("Lambda event must be a JSON object")

    mode = event.get("mode", "incremental")
    if mode not in {"historical", "incremental"}:
        raise ValueError("event.mode must be 'historical' or 'incremental'")

    secret_arn = os.environ.get("DB_SECRET_ARN")
    region = os.environ.get("AWS_REGION")
    if not secret_arn:
        raise RuntimeError("Lambda environment variable DB_SECRET_ARN is required")
    if not region:
        raise RuntimeError("Lambda environment variable AWS_REGION is required")

    api_url = os.environ.get("FDIC_API_URL", DEFAULT_API_URL)
    page_size = int(os.environ.get("FDIC_PAGE_SIZE", "1000"))
    overlap_days = int(os.environ.get("FDIC_OVERLAP_DAYS", "7"))

    LOGGER.info("Lambda invocation accepted: mode=%s", mode)
    LOGGER.info("Loading database connection metadata from Secrets Manager")
    engine = _get_engine(secret_arn, region)
    LOGGER.info("Database engine initialized; handing off to pipeline")
    count = run_pipeline(
        mode,
        engine=engine,
        api_url=api_url,
        page_size=page_size,
        overlap_days=overlap_days,
    )
    LOGGER.info("Lambda invocation complete: mode=%s records_fetched=%d", mode, count)
    return {"status": "succeeded", "mode": mode, "records_fetched": count}
