import json
import logging
import os
import unittest
from datetime import date, datetime
from unittest.mock import Mock, patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

import fdic_pipeline
import handler
from fdic_pipeline import FdicInstitution, PipelineRun, _upsert_records, run_pipeline


class LambdaPipelineTests(unittest.TestCase):
    def setUp(self):
        handler._engine = None
        handler._engine_key = None

    def tearDown(self):
        if handler._engine is not None:
            handler._engine.dispose()
        handler._engine = None
        handler._engine_key = None

    def test_info_progress_logs_are_enabled(self):
        self.assertLessEqual(logging.getLogger().getEffectiveLevel(), logging.INFO)

    def test_postgresql_engine_generates_iam_token_for_new_connections(self):
        secret = {
            "host": "db.example.test",
            "port": 5432,
            "username": "postgres",
            "password": "legacy-password-must-not-be-used",
            "dbname": "banks",
        }
        fake_boto3 = Mock()
        secrets_client = Mock()
        secrets_client.get_secret_value.return_value = {
            "SecretString": json.dumps(secret)
        }
        rds_client = Mock()
        rds_client.generate_db_auth_token.return_value = "temporary-iam-token"
        fake_boto3.client.side_effect = [secrets_client, rds_client]
        engine = create_engine("sqlite:///:memory:")
        hooks = []
        original_listen = handler.event.listen

        def capture_hook(target, name, callback, **kwargs):
            hooks.append(callback)
            original_listen(target, name, callback, **kwargs)

        with (
            patch.dict("sys.modules", {"boto3": fake_boto3}),
            patch.object(handler, "create_engine", return_value=engine) as make_engine,
            patch.object(handler.event, "listen", side_effect=capture_hook),
        ):
            result = handler._get_engine("secret-arn", "us-east-2")

        self.assertIs(result, engine)
        fake_boto3.client.assert_any_call("secretsmanager", region_name="us-east-2")
        fake_boto3.client.assert_any_call("rds", region_name="us-east-2")
        url = make_engine.call_args.args[0]
        self.assertEqual(url.drivername, "postgresql+psycopg2")
        self.assertIsNone(url.password)
        self.assertEqual(url.database, "banks")
        self.assertEqual(make_engine.call_args.kwargs["connect_args"]["sslmode"], "require")
        self.assertEqual(make_engine.call_args.kwargs["pool_recycle"], 840)

        connect_parameters = {}
        hooks[0](None, None, [], connect_parameters)
        self.assertEqual(connect_parameters["password"], "temporary-iam-token")
        rds_client.generate_db_auth_token.assert_called_once_with(
            DBHostname="db.example.test",
            Port=5432,
            DBUsername="postgres",
        )

    def test_bad_secret_fails_with_missing_field_names(self):
        fake_boto3 = Mock()
        fake_boto3.client.return_value.get_secret_value.return_value = {
            "SecretString": '{"host":"db.example.test"}'
        }
        with patch.dict("sys.modules", {"boto3": fake_boto3}):
            with self.assertRaisesRegex(ValueError, "username, dbname"):
                handler._read_secret("secret-arn", "us-east-2")

    def test_secret_accepts_string_port_from_console_key_value(self):
        secret = {
            "host": "db.example.test",
            "port": "5432",
            "username": "fdic_user",
            "dbname": "postgres",
        }
        fake_boto3 = Mock()
        fake_boto3.client.return_value.get_secret_value.return_value = {
            "SecretString": json.dumps(secret)
        }
        with patch.dict("sys.modules", {"boto3": fake_boto3}):
            parsed = handler._read_secret("secret-arn", "us-east-2")

        self.assertEqual(parsed["port"], 5432)

    def test_handler_defaults_to_incremental_and_runs_pipeline(self):
        engine = create_engine("sqlite:///:memory:")
        with (
            patch.dict(
                os.environ,
                {"DB_SECRET_ARN": "secret-arn", "AWS_REGION": "us-east-2"},
                clear=False,
            ),
            patch.object(handler, "_get_engine", return_value=engine),
            patch.object(handler, "run_pipeline", return_value=12) as run,
        ):
            result = handler.lambda_handler({}, None)

        self.assertEqual(result, {"status": "succeeded", "mode": "incremental", "records_fetched": 12})
        self.assertEqual(run.call_args.args[0], "incremental")
        self.assertIs(run.call_args.kwargs["engine"], engine)

    def test_sqlite_pipeline_upserts_and_records_run(self):
        engine = create_engine("sqlite:///:memory:")
        response = Mock()
        response.json.return_value = {
            "data": [
                {
                    "data": {
                        "ID": "10",
                        "CERT": 10,
                        "NAME": "Lambda Test Bank",
                        "ACTIVE": 1,
                        "DATEUPDT": "10/04/1990",
                    }
                }
            ],
            "totals": {"count": 1},
        }
        http = Mock()
        http.__enter__ = Mock(return_value=http)
        http.__exit__ = Mock(return_value=None)
        http.get.return_value = response

        with (
            patch.object(fdic_pipeline.requests, "Session", return_value=http),
            self.assertLogs(fdic_pipeline.LOGGER, level="INFO") as captured_logs,
        ):
            count = run_pipeline("historical", engine=engine)

        with Session(engine) as session:
            institution = session.get(FdicInstitution, "10")
            pipeline_run = session.scalar(select(PipelineRun))

        self.assertEqual(count, 1)
        self.assertEqual(institution.name, "Lambda Test Bank")
        self.assertEqual(institution.date_updated, date(1990, 10, 4))
        self.assertEqual(pipeline_run.status, "succeeded")
        self.assertEqual(pipeline_run.records_fetched, 1)
        progress = "\n".join(captured_logs.output)
        self.assertIn("database schema ready", progress)
        self.assertIn("FDIC fetch complete: 1 institution records fetched", progress)
        self.assertIn("Writing institution batch 1 (1-1 of 1)", progress)
        self.assertIn("committing transaction", progress)
        self.assertIn("1 records committed", progress)

    def test_bulk_upsert_handles_multiple_batches_and_duplicate_ids(self):
        engine = create_engine("sqlite:///:memory:")
        fdic_pipeline.Base.metadata.create_all(engine)
        records = [
            {
                "ID": str(index),
                "CERT": index,
                "NAME": f"Bank {index}",
                "ACTIVE": 1,
                "DATEUPDT": "10/04/1990",
            }
            for index in range(fdic_pipeline.UPSERT_BATCH_SIZE + 1)
        ]
        records.append(
            {
                "ID": "0",
                "CERT": 0,
                "NAME": "Updated Bank 0",
                "ACTIVE": 1,
                "DATEUPDT": "10/05/1990",
            }
        )

        with Session(engine) as session:
            _upsert_records(session, records)
            session.commit()
            institutions = session.scalars(select(FdicInstitution)).all()
            updated = session.get(FdicInstitution, "0")

        self.assertEqual(len(institutions), fdic_pipeline.UPSERT_BATCH_SIZE + 1)
        self.assertEqual(updated.name, "Updated Bank 0")
        self.assertEqual(updated.date_updated, date(1990, 10, 5))


if __name__ == "__main__":
    unittest.main()
