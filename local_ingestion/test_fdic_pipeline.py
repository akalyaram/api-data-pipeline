import unittest
from datetime import date, datetime
from unittest.mock import Mock, patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

import fdic_pipeline
from fdic_pipeline import (
    Base,
    FdicInstitution,
    PipelineRun,
    _fetch_records,
    _parse_date,
    run_pipeline,
)


class FdicPipelineTests(unittest.TestCase):
    def test_parse_date_accepts_fdic_and_iso_formats(self):
        self.assertEqual(_parse_date("10/04/1990"), date(1990, 10, 4))
        self.assertEqual(_parse_date("1990-10-04"), date(1990, 10, 4))
        self.assertIsNone(_parse_date(None))

    def test_fetch_records_paginates_and_applies_incremental_filter(self):
        first = Mock()
        first.json.return_value = {
            "data": [{"data": {"ID": "1"}}],
            "totals": {"count": 2},
        }
        second = Mock()
        second.json.return_value = {
            "data": [{"data": {"ID": "2"}}],
            "totals": {"count": 2},
        }
        http = Mock()
        http.get.side_effect = [first, second]

        records = _fetch_records(http, "https://example.test/institutions", 1, date(2026, 1, 2))

        self.assertEqual(records, [{"ID": "1"}, {"ID": "2"}])
        self.assertEqual(http.get.call_count, 2)
        self.assertEqual(
            http.get.call_args_list[0].kwargs["params"]["filters"],
            "DATEUPDT:[2026-01-02 TO *]",
        )
        self.assertEqual(http.get.call_args_list[1].kwargs["params"]["offset"], 1)

    def test_historical_run_upserts_records_and_records_run(self):
        engine = create_engine("sqlite:///:memory:")

        class FakeHttp:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def get(self, *args, **kwargs):
                response = Mock()
                response.json.return_value = {
                    "data": [
                        {
                            "data": {
                                "ID": "10",
                                "CERT": 10,
                                "NAME": "Example Bank",
                                "ACTIVE": 1,
                                "DATEUPDT": "10/04/1990",
                            }
                        }
                    ],
                    "totals": {"count": 1},
                }
                return response

        Base.metadata.create_all(engine)

        with (
            patch.object(fdic_pipeline, "create_database", return_value=engine),
            patch.object(fdic_pipeline.requests, "Session", FakeHttp),
        ):
            count = run_pipeline("historical", database_url="sqlite:///:memory:")

        with Session(engine) as session:
            institution = session.get(FdicInstitution, "10")
            pipeline_run = session.scalar(select(PipelineRun))

        self.assertEqual(count, 1)
        self.assertEqual(institution.name, "Example Bank")
        self.assertEqual(institution.date_updated, date(1990, 10, 4))
        self.assertEqual(pipeline_run.status, "succeeded")
        self.assertEqual(pipeline_run.records_fetched, 1)

    def test_incremental_run_uses_overlap_and_updates_existing_record(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        with Session(engine) as session:
            session.add(
                FdicInstitution(
                    record_id="10",
                    cert=10,
                    name="Old Name",
                    active=1,
                    date_updated=date(2026, 1, 10),
                    source_record='{"ID":"10"}',
                    fetched_at=datetime(2026, 1, 10),
                )
            )
            session.commit()

        response = Mock()
        response.json.return_value = {
            "data": [
                {
                    "data": {
                        "ID": "10",
                        "CERT": 10,
                        "NAME": "Updated Name",
                        "ACTIVE": 1,
                        "DATEUPDT": "01/12/2026",
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
            patch.object(fdic_pipeline, "create_database", return_value=engine),
            patch.object(fdic_pipeline.requests, "Session", return_value=http),
        ):
            count = run_pipeline("incremental", database_url="sqlite:///:memory:")

        with Session(engine) as session:
            institution = session.get(FdicInstitution, "10")

        self.assertEqual(count, 1)
        self.assertEqual(institution.name, "Updated Name")
        self.assertEqual(institution.date_updated, date(2026, 1, 12))
        self.assertEqual(
            http.get.call_args.kwargs["params"]["filters"],
            "DATEUPDT:[2026-01-03 TO *]",
        )


if __name__ == "__main__":
    unittest.main()
