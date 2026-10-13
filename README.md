# api-data-pipeline

## Local FDIC institutions pipeline

This project loads the FDIC BankFind Suite **institutions** endpoint
(`https://api.fdic.gov/banks/institutions`) into a local SQLite database using
SQLAlchemy. The complete source record is retained as JSON in
`fdic_institutions.source_record`; commonly queried fields and the source
update date are also available as columns. `schema.sql` contains the equivalent
SQLite DDL, and the pipeline creates these tables automatically.

### Setup and run

Use Python 3.10 or newer:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python fdic_pipeline.py historical
python fdic_pipeline.py incremental
```

The default database is `fdic.sqlite3` in the current directory. An initial
`historical` run retrieves all records currently published by the endpoint.
`incremental` filters on the maximum stored `DATEUPDT`, re-fetches a seven-day
overlap to pick up boundary changes, and upserts on the FDIC record `ID`.
Repeating a historical run refreshes/upserts all published records; neither
mode deletes records that disappear from the API. Incremental mode on an empty
database performs a full load.

Override settings with `--database-url`, `--api-url`, `--page-size`, and
`--overlap-days`, or set `FDIC_DATABASE_URL` / `FDIC_API_URL`. The pipeline,
DDL, and conflict-safe upsert currently target SQLite.

Each attempt is tracked in `pipeline_runs`, including failed runs. To run the
tests, use `python -m unittest`.