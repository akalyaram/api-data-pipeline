CREATE TABLE IF NOT EXISTS fdic_institutions (
    record_id VARCHAR PRIMARY KEY,
    cert INTEGER,
    name VARCHAR,
    active INTEGER,
    date_updated DATE,
    source_record TEXT NOT NULL,
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_fdic_institutions_cert
    ON fdic_institutions (cert);
CREATE INDEX IF NOT EXISTS ix_fdic_institutions_name
    ON fdic_institutions (name);
CREATE INDEX IF NOT EXISTS ix_fdic_institutions_date_updated
    ON fdic_institutions (date_updated);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    mode VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE,
    records_fetched INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);
