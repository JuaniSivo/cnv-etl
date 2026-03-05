-- cnv_etl SQLite schema
-- All tables use INSERT OR REPLACE semantics for upserts.
-- Run once at DB initialisation; safe to re-run (IF NOT EXISTS guards).

PRAGMA journal_mode = WAL;   -- better write concurrency
PRAGMA foreign_keys = ON;


-- ---------------------------------------------------------------------------
-- companies
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS companies (
    id                        INTEGER PRIMARY KEY,   -- CNV CUIT
    name                      TEXT    NOT NULL,
    ticker                    TEXT    NOT NULL UNIQUE,
    description               TEXT,
    sector                    TEXT,
    industry_group            TEXT,
    industry                  TEXT,
    sub_industry              TEXT,
    sub_industry_description  TEXT
);


-- ---------------------------------------------------------------------------
-- statement_metadata
-- One row per financial statement (document).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS statement_metadata (
    document_id                            INTEGER PRIMARY KEY,
    company_id                             INTEGER NOT NULL REFERENCES companies(id),
    document_description                   TEXT    NOT NULL,
    document_link                          TEXT    NOT NULL,
    submission_date                        TEXT    NOT NULL,   -- ISO-8601 datetime
    period_end_date                        TEXT    NOT NULL,   -- ISO-8601 date
    reporting_period                       TEXT    NOT NULL,   -- Annual | Semester | Quarter | Irregular
    financial_statements_type              TEXT    NOT NULL,   -- Separate | Consolidated
    presentation_currency                  TEXT    NOT NULL,
    accounting_standards_applied           TEXT    NOT NULL,
    merger_or_demerger_in_process          INTEGER NOT NULL,   -- 0 / 1
    treasury_shares                        INTEGER NOT NULL,
    insolvency_proceedings                 INTEGER NOT NULL,
    public_offering_of_equity_instruments  INTEGER NOT NULL,
    subscribed_shares_at_period_end        INTEGER,
    equity_share_price_at_period_end       REAL,
    free_float_percentage                  REAL,
    number_of_employees                    INTEGER
);

CREATE INDEX IF NOT EXISTS idx_stmt_company
    ON statement_metadata(company_id);

CREATE INDEX IF NOT EXISTS idx_stmt_period
    ON statement_metadata(period_end_date);


-- ---------------------------------------------------------------------------
-- statement_values
-- One row per (document, concept) pair.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS statement_values (
    document_id  INTEGER NOT NULL REFERENCES statement_metadata(document_id),
    order_index  INTEGER NOT NULL,
    concept      TEXT    NOT NULL,
    value        REAL    NOT NULL,
    PRIMARY KEY (document_id, concept)
);

CREATE INDEX IF NOT EXISTS idx_values_document
    ON statement_values(document_id);


-- ---------------------------------------------------------------------------
-- pipeline_errors
-- One row per ETLError recorded during any pipeline run.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline_errors (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    run_started_at        TEXT    NOT NULL,   -- ISO-8601 datetime of the run
    stage                 TEXT    NOT NULL,   -- fetch | download | transform | load
    ticker                TEXT    NOT NULL,
    error_type            TEXT    NOT NULL,
    message               TEXT    NOT NULL,
    document_description  TEXT,
    timestamp             TEXT    NOT NULL    -- ISO-8601 datetime of the error
);

CREATE INDEX IF NOT EXISTS idx_errors_run
    ON pipeline_errors(run_started_at);

CREATE INDEX IF NOT EXISTS idx_errors_ticker
    ON pipeline_errors(ticker);