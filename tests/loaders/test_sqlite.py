"""
Integration tests for cnv_etl.loaders.sqlite.SQLiteLoader.

Uses a real SQLite DB in pytest's tmp_path — no mocking needed.
"""

import sqlite3
import pytest
from datetime import date, datetime
from pathlib import Path

from cnv_etl.loaders.sqlite import SQLiteLoader
from cnv_etl.models.document import CleanConceptValue
from cnv_etl.errors import ETLError, PipelineReport


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row_count(db_path: Path, table: str) -> int:
    conn = sqlite3.connect(db_path)
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    conn.close()
    return count


def _fetch_one(db_path: Path, table: str, where: str, params: tuple):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(f"SELECT * FROM {table} WHERE {where}", params).fetchone()
    conn.close()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

class TestSchema:

    def test_all_tables_created(self, tmp_db_path):
        with SQLiteLoader(tmp_db_path):
            pass  # just opening should apply schema

        conn = sqlite3.connect(tmp_db_path)
        tables = {
            row[0] for row in
            conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        conn.close()

        assert "companies"          in tables
        assert "statement_metadata" in tables
        assert "statement_values"   in tables
        assert "pipeline_errors"    in tables

    def test_schema_safe_to_apply_twice(self, tmp_db_path):
        """IF NOT EXISTS guards mean opening the DB twice should not raise."""
        with SQLiteLoader(tmp_db_path):
            pass
        with SQLiteLoader(tmp_db_path):
            pass


# ---------------------------------------------------------------------------
# upsert_company
# ---------------------------------------------------------------------------

class TestUpsertCompany:

    def test_company_inserted(self, tmp_db_path, sample_company):
        with SQLiteLoader(tmp_db_path) as db:
            db.upsert_company(sample_company)

        row = _fetch_one(tmp_db_path, "companies", "ticker = ?", ("TEST",))
        assert row is not None
        assert row["name"]   == "Test Company SA"
        assert row["id"]     == 30123456789
        assert row["ticker"] == "TEST"

    def test_overwrite_replaces_existing(self, tmp_db_path, sample_company):
        with SQLiteLoader(tmp_db_path, overwrite=True) as db:
            db.upsert_company(sample_company)
            sample_company.name = "Updated Name SA"
            db.upsert_company(sample_company)

        row = _fetch_one(tmp_db_path, "companies", "ticker = ?", ("TEST",))
        assert row["name"] == "Updated Name SA"

    def test_ignore_mode_keeps_existing(self, tmp_db_path, sample_company):
        with SQLiteLoader(tmp_db_path, overwrite=False) as db:
            db.upsert_company(sample_company)
            sample_company.name = "Should Not Update"
            db.upsert_company(sample_company)

        row = _fetch_one(tmp_db_path, "companies", "ticker = ?", ("TEST",))
        assert row["name"] == "Test Company SA"


# ---------------------------------------------------------------------------
# upsert_statement
# ---------------------------------------------------------------------------

class TestUpsertStatement:

    def test_metadata_and_values_inserted(self, tmp_db_path, sample_company, sample_statement):
        with SQLiteLoader(tmp_db_path) as db:
            db.upsert_company(sample_company)
            db.upsert_statement(sample_company.id, sample_statement)

        assert _row_count(tmp_db_path, "statement_metadata") == 1
        assert _row_count(tmp_db_path, "statement_values")   == 3  # Assets, Liabilities, Equity

    def test_metadata_stored_correctly(self, tmp_db_path, sample_company, sample_statement):
        with SQLiteLoader(tmp_db_path) as db:
            db.upsert_company(sample_company)
            db.upsert_statement(sample_company.id, sample_statement)

        row = _fetch_one(
            tmp_db_path, "statement_metadata",
            "document_id = ?", (sample_statement.document_id,)
        )
        assert row["reporting_period"]          == "Annual"
        assert row["financial_statements_type"] == "Consolidated"
        assert row["period_end_date"]           == "2024-12-31"
        assert row["merger_or_demerger_in_process"] == 0
        assert row["public_offering_of_equity_instruments"] == 1

    def test_values_stored_correctly(self, tmp_db_path, sample_company, sample_statement):
        with SQLiteLoader(tmp_db_path) as db:
            db.upsert_company(sample_company)
            db.upsert_statement(sample_company.id, sample_statement)

        row = _fetch_one(
            tmp_db_path, "statement_values",
            "document_id = ? AND concept = ?",
            (sample_statement.document_id, "Assets")
        )
        assert row is not None
        assert row["value"]       == 1_000_000.0
        assert row["order_index"] == 1

    def test_overwrite_replaces_values(self, tmp_db_path, sample_company, sample_statement):
        with SQLiteLoader(tmp_db_path, overwrite=True) as db:
            db.upsert_company(sample_company)
            db.upsert_statement(sample_company.id, sample_statement)

            # Modify a concept value and upsert again
            sample_statement.statement["Assets"] = CleanConceptValue(
                label="Assets", value=9_999_999.0, order=1
            )
            db.upsert_statement(sample_company.id, sample_statement)

        row = _fetch_one(
            tmp_db_path, "statement_values",
            "document_id = ? AND concept = ?",
            (sample_statement.document_id, "Assets")
        )
        assert row["value"] == 9_999_999.0

    def test_ignore_mode_keeps_original_values(self, tmp_db_path, sample_company, sample_statement):
        with SQLiteLoader(tmp_db_path, overwrite=False) as db:
            db.upsert_company(sample_company)
            db.upsert_statement(sample_company.id, sample_statement)

            sample_statement.statement["Assets"] = CleanConceptValue(
                label="Assets", value=9_999_999.0, order=1
            )
            db.upsert_statement(sample_company.id, sample_statement)

        row = _fetch_one(
            tmp_db_path, "statement_values",
            "document_id = ? AND concept = ?",
            (sample_statement.document_id, "Assets")
        )
        assert row["value"] == 1_000_000.0


# ---------------------------------------------------------------------------
# get_existing_document_ids
# ---------------------------------------------------------------------------

class TestGetExistingDocumentIds:

    def test_returns_empty_set_for_new_company(self, tmp_db_path, sample_company):
        with SQLiteLoader(tmp_db_path) as db:
            db.upsert_company(sample_company)
            ids = db.get_existing_document_ids(sample_company.id)
        assert ids == set()

    def test_returns_stored_document_ids(self, tmp_db_path, sample_company, sample_statement):
        with SQLiteLoader(tmp_db_path) as db:
            db.upsert_company(sample_company)
            db.upsert_statement(sample_company.id, sample_statement)
            ids = db.get_existing_document_ids(sample_company.id)

        assert ids == {sample_statement.document_id}

    def test_does_not_return_other_company_ids(
        self, tmp_db_path, sample_company, sample_statement
    ):
        other = sample_company.__class__(
            id=99999999999,
            name="Other Company",
            ticker="OTHER",
        )
        with SQLiteLoader(tmp_db_path) as db:
            db.upsert_company(sample_company)
            db.upsert_company(other)
            db.upsert_statement(sample_company.id, sample_statement)
            ids = db.get_existing_document_ids(other.id)

        assert ids == set()


# ---------------------------------------------------------------------------
# upsert_errors
# ---------------------------------------------------------------------------

class TestUpsertErrors:

    def test_errors_persisted(self, tmp_db_path):
        report = PipelineReport()
        report.add_error(ETLError(
            stage="download",
            ticker="TEST",
            error_type="TimeoutError",
            message="CNV modal never released",
            document_description="PERIODICIDAD: ANUAL",
        ))

        with SQLiteLoader(tmp_db_path) as db:
            db.upsert_errors(report)

        assert _row_count(tmp_db_path, "pipeline_errors") == 1

    def test_empty_report_inserts_nothing(self, tmp_db_path):
        report = PipelineReport()
        with SQLiteLoader(tmp_db_path) as db:
            db.upsert_errors(report)

        assert _row_count(tmp_db_path, "pipeline_errors") == 0

    def test_multiple_errors_all_persisted(self, tmp_db_path):
        report = PipelineReport()
        for i in range(5):
            report.add_error(ETLError(
                stage="transform",
                ticker=f"TICK{i}",
                error_type="ValueError",
                message=f"Error {i}",
            ))

        with SQLiteLoader(tmp_db_path) as db:
            db.upsert_errors(report)

        assert _row_count(tmp_db_path, "pipeline_errors") == 5

    def test_errors_accumulate_across_runs(self, tmp_db_path):
        """Each call appends — errors are never deduplicated."""
        report = PipelineReport()
        report.add_error(ETLError(
            stage="fetch", ticker="TEST",
            error_type="ConnectionError", message="timeout"
        ))

        with SQLiteLoader(tmp_db_path) as db:
            db.upsert_errors(report)
            db.upsert_errors(report)   # second call should add another row

        assert _row_count(tmp_db_path, "pipeline_errors") == 2