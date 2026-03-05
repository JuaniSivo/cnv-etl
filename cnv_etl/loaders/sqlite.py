"""
SQLite loader for cnv_etl.

Persists companies, financial statements, and pipeline errors to a local
SQLite database. Supports both overwrite and update (skip-existing) modes.

Usage
-----
# As a context manager (recommended — connection is closed automatically)
with SQLiteLoader("data/output/cnv.db") as db:
    db.upsert_company(company)
    db.upsert_statement(company.id, statement)

# Manual lifetime
db = SQLiteLoader("data/output/cnv.db")
db.upsert_company(company)
db.close()
"""

import sqlite3
from pathlib import Path
from datetime import date, datetime
from typing import Optional

from cnv_etl.models.company import Company
from cnv_etl.models.document import CleanFinancialStatement
from cnv_etl.errors import PipelineReport
from cnv_etl.logging_config import get_logger

logger = get_logger(__name__)

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


class SQLiteLoader:
    """
    Loads cnv_etl domain objects into a SQLite database.

    Parameters
    ----------
    db_path : Path | str
        Path to the SQLite file. Created if it does not exist.
    overwrite : bool
        If True, existing rows are replaced (INSERT OR REPLACE).
        If False, existing rows are kept unchanged (INSERT OR IGNORE).
    """

    def __init__(self, db_path: Path | str, overwrite: bool = False) -> None:
        self.db_path  = Path(db_path)
        self.overwrite = overwrite

        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._apply_schema()

        logger.info(f"SQLiteLoader connected to {self.db_path} (overwrite={overwrite})")

    # ------------------------------------------------------------------ #
    # Context manager                                                      #
    # ------------------------------------------------------------------ #

    def __enter__(self) -> "SQLiteLoader":
        return self

    def __exit__(self, *_) -> None:
        self.close()

    def close(self) -> None:
        """Commit any pending transaction and close the connection."""
        if self._conn:
            self._conn.commit()
            self._conn.close()
            logger.debug(f"SQLiteLoader closed {self.db_path}")

    # ------------------------------------------------------------------ #
    # Public upsert API                                                    #
    # ------------------------------------------------------------------ #

    def upsert_company(self, company: Company) -> None:
        """
        Insert or replace/ignore a company row.

        Parameters
        ----------
        company : Company
            Company domain object to persist.
        """
        sql = self._insert_sql("companies", [
            "id", "name", "ticker", "description", "sector",
            "industry_group", "industry", "sub_industry",
            "sub_industry_description",
        ])

        with self._transaction():
            self._conn.execute(sql, (
                company.id,
                company.name,
                company.ticker,
                company.description,
                company.sector,
                company.industry_group,
                company.industry,
                company.sub_industry,
                company.sub_industry_description,
            ))

        logger.debug(f"Upserted company {company.ticker}")

    def upsert_statement(
        self,
        company_id: int,
        statement: CleanFinancialStatement,
    ) -> None:
        """
        Insert or replace/ignore a statement and all its concept values
        in a single transaction.

        Parameters
        ----------
        company_id : int
            The CNV CUIT of the owning company.
        statement : CleanFinancialStatement
            Clean statement domain object to persist.
        """
        with self._transaction():
            self._upsert_statement_metadata(company_id, statement)
            self._upsert_statement_values(statement)

        logger.debug(
            f"Upserted statement {statement.document_id} "
            f"({len(statement.statement)} concepts)"
        )

    def upsert_errors(self, report: PipelineReport) -> None:
        """
        Persist all ETLErrors from a PipelineReport into pipeline_errors.

        Each call appends new rows — errors are never deduplicated here
        because the same error may occur in multiple runs.

        Parameters
        ----------
        report : PipelineReport
            The report produced by the pipeline run.
        """
        if not report.all_errors:
            return

        run_started_at = report.started_at.isoformat()

        rows = [
            (
                run_started_at,
                err.stage,
                err.ticker,
                err.error_type,
                err.message,
                err.document_description,
                err.timestamp.isoformat(),
            )
            for err in report.all_errors
        ]

        sql = """
            INSERT INTO pipeline_errors
                (run_started_at, stage, ticker, error_type,
                 message, document_description, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        with self._transaction():
            self._conn.executemany(sql, rows)

        logger.info(f"Persisted {len(rows)} pipeline errors to DB")

    # ------------------------------------------------------------------ #
    # Query helpers                                                        #
    # ------------------------------------------------------------------ #

    def get_existing_document_ids(self, company_id: int) -> set[int]:
        """
        Return the set of document_ids already stored for a company.

        Used in update mode to skip re-downloading statements that are
        already in the database.

        Parameters
        ----------
        company_id : int
            The CNV CUIT of the company to query.

        Returns
        -------
        set[int]
            Document IDs already present in statement_metadata.
        """
        cursor = self._conn.execute(
            "SELECT document_id FROM statement_metadata WHERE company_id = ?",
            (company_id,),
        )
        return {row["document_id"] for row in cursor.fetchall()}

    # ------------------------------------------------------------------ #
    # Private helpers                                                      #
    # ------------------------------------------------------------------ #

    def _apply_schema(self) -> None:
        """Read and execute schema.sql to initialise all tables."""
        schema = _SCHEMA_PATH.read_text(encoding="utf-8")
        self._conn.executescript(schema)
        self._conn.commit()
        logger.debug("Schema applied")

    def _insert_sql(self, table: str, columns: list[str]) -> str:
        """
        Build an INSERT OR REPLACE / INSERT OR IGNORE statement.

        Parameters
        ----------
        table : str
            Target table name.
        columns : list[str]
            Column names in insertion order.
        """
        verb        = "INSERT OR REPLACE" if self.overwrite else "INSERT OR IGNORE"
        col_names   = ", ".join(columns)
        placeholders = ", ".join("?" * len(columns))
        return f"{verb} INTO {table} ({col_names}) VALUES ({placeholders})"

    def _upsert_statement_metadata(
        self,
        company_id: int,
        stmt: CleanFinancialStatement,
    ) -> None:
        sql = self._insert_sql("statement_metadata", [
            "document_id", "company_id", "document_description",
            "document_link", "submission_date", "period_end_date",
            "reporting_period", "financial_statements_type",
            "presentation_currency", "accounting_standards_applied",
            "merger_or_demerger_in_process", "treasury_shares",
            "insolvency_proceedings",
            "public_offering_of_equity_instruments",
            "subscribed_shares_at_period_end",
            "equity_share_price_at_period_end",
            "free_float_percentage", "number_of_employees",
        ])

        self._conn.execute(sql, (
            stmt.document_id,
            company_id,
            stmt.document_description,
            stmt.document_link,
            _fmt_datetime(stmt.submission_date),
            _fmt_date(stmt.period_end_date),
            stmt.reporting_period,
            stmt.financial_statements_type,
            stmt.presentation_currency,
            stmt.accounting_standards_applied,
            int(stmt.merger_or_demerger_in_process),
            int(stmt.treasury_shares),
            int(stmt.insolvency_proceedings),
            int(stmt.public_offering_of_equity_instruments),
            stmt.subscribed_shares_at_period_end,
            stmt.equity_share_price_at_period_end,
            stmt.free_float_percentage,
            stmt.number_of_employees,
        ))

    def _upsert_statement_values(self, stmt: CleanFinancialStatement) -> None:
        sql = self._insert_sql(
            "statement_values",
            ["document_id", "order_index", "concept", "value"],
        )

        rows = [
            (stmt.document_id, cv.order, cv.label, cv.value)
            for cv in stmt.statement.values()
        ]
        self._conn.executemany(sql, rows)

    def _transaction(self):
        """Simple context manager that commits on success, rolls back on error."""
        return _Transaction(self._conn)


class _Transaction:
    """Minimal transaction context manager for SQLite connections."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is None:
            self._conn.commit()
        else:
            self._conn.rollback()
            logger.error(f"Transaction rolled back due to {exc_type.__name__}: {exc_val}")
        return False   # never suppress exceptions


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_date(d: date) -> str:
    return d.isoformat()


def _fmt_datetime(dt: datetime) -> str:
    return dt.isoformat(sep=" ", timespec="minutes")