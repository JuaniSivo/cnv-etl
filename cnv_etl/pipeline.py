"""
Core ETL pipeline for cnv_etl.

Entry point: python -m cnv_etl  (see cnv_etl/cli.py)
"""

import time
from datetime import date
from typing import List, Literal, Optional
from pathlib import Path

from cnv_etl.logging_config import setup_logging, get_logger
from cnv_etl.models.company import Company, Companies
from cnv_etl.models.document import RawFinancialStatement, CleanFinancialStatement, RawDocument
from cnv_etl.scraping.session import driver_session
from cnv_etl.scraping.navigator import CNVNavigator
from cnv_etl.parsing.documents_table import DocumentsTableParser
from cnv_etl.parsing.statement_metadata import StatementMetadataParser
from cnv_etl.parsing.company_metadata import CompanyMetadataParser
from cnv_etl.parsing.statement_values import StatementValuesParser
from cnv_etl.transformers.raw_to_clean_fs import raw_to_clean_financial_statement
from cnv_etl.transformers.dates import parse_period_end_date_from_description, parse_cnv_datetime
from cnv_etl.transformers.literals import parse_statements_type_from_description
from cnv_etl.loaders.excel import export_company_to_excel
from cnv_etl.loaders.sqlite import SQLiteLoader
from cnv_etl.errors import ETLError, CompanyStats, PipelineReport
from cnv_etl.enrichment import Enricher, FileSource
from cnv_etl.config import (
    ENRICHMENT_REGISTRY,
    ENRICHMENTS_FILE_PATH,
)

setup_logging()
logger = get_logger(__name__)

OutputMode = Literal["excel", "sqlite", "both"]
RunMode    = Literal["overwrite", "update"]

_DEFAULT_DB_PATH   = Path("data/output/cnv.db")
_DEFAULT_EXCEL_DIR = Path("data/output")


# ---------------------------------------------------------------------------
# Pure helpers (no Selenium)
# ---------------------------------------------------------------------------

def _deduplicate_documents(raw_docs: List[RawDocument]) -> List[RawDocument]:
    """
    For each period end date, keep only one document:
      1. Prefer Consolidated over Separate.
      2. Among same type, keep the most recent submission date.
    Documents with no parseable period end date are kept as-is.
    """
    undated = [d for d in raw_docs if parse_period_end_date_from_description(d.document_description) is None]
    dated   = [d for d in raw_docs if parse_period_end_date_from_description(d.document_description) is not None]

    groups: dict = {}
    for doc in dated:
        key = parse_period_end_date_from_description(doc.document_description)
        groups.setdefault(key, []).append(doc)

    kept = []
    for period_date, docs in groups.items():
        if len(docs) == 1:
            kept.append(docs[0])
            continue

        consolidated = [d for d in docs if parse_statements_type_from_description(d.document_description) == "Consolidated"]
        candidates   = consolidated if consolidated else docs
        winner       = max(candidates, key=lambda d: parse_cnv_datetime(d.submission_date))

        for doc in (d for d in docs if d is not winner):
            stmt_type = parse_statements_type_from_description(doc.document_description)
            logger.info(f"  Removed {stmt_type} document from {period_date} (submission: {doc.submission_date})")

        kept.append(winner)

    return undated + kept


def _transform_statements(
    raw_statements: List[RawFinancialStatement],
    stats: CompanyStats,
    report: PipelineReport,
) -> List[CleanFinancialStatement]:
    """Transform raw statements, recording any failures in the report."""
    clean_statements: List[CleanFinancialStatement] = []

    for raw_fs in raw_statements:
        try:
            clean_fs = raw_to_clean_financial_statement(raw_fs)
            clean_statements.append(clean_fs)
            stats.statements_transformed += 1
            logger.debug(f"  Transformed '{raw_fs.document_description}'")
        except Exception as e:
            report.add_error(ETLError(
                stage="transform",
                ticker=stats.ticker,
                error_type=type(e).__name__,
                message=str(e),
                document_description=raw_fs.document_description,
            ))
            logger.error(
                f"Couldn't transform '{raw_fs.document_description}'. "
                f"{type(e).__name__}: {e}"
            )

    logger.info(f"Transformed {len(clean_statements)}/{len(raw_statements)} statements")
    return clean_statements


# ---------------------------------------------------------------------------
# Single-session scraping
# ---------------------------------------------------------------------------

def _scrape_company(
    company: Company,
    date_from: date,
    date_to: date,
    exclude: List[str],
    existing_ids: set[int],
    stats: CompanyStats,
    report: PipelineReport,
) -> List[RawFinancialStatement]:
    """
    Open one browser session, fetch + deduplicate the documents table,
    filter out already-stored documents (update mode), then download
    the remaining statements — all before closing the driver.
    """
    raw_statements: List[RawFinancialStatement] = []

    with driver_session() as driver:
        navigator = CNVNavigator(driver)

        # --- Fetch ---
        logger.info("--- FETCHING DOCUMENTS ---")
        try:
            header, rows = navigator.open_documents_table(str(company.id), date_from, date_to)
            raw_docs = DocumentsTableParser().parse(header, rows, exclude)
        except Exception as e:
            report.add_error(ETLError(
                stage="fetch",
                ticker=stats.ticker,
                error_type=type(e).__name__,
                message=str(e),
            ))
            logger.error(f"Failed to fetch documents for {company.ticker}. {type(e).__name__}: {e}")
            return []

        logger.info(f"Found {len(raw_docs)} documents for {company.ticker}")
        for i, doc in enumerate(raw_docs, start=1):
            logger.info(f"  {i}. {doc.document_description}")

        # --- Deduplicate ---
        logger.info("--- DEDUPLICATING DOCUMENTS ---")
        raw_docs = _deduplicate_documents(raw_docs)

        # --- Filter already-stored (update mode) ---
        if existing_ids:
            before   = len(raw_docs)
            raw_docs = [d for d in raw_docs if int(d.document_id) not in existing_ids]
            skipped  = before - len(raw_docs)
            if skipped:
                logger.info(f"  Skipped {skipped} already-stored document(s)")

        if not raw_docs:
            logger.info("  Nothing new to download.")
            return []

        # --- Download ---
        logger.info("--- DOWNLOADING STATEMENTS ---")
        for i, raw_doc in enumerate(raw_docs, start=1):
            raw_fs = RawFinancialStatement(
                raw_doc.document_id,
                raw_doc.document_description,
                raw_doc.document_link,
                raw_doc.submission_date,
            )

            fs_end_date = parse_period_end_date_from_description(raw_doc.document_description)
            fs_type     = parse_statements_type_from_description(raw_doc.document_description)
            logger.info(f"  {i}/{len(raw_docs)} Downloading {fs_end_date} - {fs_type}")
            logger.debug(f"  Link: {raw_doc.document_link}")

            try:
                navigator.open_statement(raw_fs.document_link)

                navigator.open_statement_metadata_tab()
                raw_fs = StatementMetadataParser().parse(driver, raw_fs)

                navigator.open_company_metadata_tab()
                raw_fs = CompanyMetadataParser().parse(driver, raw_fs)

                navigator.open_statement_values_tab()
                raw_fs = StatementValuesParser().parse(driver, raw_fs)

                raw_statements.append(raw_fs)
                stats.statements_downloaded += 1

            except Exception as e:
                report.add_error(ETLError(
                    stage="download",
                    ticker=stats.ticker,
                    error_type=type(e).__name__,
                    message=str(e),
                    document_description=raw_doc.document_description,
                ))
                logger.error(
                    f"Couldn't download statement for {fs_end_date}. "
                    f"{type(e).__name__}: {e}"
                )

    return raw_statements


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class FinancialStatementPipeline:
    """
    Orchestrates the full ETL pipeline for a single company.

    Parameters
    ----------
    date_from : date
        Start of the document search window.
    date_to : date
        End of the document search window.
    exclude : list[str]
        Keywords — documents whose description contains any of these
        are filtered out before downloading.
    run_mode : 'overwrite' | 'update'
        overwrite — replace existing data for the company.
        update    — skip statements already present in the DB.
    output : 'excel' | 'sqlite' | 'both'
        Where to persist clean statements.
    db_path : Path, optional
        SQLite file path. Only used when output is 'sqlite' or 'both'.
    report : PipelineReport, optional
        Shared report; a new one is created if not provided.
    """

    def __init__(
        self,
        date_from:  date,
        date_to:    date,
        exclude:    List[str],
        run_mode:   RunMode    = "overwrite",
        output:     OutputMode = "excel",
        db_path:    Optional[Path] = None,
        report:     Optional[PipelineReport] = None,
    ) -> None:
        self.date_from = date_from
        self.date_to   = date_to
        self.exclude   = exclude
        self.run_mode  = run_mode
        self.output    = output
        self.report    = report or PipelineReport()

        self._db: Optional[SQLiteLoader] = None
        if output in ("sqlite", "both"):
            resolved_db = db_path or _DEFAULT_DB_PATH
            self._db = SQLiteLoader(resolved_db, overwrite=(run_mode == "overwrite"))

        self._enricher = Enricher(
            registry=ENRICHMENT_REGISTRY,
            source=FileSource(ENRICHMENTS_FILE_PATH),
        )

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        """Flush and close the SQLite connection if open."""
        if self._db:
            self._db.close()
            self._db = None

    def __enter__(self) -> "FinancialStatementPipeline":
        return self

    def __exit__(self, *_) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # Run                                                                  #
    # ------------------------------------------------------------------ #

    def run(self, company: Company) -> CompanyStats:
        """
        Run the full pipeline for a single company.

        Returns the CompanyStats recorded for this run.
        """
        logger.info("-----------------------------------------------")
        logger.info(f"Empresa: {company.name} | Ticker: {company.ticker}")
        logger.info("-----------------------------------------------")

        stats   = self.report.start_company(company.ticker)
        t_start = time.perf_counter()

        try:
            existing_ids: set[int] = set()
            if self.run_mode == "update" and self._db is not None:
                existing_ids = self._db.get_existing_document_ids(company.id)
                logger.info(f"  Update mode: {len(existing_ids)} document(s) already stored")

            raw_statements = _scrape_company(
                company, self.date_from, self.date_to, self.exclude,
                existing_ids, stats, self.report,
            )

            logger.info("--- ENRICHING STATEMENTS ---")
            raw_statements = self._enricher.enrich(
                ticker=company.ticker,
                statements=raw_statements,
                stats=stats,
                report=self.report,
            )

            logger.info("--- TRANSFORMING STATEMENTS ---")
            clean_statements = _transform_statements(raw_statements, stats, self.report)

            logger.info("--- LOADING ---")
            for clean_fs in clean_statements:
                company.add_statement(clean_fs)

            self._load_company(company, clean_statements, stats)

        finally:
            stats.duration_seconds = time.perf_counter() - t_start

        return stats

    # ------------------------------------------------------------------ #
    # Private                                                              #
    # ------------------------------------------------------------------ #

    def _load_company(
        self,
        company: Company,
        clean_statements: List[CleanFinancialStatement],
        stats: CompanyStats,
    ) -> None:
        """Persist clean statements to the configured output(s)."""

        if self.output in ("excel", "both"):
            try:
                export_company_to_excel(
                    company,
                    _DEFAULT_EXCEL_DIR / f"{company.ticker}.xlsx",
                )
                if self.output == "excel":
                    stats.statements_loaded = len(clean_statements)
            except Exception as e:
                self.report.add_error(ETLError(
                    stage="load",
                    ticker=stats.ticker,
                    error_type=type(e).__name__,
                    message=f"Excel export failed: {e}",
                ))
                logger.error(f"Excel export failed for {company.ticker}. {type(e).__name__}: {e}")

        if self.output in ("sqlite", "both") and self._db is not None:
            try:
                self._db.upsert_company(company)
                for stmt in clean_statements:
                    self._db.upsert_statement(company.id, stmt)
                stats.statements_loaded = len(clean_statements)
            except Exception as e:
                self.report.add_error(ETLError(
                    stage="load",
                    ticker=stats.ticker,
                    error_type=type(e).__name__,
                    message=f"SQLite upsert failed: {e}",
                ))
                logger.error(f"SQLite load failed for {company.ticker}. {type(e).__name__}: {e}")