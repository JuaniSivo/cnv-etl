import time
from datetime import date
from typing import List
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
from cnv_etl.loaders.excel import export_company_to_excel, export_report_to_excel
from cnv_etl.config import PIPELINE_DATE_FROM, PIPELINE_DATE_TO, EXCLUDE_KEYWORDS
from cnv_etl.errors import ETLError, CompanyStats, PipelineReport

setup_logging()
logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Pure helpers (no Selenium)
# ---------------------------------------------------------------------------

def _deduplicate_documents(raw_docs: List[RawDocument]) -> List[RawDocument]:
    """
    For each period end date, keep only one document using these rules:
      1. Prefer Consolidated over Separate.
      2. Among documents of the same type, keep the most recent submission date.

    Documents with no parseable period end date are always kept as-is.
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
    """
    Transform raw financial statements into clean domain objects.
    Logs, records, and skips any statement that fails transformation.
    """
    clean_statements: List[CleanFinancialStatement] = []

    for raw_fs in raw_statements:
        try:
            clean_fs = raw_to_clean_financial_statement(raw_fs)
            clean_statements.append(clean_fs)
            stats.statements_transformed += 1
            logger.debug(f"  Transformed statement '{raw_fs.document_description}'")
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
    stats: CompanyStats,
    report: PipelineReport,
) -> List[RawFinancialStatement]:
    """
    Open one browser session, fetch the documents table, deduplicate,
    then download every statement — all before closing the driver.

    Returns the list of successfully downloaded RawFinancialStatements.
    """
    raw_statements: List[RawFinancialStatement] = []

    with driver_session() as driver:
        navigator = CNVNavigator(driver)

        # --- Fetch documents table ---
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
            logger.error(
                f"Failed to fetch documents for {company.ticker}. "
                f"{type(e).__name__}: {e}"
            )
            return []

        logger.info(f"Found {len(raw_docs)} documents for {company.ticker}")
        for i, doc in enumerate(raw_docs, start=1):
            logger.info(f"  {i}. {doc.document_description}")

        # --- Deduplicate (pure logic, no driver needed) ---
        logger.info("--- DEDUPLICATING DOCUMENTS ---")
        raw_docs = _deduplicate_documents(raw_docs)

        # --- Download each statement in the same session ---
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
    Orchestrates the full ETL pipeline for a single company:
      1. Scrape documents table and download all statements (one browser session)
      2. Transform raw statements into clean domain objects
      3. Load clean statements into the company and export to Excel

    Each run records results in the shared PipelineReport passed at
    construction time. If no report is provided, a new one is created.
    """

    def __init__(
        self,
        date_from: date,
        date_to: date,
        exclude: List[str],
        report: PipelineReport | None = None,
    ) -> None:
        self.date_from = date_from
        self.date_to   = date_to
        self.exclude   = exclude
        self.report    = report or PipelineReport()

    def run(self, company: Company) -> CompanyStats:
        """
        Run the full pipeline for a single company.

        Returns the CompanyStats recorded for this run so the caller
        can inspect results without holding a reference to the report.
        """
        logger.info("-----------------------------------------------")
        logger.info(f"Empresa: {company.name} | Ticker: {company.ticker}")
        logger.info("-----------------------------------------------")

        stats   = self.report.start_company(company.ticker)
        t_start = time.perf_counter()

        try:
            # Single browser session for fetch + download
            raw_statements = _scrape_company(
                company, self.date_from, self.date_to, self.exclude, stats, self.report
            )

            logger.info("--- TRANSFORMING STATEMENTS ---")
            clean_statements = _transform_statements(raw_statements, stats, self.report)

            logger.info("--- LOADING INTO COMPANY ---")
            for clean_fs in clean_statements:
                company.add_statement(clean_fs)

            logger.info("--- SAVING TO EXCEL ---")
            try:
                export_company_to_excel(
                    company,
                    Path(f"data/output/{company.ticker}.xlsx")
                )
                stats.statements_loaded = len(clean_statements)
            except Exception as e:
                report_error = ETLError(
                    stage="load",
                    ticker=stats.ticker,
                    error_type=type(e).__name__,
                    message=str(e),
                )
                self.report.add_error(report_error)
                logger.error(
                    f"Couldn't save {company.name} to Excel. "
                    f"{type(e).__name__}: {e}"
                )

        finally:
            stats.duration_seconds = time.perf_counter() - t_start

        return stats


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    companies = Companies()
    companies.load_from_excel("data/input/companies.xlsx")

    report = PipelineReport()
    pipeline = FinancialStatementPipeline(
        date_from=PIPELINE_DATE_FROM,
        date_to=PIPELINE_DATE_TO,
        exclude=EXCLUDE_KEYWORDS,
        report=report,
    )

    for company in companies:
        try:
            pipeline.run(company)
        except Exception as e:
            logger.error(
                f"Pipeline failed for {company.name}. "
                f"{type(e).__name__}: {e}"
            )

    logger.info(report.summary())

    try:
        export_report_to_excel(report, Path("data/output/pipeline_report.xlsx"))
    except Exception as e:
        logger.error(f"Could not save pipeline report. {type(e).__name__}: {e}")