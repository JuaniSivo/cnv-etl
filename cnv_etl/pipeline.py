from datetime import date
from typing import List
from pathlib import Path

from cnv_etl.logging_config import setup_logging, get_logger
from cnv_etl.models.company import Company, Companies
from cnv_etl.models.document import RawFinancialStatement, CleanFinancialStatement, RawDocument
from cnv_etl.scraping.session import create_driver
from cnv_etl.scraping.navigator import CNVNavigator
from cnv_etl.parsing.documents_table import DocumentsTableParser
from cnv_etl.parsing.statement_metadata import StatementMetadataParser
from cnv_etl.parsing.company_metadata import CompanyMetadataParser
from cnv_etl.parsing.statement_values import StatementValuesParser
from cnv_etl.transformers.raw_to_clean_fs import raw_to_clean_financial_statement
from cnv_etl.transformers.dates import parse_period_end_date_from_description, parse_cnv_datetime
from cnv_etl.transformers.literals import parse_statements_type_from_description
from cnv_etl.loaders.excel import export_company_to_excel
from cnv_etl.config import PIPELINE_DATE_FROM, PIPELINE_DATE_TO, EXCLUDE_KEYWORDS

setup_logging()
logger = get_logger(__name__)


def _transform_statements(
    raw_statements: List[RawFinancialStatement]
) -> List[CleanFinancialStatement]:
    """
    Transform raw financial statements into clean domain objects.
    Logs and skips any statement that fails transformation.
    """
    clean_statements: List[CleanFinancialStatement] = []

    for raw_fs in raw_statements:
        try:
            clean_fs = raw_to_clean_financial_statement(raw_fs)
            clean_statements.append(clean_fs)
            logger.debug(f"  Transformed statement '{raw_fs.document_description}'")
        except Exception as e:
            logger.error(
                f"Couldn't transform statement '{raw_fs.document_description}'. "
                f"{type(e).__name__}: {e}"
            )

    logger.info(f"Transformed {len(clean_statements)}/{len(raw_statements)} statements")
    return clean_statements


def _fetch_raw_documents(
    company: Company,
    date_from: date,
    date_to: date,
    exclude: List[str]
) -> List[RawDocument]:
    """
    Open a Selenium session, navigate to the company's documents table
    and return the filtered list of raw documents.
    """
    driver    = create_driver()
    navigator = CNVNavigator(driver)

    try:
        header, rows = navigator.open_documents_table(str(company.id), date_from, date_to)
        raw_docs = DocumentsTableParser().parse(header, rows, exclude)
    finally:
        driver.quit()

    logger.info(f"Found {len(raw_docs)} documents for {company.ticker}")
    for i, doc in enumerate(raw_docs, start=1):
        logger.info(f"  {i}. {doc.document_description}")

    return raw_docs


def _download_statements(raw_docs: List[RawDocument]) -> List[RawFinancialStatement]:
    """
    Open a Selenium session and download each raw financial statement
    from the CNV website.
    """
    driver    = create_driver()
    navigator = CNVNavigator(driver)

    raw_statements: List[RawFinancialStatement] = []

    try:
        for i, raw_doc in enumerate(raw_docs, start=1):
            raw_fs = RawFinancialStatement(
                raw_doc.document_id,
                raw_doc.document_description,
                raw_doc.document_link,
                raw_doc.submission_date
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
            except Exception as e:
                logger.error(
                    f"Couldn't download statement for date {fs_end_date}. "
                    f"{type(e).__name__}: {e}"
                )
    finally:
        driver.quit()

    return raw_statements


def _deduplicate_documents(raw_docs: List[RawDocument]) -> List[RawDocument]:
    """
    For each period end date, keep only one document using these rules:
      1. Prefer Consolidated over Separate.
      2. Among documents of the same type, keep the most recent submission date.

    Documents with no parseable period end date are always kept as-is.
    """
    # Separate documents with no parseable date — they are never deduplicated
    undated = [d for d in raw_docs if parse_period_end_date_from_description(d.document_description) is None]
    dated   = [d for d in raw_docs if parse_period_end_date_from_description(d.document_description) is not None]

    # Group by period end date
    groups: dict = {}
    for doc in dated:
        key = parse_period_end_date_from_description(doc.document_description)
        groups.setdefault(key, []).append(doc)

    kept = []
    for period_date, docs in groups.items():
        if len(docs) == 1:
            kept.append(docs[0])
            continue

        # Prefer Consolidated; fall back to Separate if none exist
        consolidated = [d for d in docs if parse_statements_type_from_description(d.document_description) == "Consolidated"]
        candidates   = consolidated if consolidated else docs

        # Among candidates, pick the most recent submission date
        winner = max(candidates, key=lambda d: parse_cnv_datetime(d.submission_date))

        removed = [d for d in docs if d is not winner]
        for doc in removed:
            stmt_type = parse_statements_type_from_description(doc.document_description)
            logger.info(f"  Removed {stmt_type} document from {period_date} (submission: {doc.submission_date})")

        kept.append(winner)

    return undated + kept


class FinancialStatementPipeline:
    """
    Orchestrates the full ETL pipeline for a single company:
      1. Fetch raw documents list from CNV
      2. Deduplicate by period end date
      3. Download each financial statement
      4. Transform raw statements into clean domain objects
      5. Load clean statements into the company and export to Excel
    """

    def __init__(
        self,
        date_from: date,
        date_to: date,
        exclude: List[str]
    ) -> None:
        self.date_from = date_from
        self.date_to   = date_to
        self.exclude   = exclude

    def run(self, company: Company) -> None:
        logger.info("-----------------------------------------------")
        logger.info(f"Empresa: {company.name} | Ticker: {company.ticker}")
        logger.info("-----------------------------------------------")

        logger.info("--- FETCHING DOCUMENTS ---")
        raw_docs = _fetch_raw_documents(company, self.date_from, self.date_to, self.exclude)

        logger.info("--- DEDUPLICATING DOCUMENTS ---")
        raw_docs = _deduplicate_documents(raw_docs)

        logger.info("--- DOWNLOADING STATEMENTS ---")
        raw_statements = _download_statements(raw_docs)

        logger.info("--- TRANSFORMING STATEMENTS ---")
        clean_statements = _transform_statements(raw_statements)

        logger.info("--- LOADING INTO COMPANY ---")
        for clean_fs in clean_statements:
            company.add_statement(clean_fs)

        logger.info("--- SAVING TO EXCEL ---")
        try:
            export_company_to_excel(
                company,
                Path(f"data/output/{company.ticker}.xlsx")
            )
        except Exception as e:
            logger.error(
                f"Couldn't save company {company.name} to Excel. "
                f"{type(e).__name__}: {e}"
            )


def load_financial_statements(
    company: Company,
    date_from: date,
    date_to: date,
    exclude: List[str] = list()
) -> None:
    """Convenience wrapper kept for backwards compatibility."""
    FinancialStatementPipeline(date_from, date_to, exclude).run(company)


if __name__ == "__main__":
    companies = Companies()
    companies.load_from_excel("data/input/companies.xlsx")

    pipeline = FinancialStatementPipeline(
        date_from=PIPELINE_DATE_FROM,
        date_to=PIPELINE_DATE_TO,
        exclude=EXCLUDE_KEYWORDS
    )

    for company in companies:
        try:
            pipeline.run(company)
        except Exception as e:
            logger.error(
                f"Pipeline failed for {company.name}. "
                f"{type(e).__name__}: {e}"
            )