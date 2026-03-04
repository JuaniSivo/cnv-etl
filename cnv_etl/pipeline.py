from datetime import date
from typing import List
from pathlib import Path

from cnv_etl.logging_config import setup_logging, get_logger
from cnv_etl.models.company import Company, Companies
from cnv_etl.models.document import RawFinancialStatement
from cnv_etl.scraping.session import create_driver
from cnv_etl.scraping.navigator import CNVNavigator
from cnv_etl.parsing.documents_table import DocumentsTableParser
from cnv_etl.parsing.statement_metadata import StatementMetadataParser
from cnv_etl.parsing.company_metadata import CompanyMetadataParser
from cnv_etl.parsing.statement_values import StatementValuesParser
from cnv_etl.transformers.raw_to_clean_fs import raw_to_clean_financial_statement
from cnv_etl.transformers.dates import parse_period_end_date_from_description
from cnv_etl.transformers.literals import parse_statements_type_from_description
from cnv_etl.loaders.excel import export_company_to_excel
from cnv_etl.config import PIPELINE_DATE_FROM, PIPELINE_DATE_TO, EXCLUDE_KEYWORDS

setup_logging()
logger = get_logger(__name__)


def load_financial_statements(
    company: Company,
    date_from: date,
    date_to: date,
    exclude: List[str] = list()
) -> None:

    # -- EXTRACT & PARSE DOCUMENTS --

    driver = create_driver()
    navigator = CNVNavigator(driver)

    header, rows = navigator.open_documents_table(
        str(company.id),
        date_from,
        date_to
    )
    raw_docs = DocumentsTableParser().parse(header, rows, exclude)

    logger.info("--- DOCUMENTS FOUND ---")
    for i, raw_doc in enumerate(raw_docs):
        logger.info(f"  {i+1}. {raw_doc.document_description}")

    # -- CLEAN DOCUMENTS --

    logger.info("--- DEDUPLICATING DOCUMENTS ---")
    end_dates = [parse_period_end_date_from_description(d.document_description) for d in raw_docs]
    for idx, raw_doc in enumerate(raw_docs):
        fs_end_date = parse_period_end_date_from_description(raw_doc.document_description)
        if fs_end_date is None:
            continue

        if end_dates.count(fs_end_date) > 1:
            statement_type = parse_statements_type_from_description(raw_doc.document_description)
            if statement_type == "Separate":
                raw_docs.pop(idx)
                logger.info(f"  Removed {statement_type} document from {fs_end_date}")

    # -- EXTRACT & PARSE FINANCIAL STATEMENTS --

    logger.info("--- DOWNLOADING FINANCIAL STATEMENTS ---")

    raw_statements: List[RawFinancialStatement] = list()

    for i, raw_doc in enumerate(raw_docs, start=1):

        raw_fs = RawFinancialStatement(
            raw_doc.document_id,
            raw_doc.document_description,
            raw_doc.document_link,
            raw_doc.submission_date
        )

        fs_end_date = parse_period_end_date_from_description(raw_doc.document_description)
        fs_type     = parse_statements_type_from_description(raw_doc.document_description)
        logger.info(f"  {i}. {fs_end_date} - {fs_type} - {raw_doc.document_link}")

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

    driver.quit()

    # -- TRANSFORM --

    logger.info("--- TRANSFORMING FINANCIAL STATEMENTS ---")

    for raw_fs in raw_statements:
        try:
            clean_fs = raw_to_clean_financial_statement(raw_fs)
            company.add_statement(clean_fs)
        except Exception as e:
            logger.error(
                f"Couldn't transform statement '{raw_fs.document_description}'. "
                f"{type(e).__name__}: {e}"
            )

    # -- LOAD --

    logger.info("--- SAVING COMPANY ---")

    try:
        export_company_to_excel(
            company,
            Path(f"data/{company.ticker}.xlsx")
        )
    except Exception as e:
        logger.error(
            f"Couldn't save company {company.name} to Excel. "
            f"{type(e).__name__}: {e}"
        )


companies = Companies()
companies.load_from_excel("data/input/companies.xlsx")

for company in list(companies.by_ticker.values())[:1]:
    logger.info(f"-----------------------------------------------")
    logger.info(f"Empresa: {company.name} | Ticker: {company.ticker}")
    logger.info(f"-----------------------------------------------")
    try:
        load_financial_statements(
            company,
            PIPELINE_DATE_FROM,
            PIPELINE_DATE_TO,
            EXCLUDE_KEYWORDS
        )
    except Exception as e:
        logger.error(
            f"Pipeline failed for company {company.name}. "
            f"{type(e).__name__}: {e}"
        )