from datetime import date
from typing import List
from pathlib import Path

from cnv_etl.models.company import Company, Companies
from cnv_etl.models.document import RawFinancialStatement
from cnv_etl.scraping.session import create_driver
from cnv_etl.scraping.navigator import CNVNavigator
from cnv_etl.parsing.documents_table import DocumentsTableParser
from cnv_etl.parsing.statement_metadata import StatementMetadataParser
from cnv_etl.parsing.company_metadata import CompanyMetadataParser
from cnv_etl.parsing.statement_values import StatementValuesParser
from cnv_etl.transformers.raw_to_clean_fs import raw_to_clean_financial_statement
from cnv_etl.loaders.excel import export_company_to_excel


def load_financial_statements(
    company: Company,
    date_from: date,
    date_to: date,
    exclude: List[str] = list()
) -> None:

    # -- EXTRACT & PARSE --

    driver = create_driver()
    navigator = CNVNavigator(driver)

    header, rows = navigator.open_documents_table(
        str(company.id),
        date_from,
        date_to
    )
    raw_docs = DocumentsTableParser().parse(header, rows, exclude)

    print("\n--- DOCUMENTS FOUND ---\n")
    for i, raw_doc in enumerate(raw_docs):
        print(f"  {i+1}. {raw_doc.document_description}")

    download = input("\nDownload financial statements? [y/n]: ")
    if download.lower() == "n":
        return None

    raw_statements: List[RawFinancialStatement] = list()

    for raw_doc in raw_docs:

        raw_fs = RawFinancialStatement(
            raw_doc.document_id,
            raw_doc.document_description,
            raw_doc.document_link,
            raw_doc.submission_date
        )

        navigator.open_statement(raw_fs.document_link)

        navigator.open_statement_metadata_tab()
        raw_fs = StatementMetadataParser().parse(driver, raw_fs)

        navigator.open_company_metadata_tab()
        raw_fs = CompanyMetadataParser().parse(driver, raw_fs)

        navigator.open_statement_values_tab()
        raw_fs = StatementValuesParser().parse(driver, raw_fs)

        raw_statements.append(raw_fs)

    driver.quit()

    # -- TRANSFORM --

    for raw_fs in raw_statements:
        clean_fs = raw_to_clean_financial_statement(raw_fs)
        company.add_statement(clean_fs)

    # -- LOAD --

    export_company_to_excel(
        company,
        Path(f"data/{company.ticker}.xlsx")
    )


companies = Companies()
companies.load_from_excel("data/input/companies.xlsx")
company = companies.get_by_ticker("SEMI")

load_financial_statements(
    company,
    date(2025,12,1),
    date(2026,3,1),
    [
        "RELAC.: CONTROLADA",
        "NORMA CONTABLE: NCP"
    ]
)