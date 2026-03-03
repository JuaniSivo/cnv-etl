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
from cnv_etl.transformers.dates import parse_period_end_date_from_description
from cnv_etl.transformers.literals import parse_statements_type_from_description
from cnv_etl.loaders.excel import export_company_to_excel


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

    print("\n--- DOCUMENTS FOUND ---\n")
    for i, raw_doc in enumerate(raw_docs):
        print(f"{i+1}. {raw_doc.document_description}")

    # -- CLEAN DOCUMENTS --

    print("\n--- DOCUMENTS REMOVED ---\n")
    dates = [parse_period_end_date_from_description(raw_doc.document_description) for raw_doc in raw_docs]
    for idx, raw_doc in enumerate(raw_docs):
        date = parse_period_end_date_from_description(raw_doc.document_description)
        if date is None:
            continue

        if dates.count(date) > 1:
            statement_type = parse_statements_type_from_description(raw_doc.document_description)
            if statement_type == "Separate":
                raw_docs.pop(idx)
                print(f"- Removed {statement_type} document from {date}")

    download = input(f"\nDownload {len(raw_docs)} financial statements? [y/n]: ")
    if download.lower() == "n":
        return None

    # -- EXTRACT & PARSE FINANCIAL STATEMENTS --

    print("\n--- DOWNLOADING FINANCIAL STATEMENTS ---\n")

    raw_statements: List[RawFinancialStatement] = list()

    for i, raw_doc in enumerate(raw_docs, start=1):

        raw_fs = RawFinancialStatement(
            raw_doc.document_id,
            raw_doc.document_description,
            raw_doc.document_link,
            raw_doc.submission_date
        )

        fs_date = parse_period_end_date_from_description(raw_doc.document_description)
        fs_type = parse_statements_type_from_description(raw_doc.document_description)
        print(f"{i}. {fs_date} - {fs_type} - {raw_doc.document_link}")

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
            print(f"  Exception {type(e)}. Couldn't download financial statement for date {date}. Description: {e}")


    driver.quit()

    # -- TRANSFORM --

    print("\n--- TRANSFORMING FINANCIAL STATEMENTS ---\n")

    for raw_fs in raw_statements:
        try:
            clean_fs = raw_to_clean_financial_statement(raw_fs)
            company.add_statement(clean_fs)
        except Exception as e:
            print(f"  Exception {type(e)}. Couldn't load clean financial statement for date {date}. Description: {e}")

    # -- LOAD --

    print("\n--- SAVING COMPANY ---\n")

    try:
        export_company_to_excel(
            company,
            Path(f"data/{company.ticker}.xlsx")
        )
    except Exception as e:
            print(f"  Exception {type(e)}. Couldn't save company {company.name} to excel. Description: {e}")


companies = Companies()
companies.load_from_excel("data/input/companies.xlsx")
company = companies.get_by_ticker("SEMI")

load_financial_statements(
    company,
    date(2019,1,1),
    date(2019,3,1),
    [
        "RELAC.: CONTROLADA",
        "NORMA CONTABLE: NCP",
        "BALANCE SUBSIDIARIA"
    ]
)