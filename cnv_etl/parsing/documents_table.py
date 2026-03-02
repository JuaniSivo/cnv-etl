"""
Parser for the CNV financial documents listing table.

Transforms the raw HTML table of available filings into a structured
pandas DataFrame with normalized dates and document links.
"""

from typing import List

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement

from cnv_etl.models.document import RawDocument

class DocumentsTableParser:
    """
    Parser for the CNV documents listing table.

    Converts the raw documents table (headers and rows) into a structured
    Pandas DataFrame with normalized dates and document links.
    """

    def parse(self, header: list[WebElement], rows: list[WebElement], exclude: List[str]) -> List[RawDocument]:
        # extract column names to list
        column_names = [h.text.strip() if h.text.strip() else "column" for h in header] # Should be ['FECHA', 'HORA', 'DESCRIPCIÓN', 'DOCUMENTO', 'VER']

        documents = list()
        for row in rows[1:]:
            cols = row.find_elements(By.TAG_NAME, "td")

            date = str(cols[column_names.index("FECHA")].text)
            hour = str(cols[column_names.index("HORA")].text)
            description = str(cols[column_names.index("DESCRIPCIÓN")].text)
            id = str(cols[column_names.index("DOCUMENTO")].text)
            link = str(cols[column_names.index("VER")].find_element(By.TAG_NAME, "a").get_attribute("href"))

            fs = RawDocument(
                document_id=id,
                document_description=description,
                document_link=link,
                submission_date=date + " " + hour
            )

            document_filter = True
            for item in exclude:
                document_filter = document_filter and str(fs.document_description).lower().find(item.lower()) == -1
            
            if document_filter:
                documents.append(fs)

        return documents
