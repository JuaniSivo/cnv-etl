"""
Parser for the CNV financial documents listing table.

Transforms the raw HTML table of available filings into a structured
pandas DataFrame with normalized dates and document links.
"""

from typing import List

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement

from cnv_scraper.domain.document import FinancialStatement
from cnv_scraper.domain.company import Company

class DocumentsTableParser:
    """
    Parser for the CNV documents listing table.

    Converts the raw documents table (headers and rows) into a structured
    Pandas DataFrame with normalized dates and document links.
    """

    def parse(self, header: list[WebElement], rows: list[WebElement], company: Company, exclude: List[str]) -> Company:
        # extract column names to list
        column_names = [h.text.strip() if h.text.strip() else "column" for h in header] # Should be ['FECHA', 'HORA', 'DESCRIPCIÓN', 'DOCUMENTO', 'VER']

        documents = list()
        for row in rows[1:]:
            cols = row.find_elements(By.TAG_NAME, "td")
            if cols:
                link = None
                try:
                    link = cols[-1].find_element(By.TAG_NAME, "a").get_attribute("href")
                except:
                    pass
                documents.append([c.text.strip() for c in cols[:-1]] + [link])
        
        df = pd.DataFrame(data=documents, columns=column_names, dtype="string")
        df = df.rename(
            columns={
                "DOCUMENTO": "document_id",
                "DESCRIPCIÓN": "document_description",
                "FECHA": "date",        # date example: "13 oct. 2025"
                "HORA": "time",         # time example: "14:20"
                "VER": "document_link"
            }
        )

        df["submission_date"] = df["date"]\
                        .str.replace("ene.", "01")\
                        .str.replace("feb.", "02")\
                        .str.replace("mar.", "03")\
                        .str.replace("abr.", "04")\
                        .str.replace("may.", "05")\
                        .str.replace("jun.", "06")\
                        .str.replace("jul.", "07")\
                        .str.replace("ago.", "08")\
                        .str.replace("sep.", "09")\
                        .str.replace("oct.", "10")\
                        .str.replace("nov.", "11")\
                        .str.replace("dic.", "12")
        
        df["submission_date"] = df["submission_date"].str.cat(df["time"], sep=" ") # submission date example: "13 01 2025 14:23"
        df["submission_date"] = pd.to_datetime(
            df["submission_date"],
            format="%d %m %Y %H:%M",
            dayfirst=True
        )

        for _, row in df.iterrows():
            fs = FinancialStatement(
                document_id=str(row["document_id"]),
                document_description=str(row["document_description"]),
                document_link=str(row["document_link"]),
                submission_date=row["submission_date"].to_pydatetime()
            )

            # TODO: Preload extra information about the financial statement based on its description

            document_filter = True
            for item in exclude:
                document_filter = document_filter and str(fs.document_description).lower().find(item.lower()) == -1            
            
            if document_filter:
                company.add_statement(fs)

        return company
