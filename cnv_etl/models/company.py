from typing import Dict
from dataclasses import dataclass, field

from cnv_etl.models.document import FinancialStatement


@dataclass
class Company:
    company_id: int
    company_name: str
    company_ticker: str
    statements: Dict[str, FinancialStatement] = field(default_factory=dict)
    
    def add_statement(self, statement: FinancialStatement) -> None:
        self.statements[statement.document_id] = statement


class Companies:
    def __init__(self):
        self.by_ticker: Dict[str, Company] = dict()

    def add(self, company: Company) -> None:
        self.by_ticker[company.company_ticker] = company