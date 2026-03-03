from typing import Dict
from dataclasses import dataclass, field

from cnv_etl.models.document import CleanFinancialStatement


@dataclass
class Company:
    company_id: int
    company_name: str
    company_ticker: str
    statements: Dict[int, CleanFinancialStatement] = field(default_factory=dict)
    
    def add_statement(self, statement: CleanFinancialStatement) -> None:
        self.statements[statement.document_id] = statement


class Companies:
    def __init__(self):
        self.by_ticker: Dict[str, Company] = dict()

    def add(self, company: Company) -> None:
        self.by_ticker[company.company_ticker] = company