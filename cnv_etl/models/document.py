from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Literal, Dict


@dataclass
class Document:
    document_id: str
    document_description: str
    document_link: str
    submission_date: datetime


@dataclass(frozen=True)
class ConceptValue:
    label: str
    value: float
    id: int


@dataclass
class FinancialStatement(Document):

    # Statement metadata
    reporting_period: Literal["Annual", "Semester", "Quarter", "Irregular"]
    period_end_date: date
    financial_statements_type: Literal["Separate", "Consolidated"]
    presentation_currency: str
    unit_of_measure: Literal["Unit", "Thousands", "Millions"]
    accounting_standards_applied: str

    # Company metadata
    merger_or_demerger_in_process: bool
    treasury_shares: bool
    insolvency_proceedings: bool
    public_offering_of_equity_instruments: bool
    subscribed_shares_at_period_end: bool
    equity_share_price_at_period_end: float
    price_earnings_ratio: float
    market_capitalization: float
    free_float_percentage: float
    number_of_employees: int

    # Statement
    statement: Dict[str, ConceptValue] = field(default_factory=dict)

    def add_concept(self, concept: ConceptValue) -> None:
        self.statement[concept.label] = concept