from dataclasses import dataclass, field, replace
from datetime import datetime, date
from typing import Optional, Literal, Dict, List


@dataclass
class RawDocument:
    document_id: str
    document_description: str
    document_link: str
    submission_date: str


@dataclass(frozen=True)
class RawConceptValue:
    label: Optional[str] = None
    value: Optional[str] = None
    id: Optional[str] = None


@dataclass
class RawFinancialStatement(RawDocument):
    # Statement metadata
    reporting_period: Optional[str] = None
    period_end_date: Optional[str] = None
    financial_statements_type: Optional[str] = None
    presentation_currency: Optional[str] = None
    unit_of_measure: Optional[str] = None
    accounting_standards_applied: Optional[str] = None

    # Company metadata
    merger_or_demerger_in_process: Optional[str] = None
    treasury_shares: Optional[str] = None
    insolvency_proceedings: Optional[str] = None
    public_offering_of_equity_instruments: Optional[str] = None
    subscribed_shares_at_period_end: Optional[str] = None
    equity_share_price_at_period_end: Optional[str] = None
    price_earnings_ratio: Optional[str] = None
    market_capitalization: Optional[str] = None
    free_float_percentage: Optional[str] = None
    number_of_employees: Optional[str] = None

    # Statement
    statement: List[RawConceptValue] = field(default_factory=list)

    def add_concept(self, concept: RawConceptValue) -> None:
        self.statement.append(concept)

    def transform(
        self,
        statement: Optional[List[RawConceptValue]] = None,
        **metadata_updates
    ) -> "RawFinancialStatement":
        
        updates = {}
        if statement is not None:
            updates["statement"] = statement
        updates.update(metadata_updates)
        return replace(self, **updates)


@dataclass
class CleanDocument:
    document_id: int
    document_description: str
    document_link: str
    submission_date: datetime


@dataclass(frozen=True)
class CleanConceptValue:
    label: str
    value: float
    order: int


@dataclass
class CleanFinancialStatement(CleanDocument):
    # Statement metadata
    reporting_period: Literal["Annual", "Semester", "Quarter", "Irregular"]
    period_end_date: date
    financial_statements_type: Literal["Separate", "Consolidated"]
    presentation_currency: str
    accounting_standards_applied: str

    # Company metadata
    merger_or_demerger_in_process: bool
    treasury_shares: bool
    insolvency_proceedings: bool
    public_offering_of_equity_instruments: bool
    subscribed_shares_at_period_end: int
    equity_share_price_at_period_end: float
    free_float_percentage: float
    number_of_employees: int

    # Statement
    statement: Dict[str, CleanConceptValue] = field(default_factory=dict)

    def add_concept(self, concept: CleanConceptValue) -> None:
        self.statement[concept.label] = concept