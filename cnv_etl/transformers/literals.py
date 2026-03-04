import re
from typing import Literal, Optional

from cnv_etl.config import REPORTING_PERIOD, STATEMENT_TYPE, ACCOUNTING_STANDARD


# ---------------------------------------------------------------------------
# Reporting period
# ---------------------------------------------------------------------------

def parse_reporting_period(
    document_description: str,
    reporting_period: str
) -> Literal["Annual", "Semester", "Quarter", "Irregular"]:

    period_from_description = parse_reporting_period_from_description(document_description)
    period_from_metadata    = parse_reporting_period_from_metadata(reporting_period)

    if period_from_description is None and period_from_metadata is None:
        raise ValueError(
            f"Cannot parse reporting period from description or metadata.\n"
            f"- Description: {document_description}\n"
            f"- Metadata:    {reporting_period}"
        )

    return period_from_metadata or period_from_description  # type: ignore


def parse_reporting_period_from_metadata(
    reporting_period: str
) -> Optional[Literal["Annual", "Semester", "Quarter", "Irregular"]]:
    return REPORTING_PERIOD.get(reporting_period.upper())


def parse_reporting_period_from_description(
    description: str
) -> Optional[Literal["Annual", "Semester", "Quarter", "Irregular"]]:
    """Extract reporting period from patterns like 'PERIODICIDAD: ANUAL'."""
    match = re.search(r'PERIODICIDAD\s*:\s*(\w+)', description)
    if match:
        return REPORTING_PERIOD.get(match.group(1).strip())
    return None


# ---------------------------------------------------------------------------
# Financial statements type
# ---------------------------------------------------------------------------

def parse_financial_statements_type(
    document_description: str,
    financial_statements_type: str
) -> Literal["Separate", "Consolidated"]:

    type_from_description = parse_statements_type_from_description(document_description)
    type_from_metadata    = parse_statements_type_from_statement_metadata(financial_statements_type)

    if type_from_description is None and type_from_metadata is None:
        raise ValueError(
            f"Cannot parse statements type from description or metadata.\n"
            f"- Description: {document_description}\n"
            f"- Metadata:    {financial_statements_type}"
        )

    return type_from_metadata or type_from_description  # type: ignore


def parse_statements_type_from_statement_metadata(
    financial_statements_type: str
) -> Optional[Literal["Separate", "Consolidated"]]:
    return STATEMENT_TYPE.get(financial_statements_type.upper())


def parse_statements_type_from_description(
    description: str
) -> Optional[Literal["Separate", "Consolidated"]]:
    """Extract statement type from patterns like 'TIPO BALANCE: CONSOLIDADO'."""
    match = re.search(r'TIPO\s+BALANCE\s*:\s*(\w+)', description)
    if match:
        return STATEMENT_TYPE.get(match.group(1).strip())
    return None


# ---------------------------------------------------------------------------
# Accounting standards
# ---------------------------------------------------------------------------

def parse_accounting_standards_applied(
    document_description: str,
    accounting_std: str
) -> Literal["IFRS", "Argentine GAAP (RT)", "Argentine GAAP"]:

    std_from_description = parse_accounting_standard_from_description(document_description)
    std_from_metadata    = parse_accounting_standard_from_statement_metadata(accounting_std)

    if std_from_description is None and std_from_metadata is None:
        raise ValueError(
            f"Cannot parse accounting standard from description or metadata.\n"
            f"- Description: {document_description}\n"
            f"- Metadata:    {accounting_std}"
        )

    return std_from_metadata or std_from_description  # type: ignore


def parse_accounting_standard_from_statement_metadata(
    accounting_std: str
) -> Optional[Literal["IFRS", "Argentine GAAP (RT)", "Argentine GAAP"]]:
    return ACCOUNTING_STANDARD.get(accounting_std.upper())


def parse_accounting_standard_from_description(
    description: str
) -> Optional[Literal["IFRS", "Argentine GAAP (RT)", "Argentine GAAP"]]:
    """Extract accounting standard from patterns like 'NORMA CONTABLE: NIIF'."""
    match = re.search(r'NORMA\s+CONTABLE\s*:\s*([A-Z\s]+?)(?:\s*-|$)', description)
    if match:
        return ACCOUNTING_STANDARD.get(match.group(1).strip())
    return None