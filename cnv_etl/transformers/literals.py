import re
from typing import Literal, Dict, Optional


# Mapping tables
REPORTING_PERIOD_MAP: Dict[str, Literal["Annual", "Semester", "Quarter", "Irregular"]] = {
    "ANUAL": "Annual",
    "1": "Annual",
    "SEMESTRAL": "Semester", 
    "2": "Semester",
    "TRIMESTRAL": "Quarter",
    "3": "Quarter",
    "IRREGULAR": "Irregular"
}

STATEMENT_TYPE_MAP: Dict[str, Literal["Separate", "Consolidated"]] = {
    "INDIVIDUAL": "Separate",
    "CONSOLIDADO": "Consolidated"
}

ACCOUNTING_STANDARD_MAP: Dict[str, Literal["IFRS", "Argentine GAAP (RT)", "Argentine GAAP"]] = {
    "NIIF": "IFRS",
    "RT": "Argentine GAAP (RT)",
    "NORMAS CONTABLES PROFESIONALES": "Argentine GAAP"
}


def parse_cnv_reporting_period(document_description: str, reporting_period: str) -> Literal["Annual", "Semester", "Quarter", "Irregular"]:
    period_from_description = parse_reporting_period_from_description(document_description)
    period_from_statement_metadata = parse_reporting_period_from_metadata(reporting_period)

    if period_from_description is None and period_from_statement_metadata is None:
        raise ValueError(f"Cannot parse date from period end date nor document description.\n- Description: {document_description}\n- Metadata: {reporting_period}")

    if period_from_statement_metadata is None:
        return period_from_description # type: ignore

    return period_from_statement_metadata


def parse_reporting_period_from_metadata(reporting_period: str) -> Optional[Literal["Annual", "Semester", "Quarter", "Irregular"]]:
    reporting_period = reporting_period.upper()
    if reporting_period in REPORTING_PERIOD_MAP.keys():
        return REPORTING_PERIOD_MAP[reporting_period]
    
    return None


def parse_reporting_period_from_description(description: str) -> Optional[Literal["Annual", "Semester", "Quarter", "Irregular"]]:
        """
        Extract reporting period from description.
        
        Looks for patterns like:
        - "PERIODICIDAD: 3"
        - "PERIODICIDAD: TRIMESTRAL"
        - "PERIODICIDAD: ANUAL"
        """
        # Pattern: PERIODICIDAD: <value>
        match = re.search(r'PERIODICIDAD:\s*(\w+)', description)
        if match:
            value = match.group(1).strip()
            return REPORTING_PERIOD_MAP.get(value)
        
        return None


def parse_cnv_financial_statements_type(document_description: str, financial_statements_type: str) -> Literal["Separate", "Consolidated"]:
    type_from_description = parse_statements_type_from_description(document_description)
    type_from_statement_metadata = parse_statements_type_from_statement_metadata(financial_statements_type)

    if type_from_description is None and type_from_statement_metadata is None:
        raise ValueError(f"Cannot parse date from period end date nor document description.\n- Description: {document_description}\n- Metadata: {financial_statements_type}")

    if type_from_statement_metadata is None:
        return type_from_description # type: ignore

    return type_from_statement_metadata


def parse_statements_type_from_statement_metadata(financial_statements_type: str) -> Optional[Literal["Separate", "Consolidated"]]:
    financial_statements_type = financial_statements_type.upper()
    if financial_statements_type in STATEMENT_TYPE_MAP.keys():
        return STATEMENT_TYPE_MAP[financial_statements_type] 
    
    return None


def parse_statements_type_from_description(description: str) -> Optional[Literal["Separate", "Consolidated"]]:
        """
        Extract financial statement type from description.
        
        Looks for patterns like:
        - "TIPO BALANCE: INDIVIDUAL"
        - "TIPO BALANCE: CONSOLIDADO"
        """
        # Pattern: TIPO BALANCE: <value>
        match = re.search(r'TIPO BALANCE:\s*(\w+)', description)
        if match:
            value = match.group(1).strip()
            return STATEMENT_TYPE_MAP.get(value)
        
        return None


def parse_accounting_standards_applied(document_description: str, accounting_std: str) -> Literal["IFRS", "Argentine GAAP (RT)", "Argentine GAAP"]:
    accounting_std_from_description = parse_accounting_standard_from_description(document_description)
    accounting_std_from_statement_metadata = parse_accounting_standard_from_statement_metadata(accounting_std)

    if accounting_std_from_description is None and accounting_std_from_statement_metadata is None:
        raise ValueError(f"Cannot parse date from period end date nor document description.\n- Description: {document_description}\n- Metadata: {accounting_std}")

    if accounting_std_from_statement_metadata is None:
        return accounting_std_from_description # type: ignore

    return accounting_std_from_statement_metadata


def parse_accounting_standard_from_statement_metadata(accounting_std: str) -> Optional[Literal["IFRS", "Argentine GAAP (RT)", "Argentine GAAP"]]:
    accounting_std = accounting_std.upper()
    if accounting_std in ACCOUNTING_STANDARD_MAP.keys():
        return ACCOUNTING_STANDARD_MAP[accounting_std] 
    
    return None


def parse_accounting_standard_from_description(description: str) -> Optional[Literal["IFRS", "Argentine GAAP (RT)", "Argentine GAAP"]]:
        """
        Extract accounting standards from description.
        
        Looks for patterns like:
        - "NORMA CONTABLE: NIIF"
        - "NORMA CONTABLE: RT"
        """
        # Pattern: NORMA CONTABLE: <value>
        match = re.search(r'NORMA CONTABLE:\s*([A-Z\s]+?)(?:\s*-|$)', description)
        if match:
            value = match.group(1).strip()
            return ACCOUNTING_STANDARD_MAP.get(value)
        
        return None