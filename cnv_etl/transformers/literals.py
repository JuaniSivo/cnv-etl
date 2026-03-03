from typing import Literal, Dict


def parse_cnv_reporting_period(reporting_period: str) -> Literal["Annual", "Quarter"]:
    reporting_period_map: Dict[str, Literal["Annual", "Quarter"]] = {
        "Anual": "Annual",
        "1": "Annual",
        "Trimestral": "Quarter",
        "3": "Quarter"
    }
    return reporting_period_map[reporting_period]


def parse_cnv_financial_statements_type(financial_statements_type: str) -> Literal["Separate", "Consolidated"]:
    statement_type_map: Dict[str, Literal["Separate", "Consolidated"]] = {
        "Individual": "Separate",
        "Consolidado": "Consolidated"
    }
    return statement_type_map[financial_statements_type]