"""
Parser for financial statement identification metadata.

Extracts balance-level metadata such as period, currency, unit of
measure, and accounting standards from the CNV statement detail page.
"""

import time
from datetime import date

from cnv_scraper.domain.document import FinancialStatement
from cnv_scraper.parsing.parsing_utils import get_value, get_select_text, safe_date


class StatementMetadataParser:
    """
    Parser for the 'Identificación del Balance' section.

    Extracts balance-level metadata such as:
    - reporting period
    - closing date
    - currency
    - unit of measure
    - accounting standards

    Returns a flat dictionary of label-value pairs.
    """
    
    def parse(self, driver, fs: FinancialStatement) -> FinancialStatement:
        time.sleep(1)

        reporting_period = get_select_text(driver, "1select18889")
        if reporting_period in ["Anual", "1"]: fs.reporting_period = "Annual"
        if reporting_period in ["Trimestral", "3"]: fs.reporting_period = "Quarter"
        
        # Fecha de Cierre (input text). Ex.: "31/12/2025". For irregular FS the date is in "1date16474"
        fs.period_end_date = safe_date(get_value(driver, "1date17466"), sep="/", day_first=True)

        fs_type = get_select_text(driver, "1select36921")
        if fs_type == "Individual": fs.financial_statements_type = "Separate"
        if fs_type == "Consolidado": fs.financial_statements_type = "Consolidated"
        
        fs.presentation_currency = get_select_text(driver, "1select22363")

        uom = get_select_text(driver, "1select111599")
        if uom == "$": fs.unit_of_measure = "Unit"
        if uom == "Miles de $": fs.unit_of_measure = "Thousands"
        if uom == "Millones de $": fs.unit_of_measure = "Millions"

        fs.accounting_standards_applied = get_select_text(driver, "1select34258")

        return fs