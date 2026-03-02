"""
Parser for company-level metadata associated with a filing.

Extracts non-balance information such as shares, employees, merger
status, and public offerings from the CNV 'Otros Datos' section.
"""

import time

from cnv_scraper.domain.document import FinancialStatement
from cnv_scraper.parsing.parsing_utils import get_value, get_select_text, safe_int, safe_float

class CompanyMetadataParser:
    """
    Parser for the 'Otros Datos' section of a financial statement.

    Extracts company-level metadata such as:
    - shares outstanding
    - employees
    - mergers or splits
    - public offerings

    Returns a flat dictionary of label-value pairs.
    """

    def parse(self, driver, fs: FinancialStatement) -> FinancialStatement:
        time.sleep(1)

        # ---- SELECT FIELDS (Si / No) ----
        fs.merger_or_demerger_in_process = get_select_text(driver, "1select99591")
        fs.treasury_shares = get_select_text(driver, "1select21271")
        fs.insolvency_proceedings = get_select_text(driver, "1select18901")
        fs.public_offering_of_equity_instruments = get_select_text(driver, "1select496788")

        # ---- NUMERIC / TEXT INPUTS ----
        fs.subscribed_shares_at_period_end = safe_int(get_value(driver, "1decimal21466"))
        fs.equity_share_price_at_period_end = safe_float(get_value(driver, "1decimal61378"))
        fs.price_earnings_ratio = safe_float(get_value(driver, "1decimal42743"))
        fs.market_capitalization = safe_float(get_value(driver, "1decimal17252"))
        fs.free_float_percentage = safe_float(get_value(driver, "1decimal46286"))
        fs.number_of_employees = safe_int(get_value(driver, "1textbox16696"))

        return fs