import time

from cnv_etl.models.document import RawFinancialStatement
from cnv_etl.parsing.parsing_utils import get_value, get_select_text

class CompanyMetadataParser:

    def parse(self, driver, fs: RawFinancialStatement) -> RawFinancialStatement:
        time.sleep(0.5)

        parsed_fs = fs.transform(
            merger_or_demerger_in_process = get_select_text(driver, "1select99591"),
            treasury_shares = get_select_text(driver, "1select21271"),
            insolvency_proceedings = get_select_text(driver, "1select18901"),
            public_offering_of_equity_instruments = get_select_text(driver, "1select496788"),
            subscribed_shares_at_period_end = get_value(driver, "1decimal21466"),
            equity_share_price_at_period_end = get_value(driver, "1decimal61378"),
            price_earnings_ratio = get_value(driver, "1decimal42743"),
            market_capitalization = get_value(driver, "1decimal17252"),
            free_float_percentage = get_value(driver, "1decimal46286"),
            number_of_employees = get_value(driver, "1textbox16696")
        )

        return parsed_fs