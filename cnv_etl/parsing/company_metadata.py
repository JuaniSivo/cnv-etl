import time

from cnv_etl.config import COMPANY_METADATA_IDS
from cnv_etl.models.document import RawFinancialStatement
from cnv_etl.parsing.parsing_utils import get_value, get_select_text


class CompanyMetadataParser:

    def parse(self, driver, fs: RawFinancialStatement) -> RawFinancialStatement:
        time.sleep(0.5)
        ids = COMPANY_METADATA_IDS

        parsed_fs = fs.transform(
            merger_or_demerger_in_process          = get_select_text(driver, ids["merger_or_demerger_in_process"]),
            treasury_shares                        = get_select_text(driver, ids["treasury_shares"]),
            insolvency_proceedings                 = get_select_text(driver, ids["insolvency_proceedings"]),
            public_offering_of_equity_instruments  = get_select_text(driver, ids["public_offering_of_equity_instruments"]),
            subscribed_shares_at_period_end        = get_value(driver, ids["subscribed_shares_at_period_end"]),
            equity_share_price_at_period_end       = get_value(driver, ids["equity_share_price_at_period_end"]),
            price_earnings_ratio                   = get_value(driver, ids["price_earnings_ratio"]),
            market_capitalization                  = get_value(driver, ids["market_capitalization"]),
            free_float_percentage                  = get_value(driver, ids["free_float_percentage"]),
            number_of_employees                    = get_value(driver, ids["number_of_employees"])
        )

        return parsed_fs