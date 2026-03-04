import time

from cnv_etl.config import STATEMENT_METADATA_IDS, TAB_DELAY
from cnv_etl.models.document import RawFinancialStatement
from cnv_etl.parsing.parsing_utils import get_value, get_select_text


class StatementMetadataParser:

    def parse(self, driver, fs: RawFinancialStatement) -> RawFinancialStatement:
        time.sleep(TAB_DELAY)
        ids = STATEMENT_METADATA_IDS

        parsed_fs = fs.transform(
            reporting_period             = get_select_text(driver, ids["reporting_period"]),
            period_end_date              = get_value(driver, ids["period_end_date"]),
            financial_statements_type    = get_select_text(driver, ids["financial_statements_type"]),
            presentation_currency        = get_select_text(driver, ids["presentation_currency"]),
            unit_of_measure              = get_select_text(driver, ids["unit_of_measure"]),
            accounting_standards_applied = get_select_text(driver, ids["accounting_standards_applied"])
        )

        return parsed_fs