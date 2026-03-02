import time

from cnv_etl.models.document import RawFinancialStatement
from cnv_etl.parsing.parsing_utils import get_value, get_select_text


class StatementMetadataParser:
    
    def parse(self, driver, fs: RawFinancialStatement) -> RawFinancialStatement:
        time.sleep(0.5)

        parsed_fs = fs.transform(
            reporting_period = get_select_text(driver, "1select18889"),
            period_end_date = get_value(driver, "1date17466"),
            financial_statements_type = get_select_text(driver, "1select36921"),
            presentation_currency = get_select_text(driver, "1select22363"),
            unit_of_measure = get_select_text(driver, "1select111599"),
            accounting_standards_applied = get_select_text(driver, "1select34258")
        )

        return parsed_fs