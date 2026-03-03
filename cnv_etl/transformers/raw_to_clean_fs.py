from typing import Dict

from cnv_etl.models.document import RawFinancialStatement, CleanFinancialStatement, CleanConceptValue
from cnv_etl.transformers.dates import parse_period_end_date, parse_cnv_datetime
from cnv_etl.transformers.numeric import parse_cnv_number_to_int, parse_cnv_number_to_float
from cnv_etl.transformers.taxonomy import map_to_xbrl
from cnv_etl.transformers.units import get_multiplier
from cnv_etl.transformers.boolean import parse_cnv_string_to_bool
from cnv_etl.transformers.literals import parse_cnv_reporting_period, parse_cnv_financial_statements_type, parse_accounting_standards_applied


def raw_to_clean_financial_statement(raw_fs: RawFinancialStatement) -> CleanFinancialStatement:

    multiplier = get_multiplier(str(raw_fs.unit_of_measure))

    concepts: Dict[str, CleanConceptValue] = dict()
    for concept_value in raw_fs.statement:
        concept = CleanConceptValue(
            map_to_xbrl(str(concept_value.label)),
            multiplier * parse_cnv_number_to_float(str(concept_value.value)),
            parse_cnv_number_to_int(str(concept_value.id))
        )
        concepts[concept.label] = concept
        
    fs = CleanFinancialStatement(
        parse_cnv_number_to_int(str(raw_fs.document_id)),
        str(raw_fs.document_description),
        str(raw_fs.document_link),
        parse_cnv_datetime(str(raw_fs.submission_date)),
        parse_cnv_reporting_period(str(raw_fs.document_description), str(raw_fs.reporting_period)),
        parse_period_end_date(str(raw_fs.document_description), str(raw_fs.period_end_date)),
        parse_cnv_financial_statements_type(str(raw_fs.document_description), str(raw_fs.financial_statements_type)),
        str(raw_fs.presentation_currency),
        parse_accounting_standards_applied(str(raw_fs.document_description), str(raw_fs.accounting_standards_applied)),
        parse_cnv_string_to_bool(str(raw_fs.merger_or_demerger_in_process)),
        parse_cnv_string_to_bool(str(raw_fs.treasury_shares)),
        parse_cnv_string_to_bool(str(raw_fs.insolvency_proceedings)),
        parse_cnv_string_to_bool(str(raw_fs.public_offering_of_equity_instruments)),
        parse_cnv_number_to_int(str(raw_fs.subscribed_shares_at_period_end)),
        parse_cnv_number_to_float(str(raw_fs.equity_share_price_at_period_end)),
        parse_cnv_number_to_float(str(raw_fs.free_float_percentage)),
        parse_cnv_number_to_int(str(raw_fs.number_of_employees)),
        concepts
    )

    return fs