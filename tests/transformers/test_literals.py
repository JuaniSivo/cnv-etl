"""
Tests for cnv_etl.transformers.literals.

These functions parse Literals from document descriptions and metadata
strings. They are the most regex-heavy part of the transformer layer.
"""

import pytest

from cnv_etl.transformers.literals import (
    parse_reporting_period,
    parse_reporting_period_from_description,
    parse_reporting_period_from_metadata,
    parse_financial_statements_type,
    parse_statements_type_from_description,
    parse_statements_type_from_statement_metadata,
    parse_accounting_standards_applied,
    parse_accounting_standard_from_description,
    parse_accounting_standard_from_statement_metadata,
)


# ---------------------------------------------------------------------------
# Reporting period
# ---------------------------------------------------------------------------

class TestParseReportingPeriodFromDescription:

    def test_annual(self):
        assert parse_reporting_period_from_description("PERIODICIDAD: ANUAL") == "Annual"

    def test_semester(self):
        assert parse_reporting_period_from_description("PERIODICIDAD: SEMESTRAL") == "Semester"

    def test_quarter(self):
        assert parse_reporting_period_from_description("PERIODICIDAD: TRIMESTRAL") == "Quarter"

    def test_embedded_in_longer_description(self):
        desc = "TIPO BALANCE: CONSOLIDADO - PERIODICIDAD: ANUAL - FECHA CIERRE: 2024-12-31"
        assert parse_reporting_period_from_description(desc) == "Annual"

    def test_missing_returns_none(self):
        assert parse_reporting_period_from_description("NO PERIOD HERE") is None

    def test_empty_string_returns_none(self):
        assert parse_reporting_period_from_description("") is None


class TestParseReportingPeriodFromMetadata:

    def test_known_value(self):
        # The exact strings here depend on your config.toml REPORTING_PERIOD mapping
        # Adjust if your mapping keys differ
        result = parse_reporting_period_from_metadata("ANUAL")
        assert result == "Annual"

    def test_unknown_value_returns_none(self):
        assert parse_reporting_period_from_metadata("UNKNOWN_PERIOD") is None


class TestParseReportingPeriod:

    def test_metadata_preferred_over_description(self):
        desc   = "PERIODICIDAD: ANUAL"
        meta   = "SEMESTRAL"
        result = parse_reporting_period(desc, meta)
        assert result == "Semester"

    def test_falls_back_to_description(self):
        desc   = "PERIODICIDAD: ANUAL"
        meta   = "UNRECOGNISED"
        result = parse_reporting_period(desc, meta)
        assert result == "Annual"

    def test_raises_when_both_fail(self):
        with pytest.raises(ValueError, match="Cannot parse reporting period"):
            parse_reporting_period("NO PERIOD", "UNRECOGNISED")


# ---------------------------------------------------------------------------
# Financial statements type
# ---------------------------------------------------------------------------

class TestParseStatementsTypeFromDescription:

    def test_consolidated(self):
        assert parse_statements_type_from_description("TIPO BALANCE: CONSOLIDADO") == "Consolidated"

    def test_separate(self):
        assert parse_statements_type_from_description("TIPO BALANCE: INDIVIDUAL") == "Separate"

    def test_embedded_in_longer_description(self):
        desc = "PERIODICIDAD: ANUAL - TIPO BALANCE: CONSOLIDADO - FECHA CIERRE: 2024-12-31"
        assert parse_statements_type_from_description(desc) == "Consolidated"

    def test_missing_returns_none(self):
        assert parse_statements_type_from_description("NO TYPE HERE") is None


class TestParseFinancialStatementsType:

    def test_metadata_preferred(self):
        desc   = "TIPO BALANCE: INDIVIDUAL"
        meta   = "CONSOLIDADO"
        result = parse_financial_statements_type(desc, meta)
        assert result == "Consolidated"

    def test_falls_back_to_description(self):
        desc   = "TIPO BALANCE: CONSOLIDADO"
        meta   = "UNRECOGNISED"
        result = parse_financial_statements_type(desc, meta)
        assert result == "Consolidated"

    def test_raises_when_both_fail(self):
        with pytest.raises(ValueError, match="Cannot parse statements type"):
            parse_financial_statements_type("NO TYPE", "UNRECOGNISED")


# ---------------------------------------------------------------------------
# Accounting standards
# ---------------------------------------------------------------------------

class TestParseAccountingStandardFromDescription:

    def test_ifrs(self):
        assert parse_accounting_standard_from_description("NORMA CONTABLE: NIIF") == "IFRS"

    def test_embedded_in_longer_description(self):
        desc = "PERIODICIDAD: ANUAL - TIPO BALANCE: CONSOLIDADO - NORMA CONTABLE: NIIF"
        assert parse_accounting_standard_from_description(desc) == "IFRS"

    def test_missing_returns_none(self):
        assert parse_accounting_standard_from_description("NO STANDARD HERE") is None


class TestParseAccountingStandardsApplied:

    def test_metadata_preferred(self):
        desc   = "NORMA CONTABLE: NIIF"
        meta   = "NIIF"
        result = parse_accounting_standards_applied(desc, meta)
        assert result == "IFRS"

    def test_falls_back_to_description(self):
        desc   = "NORMA CONTABLE: NIIF"
        meta   = "UNRECOGNISED"
        result = parse_accounting_standards_applied(desc, meta)
        assert result == "IFRS"

    def test_raises_when_both_fail(self):
        with pytest.raises(ValueError, match="Cannot parse accounting standard"):
            parse_accounting_standards_applied("NO STANDARD", "UNRECOGNISED")