"""
Tests for cnv_etl.transformers.dates.
"""

import pytest
from datetime import date, datetime

from cnv_etl.transformers.dates import (
    parse_period_end_date,
    parse_period_end_date_from_description,
    parse_period_end_date_from_metadata,
    parse_cnv_datetime,
)


# ---------------------------------------------------------------------------
# parse_period_end_date_from_description
# ---------------------------------------------------------------------------

class TestParsePeriodEndDateFromDescription:

    def test_standard_format(self):
        desc = "PERIODICIDAD: ANUAL - TIPO BALANCE: CONSOLIDADO - FECHA CIERRE: 2024-12-31"
        assert parse_period_end_date_from_description(desc) == date(2024, 12, 31)

    def test_mid_year_date(self):
        desc = "FECHA CIERRE: 2024-06-30"
        assert parse_period_end_date_from_description(desc) == date(2024, 6, 30)

    def test_no_date_returns_none(self):
        assert parse_period_end_date_from_description("NO DATE HERE") is None

    def test_empty_string_returns_none(self):
        assert parse_period_end_date_from_description("") is None

    def test_extra_whitespace_around_colon(self):
        desc = "FECHA CIERRE  :  2024-03-31"
        assert parse_period_end_date_from_description(desc) == date(2024, 3, 31)


# ---------------------------------------------------------------------------
# parse_period_end_date_from_metadata
# ---------------------------------------------------------------------------

class TestParsePeriodEndDateFromMetadata:

    def test_standard_cnv_format(self):
        assert parse_period_end_date_from_metadata("31/12/2024") == date(2024, 12, 31)

    def test_mid_year(self):
        assert parse_period_end_date_from_metadata("30/06/2023") == date(2023, 6, 30)

    def test_invalid_format_returns_none(self):
        assert parse_period_end_date_from_metadata("2024-12-31") is None

    def test_empty_string_returns_none(self):
        assert parse_period_end_date_from_metadata("") is None

    def test_garbage_returns_none(self):
        assert parse_period_end_date_from_metadata("not_a_date") is None


# ---------------------------------------------------------------------------
# parse_period_end_date (combined, with fallback logic)
# ---------------------------------------------------------------------------

class TestParsePeriodEndDate:

    def test_metadata_takes_priority(self):
        """When both sources are valid, metadata is preferred."""
        desc   = "FECHA CIERRE: 2024-12-31"
        meta   = "30/06/2024"
        result = parse_period_end_date(desc, meta)
        assert result == date(2024, 6, 30)

    def test_falls_back_to_description(self):
        """When metadata is unparseable, description is used."""
        desc   = "FECHA CIERRE: 2024-12-31"
        meta   = "not_a_date"
        result = parse_period_end_date(desc, meta)
        assert result == date(2024, 12, 31)

    def test_raises_when_both_fail(self):
        with pytest.raises(ValueError, match="Cannot parse date"):
            parse_period_end_date("NO DATE", "not_a_date")


# ---------------------------------------------------------------------------
# parse_cnv_datetime
# ---------------------------------------------------------------------------

class TestParseCnvDatetime:

    def test_october(self):
        assert parse_cnv_datetime("13 oct. 2025 14:20") == datetime(2025, 10, 13, 14, 20)

    def test_january(self):
        assert parse_cnv_datetime("01 ene. 2024 09:05") == datetime(2024, 1, 1, 9, 5)

    def test_december(self):
        assert parse_cnv_datetime("31 dic. 2023 23:59") == datetime(2023, 12, 31, 23, 59)

    def test_all_months(self):
        """Verify every Spanish abbreviation maps to the correct month number."""
        cases = [
            ("15 ene. 2024 00:00", 1),
            ("15 feb. 2024 00:00", 2),
            ("15 mar. 2024 00:00", 3),
            ("15 abr. 2024 00:00", 4),
            ("15 may. 2024 00:00", 5),
            ("15 jun. 2024 00:00", 6),
            ("15 jul. 2024 00:00", 7),
            ("15 ago. 2024 00:00", 8),
            ("15 sep. 2024 00:00", 9),
            ("15 oct. 2024 00:00", 10),
            ("15 nov. 2024 00:00", 11),
            ("15 dic. 2024 00:00", 12),
        ]
        for s, expected_month in cases:
            assert parse_cnv_datetime(s).month == expected_month, f"Failed for: {s}"

    def test_leading_trailing_whitespace(self):
        result = parse_cnv_datetime("  13 oct. 2025 14:20  ")
        assert result == datetime(2025, 10, 13, 14, 20)