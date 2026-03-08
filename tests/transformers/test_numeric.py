"""
Tests for cnv_etl.transformers.numeric.

CNV number format uses:
  - periods (.) as thousands separators
  - commas (,) as decimal separators
"""

import pytest
from cnv_etl.transformers.numeric import parse_cnv_number_to_float, parse_cnv_number_to_int


# ---------------------------------------------------------------------------
# parse_cnv_number_to_float
# ---------------------------------------------------------------------------

class TestParseCnvNumberToFloat:

    def test_integer_string(self):
        assert parse_cnv_number_to_float("1234") == 1234.0

    def test_thousands_separator(self):
        assert parse_cnv_number_to_float("1.234") == 1234.0

    def test_decimal_comma(self):
        assert parse_cnv_number_to_float("1.234,56") == 1234.56

    def test_large_number(self):
        assert parse_cnv_number_to_float("1.234.567,89") == 1234567.89

    def test_negative(self):
        assert parse_cnv_number_to_float("-1.234,56") == -1234.56

    def test_zero(self):
        assert parse_cnv_number_to_float("0") == 0.0

    def test_zero_decimal(self):
        assert parse_cnv_number_to_float("0,00") == 0.0

    def test_whitespace_stripped(self):
        assert parse_cnv_number_to_float("  1.234,56  ") == 1234.56

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="Empty"):
            parse_cnv_number_to_float("")

    def test_blank_string_raises(self):
        with pytest.raises(ValueError, match="Empty"):
            parse_cnv_number_to_float("   ")

    def test_non_numeric_raises(self):
        with pytest.raises(ValueError):
            parse_cnv_number_to_float("not_a_number")

    def test_none_like_string_raises(self):
        with pytest.raises(ValueError):
            parse_cnv_number_to_float("None")


# ---------------------------------------------------------------------------
# parse_cnv_number_to_int
# ---------------------------------------------------------------------------

class TestParseCnvNumberToInt:

    def test_plain_integer(self):
        assert parse_cnv_number_to_int("1234") == 1234

    def test_thousands_separator(self):
        assert parse_cnv_number_to_int("1.234") == 1234

    def test_large_cuit(self):
        assert parse_cnv_number_to_int("30.123.456.789") == 30123456789

    def test_zero_decimal_accepted(self):
        """1.234,00 is an integer value written with a decimal — should parse fine."""
        assert parse_cnv_number_to_int("1.234,00") == 1234

    def test_negative_integer(self):
        assert parse_cnv_number_to_int("-1.234") == -1234

    def test_non_zero_fractional_raises(self):
        """Silent truncation of -1.7 → -1 must not happen."""
        with pytest.raises(ValueError, match="non-zero fractional"):
            parse_cnv_number_to_int("1.234,56")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="Empty"):
            parse_cnv_number_to_int("")

    def test_blank_string_raises(self):
        with pytest.raises(ValueError, match="Empty"):
            parse_cnv_number_to_int("   ")

    def test_non_numeric_raises(self):
        with pytest.raises(ValueError):
            parse_cnv_number_to_int("not_a_number")