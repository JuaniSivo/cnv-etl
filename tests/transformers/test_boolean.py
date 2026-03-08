"""
Tests for cnv_etl.transformers.boolean.
"""

import pytest
from cnv_etl.transformers.boolean import parse_cnv_string_to_bool


class TestParseCnvStringToBool:

    def test_true_values(self):
        """All configured true values should return True."""
        from cnv_etl.config import BOOL_TRUE_VALUES
        for v in BOOL_TRUE_VALUES:
            assert parse_cnv_string_to_bool(v) is True, f"Expected True for '{v}'"

    def test_false_values(self):
        """All configured false values should return False."""
        from cnv_etl.config import BOOL_FALSE_VALUES
        for v in BOOL_FALSE_VALUES:
            assert parse_cnv_string_to_bool(v) is False, f"Expected False for '{v}'"

    def test_unrecognised_raises(self):
        with pytest.raises(ValueError, match="not a recognised boolean"):
            parse_cnv_string_to_bool("MAYBE")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            parse_cnv_string_to_bool("")

    def test_returns_bool_type(self):
        """Return type must be bool, not a truthy/falsy string."""
        from cnv_etl.config import BOOL_TRUE_VALUES
        result = parse_cnv_string_to_bool(BOOL_TRUE_VALUES[0])
        assert type(result) is bool