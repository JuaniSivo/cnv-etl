"""
Tests for cnv_etl.cli argument parsing and validation.

The pipeline itself is mocked so no browser or file I/O is needed.
"""

import pytest
from unittest.mock import MagicMock, patch
from cnv_etl.cli import _build_parser, _resolve_args, _select_companies
from cnv_etl.models.company import Company, Companies


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse(args: list[str]):
    """Parse a list of CLI args and resolve them."""
    parser = _build_parser()
    return _resolve_args(parser.parse_args(args))


def _make_companies(*tickers: str) -> Companies:
    """Build a Companies registry with minimal Company objects."""
    companies = Companies()
    for i, ticker in enumerate(tickers, start=1):
        companies.add(Company(id=i, name=f"Company {ticker}", ticker=ticker))
    return companies


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

class TestDateParsing:

    def test_valid_date_from(self):
        from datetime import date
        args = _parse(["--date-from", "2023-01-01"])
        assert args.date_from == date(2023, 1, 1)

    def test_valid_date_to(self):
        from datetime import date
        args = _parse(["--date-to", "2023-12-31"])
        assert args.date_to == date(2023, 12, 31)

    def test_invalid_date_from_exits(self):
        with pytest.raises(SystemExit):
            _parse(["--date-from", "not-a-date"])

    def test_invalid_date_to_exits(self):
        with pytest.raises(SystemExit):
            _parse(["--date-to", "31-12-2023"])

    def test_date_from_after_date_to_exits(self):
        with pytest.raises(SystemExit):
            _parse(["--date-from", "2024-12-31", "--date-to", "2024-01-01"])

    def test_same_date_from_and_to_is_valid(self):
        from datetime import date
        args = _parse(["--date-from", "2024-06-30", "--date-to", "2024-06-30"])
        assert args.date_from == args.date_to == date(2024, 6, 30)


# ---------------------------------------------------------------------------
# Mode and output validation
# ---------------------------------------------------------------------------

class TestModeOutputValidation:

    def test_update_mode_with_excel_exits(self):
        """update mode needs a DB to query — excel alone is invalid."""
        with pytest.raises(SystemExit):
            _parse(["--mode", "update", "--output", "excel"])

    def test_update_mode_with_sqlite_is_valid(self):
        args = _parse(["--mode", "update", "--output", "sqlite"])
        assert args.mode   == "update"
        assert args.output == "sqlite"

    def test_update_mode_with_both_is_valid(self):
        args = _parse(["--mode", "update", "--output", "both"])
        assert args.mode   == "update"
        assert args.output == "both"

    def test_overwrite_with_excel_is_default(self):
        args = _parse([])
        assert args.mode   == "overwrite"
        assert args.output == "excel"

    def test_db_path_resolved_when_sqlite(self, tmp_path):
        path = str(tmp_path / "custom.db")
        args = _parse(["--output", "sqlite", "--db-path", path])
        assert args.db_path is not None
        assert str(args.db_path) == path

    def test_db_path_none_when_excel_only(self):
        args = _parse(["--output", "excel"])
        assert args.db_path is None


# ---------------------------------------------------------------------------
# Company selection
# ---------------------------------------------------------------------------

class TestSelectCompanies:

    def test_none_tickers_returns_all(self):
        companies = _make_companies("GGAL", "BMA", "TECO2")
        selected  = _select_companies(companies, None)
        assert len(selected) == 3

    def test_single_ticker_returns_one(self):
        companies = _make_companies("GGAL", "BMA")
        selected  = _select_companies(companies, ["GGAL"])
        assert len(selected) == 1
        assert selected[0].ticker == "GGAL"

    def test_multiple_tickers_returned_in_order(self):
        companies = _make_companies("GGAL", "BMA", "TECO2")
        selected  = _select_companies(companies, ["TECO2", "GGAL"])
        tickers   = [c.ticker for c in selected]
        assert tickers == ["TECO2", "GGAL"]

    def test_unknown_ticker_exits(self):
        companies = _make_companies("GGAL", "BMA")
        with pytest.raises(SystemExit):
            _select_companies(companies, ["UNKNOWN"])

    def test_partially_unknown_tickers_exits(self):
        companies = _make_companies("GGAL", "BMA")
        with pytest.raises(SystemExit):
            _select_companies(companies, ["GGAL", "UNKNOWN"])

    def test_tickers_uppercased(self):
        companies = _make_companies("GGAL")
        args      = _parse(["--company", "ggal"])
        assert args.tickers == ["GGAL"]


# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------

class TestConfigDefaults:

    def test_dates_default_to_config(self):
        from cnv_etl.config import PIPELINE_DATE_FROM, PIPELINE_DATE_TO
        args = _parse([])
        assert args.date_from == PIPELINE_DATE_FROM
        assert args.date_to   == PIPELINE_DATE_TO

    def test_default_output_is_excel(self):
        assert _parse([]).output == "excel"

    def test_default_mode_is_overwrite(self):
        assert _parse([]).mode == "overwrite"