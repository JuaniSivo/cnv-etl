"""
Command-line interface for cnv_etl.

Usage examples
--------------
# Run all companies with config.toml defaults
python -m cnv_etl

# Run a single company
python -m cnv_etl --company GGAL

# Run multiple companies
python -m cnv_etl --company GGAL BMA TECO2

# Override date range
python -m cnv_etl --date-from 2023-01-01 --date-to 2023-12-31

# Update mode — only fetch statements not already in the DB
python -m cnv_etl --mode update --output sqlite

# Write to both Excel and SQLite, overwriting existing data
python -m cnv_etl --mode overwrite --output both

# Use a custom DB path
python -m cnv_etl --output sqlite --db-path data/custom.db
"""

import argparse
import sys
from datetime import date
from pathlib import Path
from typing import Optional

from cnv_etl.logging_config import setup_logging, get_logger
from cnv_etl.models.company import Companies
from cnv_etl.pipeline import FinancialStatementPipeline
from cnv_etl.loaders.excel import export_report_to_excel
from cnv_etl.errors import PipelineReport
from cnv_etl.config import (
    PIPELINE_DATE_FROM,
    PIPELINE_DATE_TO,
    EXCLUDE_KEYWORDS,
)

setup_logging()
logger = get_logger(__name__)

_DEFAULT_COMPANIES_PATH = Path("data/input/companies.xlsx")
_DEFAULT_OUTPUT_DIR     = Path("data/output")
_DEFAULT_DB_PATH        = Path("data/output/cnv.db")
_DEFAULT_REPORT_DIR     = Path("data/output/report")


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cnv_etl",
        description="ETL pipeline for CNV financial statements.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--company",
        nargs="+",
        metavar="TICKER",
        default=None,
        help=(
            "One or more company tickers to process. "
            "If omitted, all companies in the input file are processed."
        ),
    )

    parser.add_argument(
        "--date-from",
        metavar="YYYY-MM-DD",
        default=None,
        help=(
            f"Start of document search window. "
            f"Defaults to config.toml value ({PIPELINE_DATE_FROM})."
        ),
    )

    parser.add_argument(
        "--date-to",
        metavar="YYYY-MM-DD",
        default=None,
        help=(
            f"End of document search window. "
            f"Defaults to config.toml value ({PIPELINE_DATE_TO})."
        ),
    )

    parser.add_argument(
        "--mode",
        choices=["overwrite", "update"],
        default="overwrite",
        help=(
            "overwrite: replace all existing data for each company. "
            "update: skip statements already stored in the DB. "
            "Default: overwrite."
        ),
    )

    parser.add_argument(
        "--output",
        choices=["excel", "sqlite", "both"],
        default="excel",
        help="Where to persist clean statements. Default: excel.",
    )

    parser.add_argument(
        "--db-path",
        metavar="PATH",
        default=None,
        help=(
            f"Path to the SQLite database file. "
            f"Only used when --output is 'sqlite' or 'both'. "
            f"Default: {_DEFAULT_DB_PATH}."
        ),
    )

    parser.add_argument(
        "--companies-file",
        metavar="PATH",
        default=str(_DEFAULT_COMPANIES_PATH),
        help=(
            f"Path to the Excel file containing the companies list. "
            f"Default: {_DEFAULT_COMPANIES_PATH}."
        ),
    )

    return parser


# ---------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------

def _parse_date(value: str, flag: str) -> date:
    """Parse a YYYY-MM-DD string, exiting with a clear message on failure."""
    try:
        return date.fromisoformat(value)
    except ValueError:
        print(
            f"error: --{flag} '{value}' is not a valid date. "
            f"Expected format: YYYY-MM-DD.",
            file=sys.stderr,
        )
        sys.exit(1)


def _resolve_args(args: argparse.Namespace):
    """
    Validate and resolve parsed arguments into typed values.
    Returns a plain namespace with cleaned-up fields.
    """
    date_from = (
        _parse_date(args.date_from, "date-from")
        if args.date_from
        else PIPELINE_DATE_FROM
    )
    date_to = (
        _parse_date(args.date_to, "date-to")
        if args.date_to
        else PIPELINE_DATE_TO
    )

    if date_from > date_to:
        print(
            f"error: --date-from ({date_from}) must be before --date-to ({date_to}).",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.mode == "update" and args.output == "excel":
        print(
            "error: --mode update requires --output sqlite or --output both. "
            "Update mode needs a database to query existing document IDs.",
            file=sys.stderr,
        )
        sys.exit(1)

    db_path: Optional[Path] = None
    if args.output in ("sqlite", "both"):
        db_path = Path(args.db_path) if args.db_path else _DEFAULT_DB_PATH

    return argparse.Namespace(
        tickers       = [t.upper() for t in args.company] if args.company else None,
        date_from     = date_from,
        date_to       = date_to,
        mode          = args.mode,
        output        = args.output,
        db_path       = db_path,
        companies_file = Path(args.companies_file),
    )


# ---------------------------------------------------------------------------
# Company selection
# ---------------------------------------------------------------------------

def _select_companies(companies: Companies, tickers: Optional[list[str]]) -> list:
    """
    Return the list of Company objects to process.
    Exits with a clear message if any requested ticker is not found.
    """
    if tickers is None:
        return list(companies)

    selected = []
    missing  = []

    for ticker in tickers:
        try:
            selected.append(companies.get_by_ticker(ticker))
        except ValueError:
            missing.append(ticker)

    if missing:
        print(
            f"error: ticker(s) not found in companies file: {', '.join(missing)}\n"
            f"Available tickers: {', '.join(companies.by_ticker.keys())}",
            file=sys.stderr,
        )
        sys.exit(1)

    return selected


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: Optional[list[str]] = None) -> None:
    """
    Parse arguments, build the pipeline, and run it.

    Parameters
    ----------
    argv : list[str], optional
        Argument list for testing. Defaults to sys.argv when None.
    """
    parser = _build_parser()
    args   = _resolve_args(parser.parse_args(argv))

    # Load companies
    if not args.companies_file.exists():
        print(
            f"error: companies file not found: {args.companies_file}",
            file=sys.stderr,
        )
        sys.exit(1)

    companies = Companies()
    companies.load_from_excel(args.companies_file)

    if len(companies) == 0:
        print("error: no companies found in the input file.", file=sys.stderr)
        sys.exit(1)

    selected = _select_companies(companies, args.tickers)

    # Log run configuration
    logger.info("=" * 50)
    logger.info("cnv_etl pipeline starting")
    logger.info(f"  Companies : {', '.join(c.ticker for c in selected)}")
    logger.info(f"  Date from : {args.date_from}")
    logger.info(f"  Date to   : {args.date_to}")
    logger.info(f"  Mode      : {args.mode}")
    logger.info(f"  Output    : {args.output}")
    if args.db_path:
        logger.info(f"  DB path   : {args.db_path}")
    logger.info("=" * 50)

    # Run pipeline
    report = PipelineReport()

    with FinancialStatementPipeline(
        date_from = args.date_from,
        date_to   = args.date_to,
        exclude   = EXCLUDE_KEYWORDS,
        run_mode  = args.mode,
        output    = args.output,
        db_path   = args.db_path,
        report    = report,
    ) as pipeline:
        for company in selected:
            try:
                pipeline.run(company)
            except Exception as e:
                logger.error(
                    f"Pipeline failed for {company.name}. "
                    f"{type(e).__name__}: {e}"
                )

    # Summary
    logger.info(report.summary())

    _DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    try:
        export_report_to_excel(report, _DEFAULT_REPORT_DIR / "pipeline_report.xlsx")
    except Exception as e:
        logger.error(f"Could not save pipeline report. {type(e).__name__}: {e}")