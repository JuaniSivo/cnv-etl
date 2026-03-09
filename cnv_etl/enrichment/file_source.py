"""
CSV-backed enrichment source.

Reads enrichments.csv once at init time and builds an in-memory lookup
structure. Subsequent load() calls are pure dict lookups — no I/O.

Expected CSV format
-------------------
ticker,document_id,field,value
RIGO,123456,subscribed_shares_at_period_end,500000000
RIGO,123456,equity_share_price_at_period_end,12.50

Rules
-----
- ticker and document_id are case-sensitive.
- field must match a RawFinancialStatement attribute name exactly.
- value is kept as a raw string — type parsing is left to the transformer.
- Rows with missing or empty required columns are skipped with a warning.
- Duplicate (ticker, document_id, field) entries: last row wins, with a warning.
"""

import csv
from pathlib import Path

from cnv_etl.enrichment.base_source import BaseSource
from cnv_etl.logging_config import get_logger

logger = get_logger(__name__)

_REQUIRED_COLUMNS = {"ticker", "document_id", "field", "value"}

# Fields that are valid enrichment targets on RawFinancialStatement.
# Acts as a safeguard against typos in the CSV that would silently
# produce no effect after transform().
_VALID_FIELDS = {
    "merger_or_demerger_in_process",
    "treasury_shares",
    "insolvency_proceedings",
    "public_offering_of_equity_instruments",
    "subscribed_shares_at_period_end",
    "equity_share_price_at_period_end",
    "price_earnings_ratio",
    "market_capitalization",
    "free_float_percentage",
    "number_of_employees",
}


class FileSource(BaseSource):
    """
    Enrichment source backed by a local CSV file.

    Parameters
    ----------
    file_path : Path | str
        Path to enrichments.csv.

    Raises
    ------
    FileNotFoundError
        If the CSV file does not exist at init time.
    """

    def __init__(self, file_path: Path | str) -> None:
        self._path = Path(file_path)
        self._data: dict[str, dict[str, dict[str, str]]] = {}
        self._load_csv()

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def load(self, ticker: str) -> dict[str, dict[str, str]]:
        """
        Return all enrichment values for a given ticker.

        Parameters
        ----------
        ticker : str
            Company ticker to look up (e.g. "RIGO").

        Returns
        -------
        dict[str, dict[str, str]]
            Outer key  : document_id as string.
            Inner key  : field name.
            Inner value: raw string value.
            Empty dict if no overrides exist for this ticker.
        """
        return self._data.get(ticker, {})

    # ------------------------------------------------------------------ #
    # Private                                                              #
    # ------------------------------------------------------------------ #

    def _load_csv(self) -> None:
        """
        Read the CSV file and populate self._data.

        Structure:
            self._data[ticker][document_id][field] = value
        """
        if not self._path.exists():
            raise FileNotFoundError(
                f"Enrichments file not found: {self._path}. "
                f"Create the file or update 'enrichments_file' in config.toml."
            )

        rows_loaded  = 0
        rows_skipped = 0

        with open(self._path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            missing_cols = _REQUIRED_COLUMNS - set(reader.fieldnames or [])
            if missing_cols:
                raise ValueError(
                    f"enrichments.csv is missing required columns: {missing_cols}. "
                    f"Expected columns: {sorted(_REQUIRED_COLUMNS)}."
                )

            for line_num, row in enumerate(reader, start=2):
                ticker      = row.get("ticker", "").strip()
                document_id = row.get("document_id", "").strip()
                field       = row.get("field", "").strip()
                value       = row.get("value", "").strip()

                # --- Validate required columns are non-empty ---
                if not all([ticker, document_id, field, value]):
                    logger.warning(
                        f"enrichments.csv line {line_num}: "
                        f"skipping row with empty required field(s). Row: {dict(row)}"
                    )
                    rows_skipped += 1
                    continue

                # --- Validate field name ---
                if field not in _VALID_FIELDS:
                    logger.warning(
                        f"enrichments.csv line {line_num}: "
                        f"unknown field '{field}' — skipping. "
                        f"Valid fields: {sorted(_VALID_FIELDS)}"
                    )
                    rows_skipped += 1
                    continue

                # --- Warn on duplicate (ticker, document_id, field) ---
                existing = self._data.get(ticker, {}).get(document_id, {})
                if field in existing:
                    logger.warning(
                        f"enrichments.csv line {line_num}: "
                        f"duplicate entry for ({ticker}, {document_id}, {field}). "
                        f"Overwriting '{existing[field]}' with '{value}'."
                    )

                # --- Store ---
                self._data.setdefault(ticker, {}).setdefault(document_id, {})[field] = value
                rows_loaded += 1

        logger.info(
            f"FileSource loaded {rows_loaded} enrichment(s) from {self._path} "
            f"({rows_skipped} skipped)."
        )