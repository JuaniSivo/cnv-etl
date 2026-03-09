"""
Central configuration loader.

Reads config.toml once at import time and exposes typed sections
as module-level constants. All other modules import from here —
nobody reads the TOML file directly.

Usage
-----
from cnv_etl.config import TAXONOMY, UNITS, ELEMENT_IDS, ...
"""

import tomllib
from pathlib import Path
from datetime import date
from typing import Any

# ---------------------------------------------------------------------------
# Load raw TOML
# ---------------------------------------------------------------------------

_CONFIG_PATH = Path(__file__).parent / "config.toml"

with open(_CONFIG_PATH, "rb") as _f:
    _cfg: dict[str, Any] = tomllib.load(_f)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

PIPELINE_DATE_FROM: date      = date.fromisoformat(_cfg["pipeline"]["date_from"])
PIPELINE_DATE_TO:   date      = date.fromisoformat(_cfg["pipeline"]["date_to"])
EXCLUDE_KEYWORDS:   list[str] = _cfg["pipeline"]["exclude_keywords"]


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

BASE_URL: str            = _cfg["scraping"]["base_url"]
XPATHS:   dict[str, str] = _cfg["scraping"]["xpaths"]

# Delay constants (seconds) — used by selenium_utils and parsers
CLICK_DELAY: float = _cfg["scraping"]["click_delay"]
TAB_DELAY:   float = _cfg["scraping"]["tab_delay"]
PAGE_DELAY:  float = _cfg["scraping"]["page_delay"]


# ---------------------------------------------------------------------------
# Parsing — element IDs
# ---------------------------------------------------------------------------

STATEMENT_METADATA_IDS: dict[str, str]         = _cfg["parsing"]["statement_metadata_ids"]
COMPANY_METADATA_IDS:   dict[str, str | list]  = _cfg["parsing"]["company_metadata_ids"]


# ---------------------------------------------------------------------------
# Transformers — mapping tables
# ---------------------------------------------------------------------------

UNITS:               dict[str, int] = _cfg["transformers"]["units"]
BOOL_TRUE_VALUES:    list[str]      = _cfg["transformers"]["boolean"]["true_values"]
BOOL_FALSE_VALUES:   list[str]      = _cfg["transformers"]["boolean"]["false_values"]
REPORTING_PERIOD:    dict[str, str] = _cfg["transformers"]["reporting_period"]
STATEMENT_TYPE:      dict[str, str] = _cfg["transformers"]["statement_type"]
ACCOUNTING_STANDARD: dict[str, str] = _cfg["transformers"]["accounting_standard"]
TAXONOMY:            dict[str, str] = _cfg["transformers"]["taxonomy"]


# ---------------------------------------------------------------------------
# Profiling
# ---------------------------------------------------------------------------

PROFILING_DATA_DIR:    Path = Path(_cfg["profiling"]["data_dir"])
PROFILING_MAX_RUNS:    int  = _cfg["profiling"]["max_stored_runs"]