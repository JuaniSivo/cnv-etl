# cnv-etl

An ETL pipeline that scrapes financial statements from Argentina's [CNV](https://www.cnv.gov.ar/) (Comisión Nacional de Valores), transforms them into clean, structured data, and persists them to Excel or SQLite.

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Pipeline Stages](#pipeline-stages)
- [Output](#output)
- [Enrichment](#enrichment)
- [Profiling & Stats](#profiling--stats)
- [Logging](#logging)

---

## Overview

`cnv-etl` automates the collection and processing of XBRL-tagged financial statements filed by publicly listed Argentine companies. Given a list of companies and a date range, the pipeline:

1. **Scrapes** the CNV website using Selenium to retrieve available filings.
2. **Deduplicates** documents, preferring consolidated over separate statements.
3. **Enriches** raw statements with field-level overrides from a local CSV (useful for fixing missing or incorrect values).
4. **Transforms** raw strings into typed, structured data (dates, booleans, numbers, XBRL taxonomy labels).
5. **Loads** clean statements into Excel files and/or a SQLite database.

---

## Project Structure

```
cnv_etl/
├── cli.py                  # Argument parsing and entry point
├── pipeline.py             # Core ETL orchestration
├── config.py               # Central config loader (reads config.toml)
├── errors.py               # ETLError, CompanyStats, PipelineReport
├── logging_config.py       # Centralised logging setup
├── config.toml             # All runtime settings
├── scraping/
│   ├── session.py          # Selenium WebDriver factory & context manager
│   ├── navigator.py        # CNV page navigation
│   └── selenium_utils.py   # Wait helpers
├── parsing/
│   ├── documents_table.py  # Parses the filings listing table
│   ├── statement_metadata.py
│   ├── company_metadata.py
│   └── statement_values.py # Paginates and extracts concept rows
├── transformers/
│   ├── raw_to_clean_fs.py  # Orchestrates all transformations
│   ├── dates.py
│   ├── numeric.py
│   ├── boolean.py
│   ├── literals.py         # Reporting period, statement type, accounting standard
│   ├── taxonomy.py         # CNV label → XBRL concept mapping
│   └── units.py            # Unit-of-measure multipliers
├── enrichment/
│   ├── base_source.py      # Abstract enrichment source interface
│   ├── file_source.py      # CSV-backed implementation
│   └── enricher.py         # Applies overrides to raw statements
├── loaders/
│   ├── excel.py            # openpyxl-based Excel exporter
│   ├── sqlite.py           # SQLite loader (upsert/update modes)
│   └── schema.sql          # Database schema
├── models/
│   ├── company.py          # Company & Companies dataclasses
│   └── document.py         # Raw/Clean document & statement dataclasses
└── profiling/
    ├── run_stats.py        # JSON-serialisable run snapshot
    └── profiler.py         # Saves, loads, and displays profiling data
```

---

## Installation

**Requirements:** Python 3.11+, Google Chrome, and a matching [ChromeDriver](https://chromedriver.chromium.org/).

```bash
# Clone the repository
git clone https://github.com/your-org/cnv-etl.git
cd cnv-etl

# Install dependencies
pip install -r requirements.txt
```

**Key dependencies:**
- `selenium` — browser automation
- `openpyxl` — Excel I/O
- `tomllib` — TOML config parsing (standard library in Python 3.11+)

---

## Configuration

All settings live in `cnv_etl/config.toml`. No other file needs to be edited for normal use.

| Section | Key | Description |
|---|---|---|
| `pipeline` | `date_from`, `date_to` | Default date window for document search |
| `pipeline` | `exclude_keywords` | Descriptions containing these strings are filtered out |
| `scraping` | `base_url` | CNV base URL |
| `scraping` | `xpaths` | XPath selectors for page elements |
| `scraping` | `click_delay`, `tab_delay`, `page_delay` | Timing controls (seconds) |
| `parsing` | `statement_metadata_ids` | HTML element IDs for statement metadata fields |
| `parsing` | `company_metadata_ids` | HTML element IDs for company metadata fields |
| `transformers` | `units`, `boolean`, `reporting_period`, `statement_type`, `accounting_standard`, `taxonomy` | Mapping tables for all transformer modules |
| `profiling` | `data_dir`, `max_stored_runs` | Profiling output directory and retention cap |
| `enrichment` | `enrichments_file` | Path to `enrichments.csv` |
| `enrichment` | `companies` | Per-company enrichment registry (fields to patch) |

---

## Usage

The pipeline is invoked as a Python module.

```bash
# Run all companies using config.toml defaults
python -m cnv_etl

# Run specific companies
python -m cnv_etl --company GGAL BMA TECO2

# Override the date range
python -m cnv_etl --date-from 2023-01-01 --date-to 2023-12-31

# Update mode — skip statements already stored in the DB
python -m cnv_etl --mode update --output sqlite

# Write to both Excel and SQLite, replacing existing data
python -m cnv_etl --mode overwrite --output both

# Use a custom DB path
python -m cnv_etl --output sqlite --db-path data/custom.db

# Use a custom companies input file
python -m cnv_etl --companies-file data/input/my_companies.xlsx
```

### CLI Reference

| Flag | Default | Description |
|---|---|---|
| `--company TICKER [...]` | all companies | One or more tickers to process |
| `--date-from YYYY-MM-DD` | `config.toml` value | Start of document search window |
| `--date-to YYYY-MM-DD` | `config.toml` value | End of document search window |
| `--mode` | `overwrite` | `overwrite` replaces existing data; `update` skips already-stored statements |
| `--output` | `excel` | `excel`, `sqlite`, or `both` |
| `--db-path PATH` | `data/output/cnv.db` | SQLite file path (used with `sqlite` or `both`) |
| `--companies-file PATH` | `data/input/companies.xlsx` | Path to the input companies Excel file |
| `--stats [N]` | — | Print a summary table of the last N runs (default: 5) and exit |
| `--profile` | — | When used with `--stats`, also print cProfile output for the most recent run |

> **Note:** `--mode update` requires `--output sqlite` or `--output both`, as it needs a database to check for existing document IDs.

### Input File

The companies input file must be an Excel workbook with a sheet named `companies` containing the following columns:

| Column | Maps to | Required |
|---|---|---|
| `cuit` | `Company.id` | Yes |
| `company_name` | `Company.name` | Yes |
| `ticker` | `Company.ticker` | Yes |
| `company_description` | `Company.description` | No |
| `sector` | `Company.sector` | No |
| `industry_group` | `Company.industry_group` | No |
| `industry` | `Company.industry` | No |
| `subindustry` | `Company.sub_industry` | No |
| `subindustry_definition` | `Company.sub_industry_description` | No |

---

## Pipeline Stages

### 1. Fetch
Opens a browser session and navigates to each company's CNV filing page to retrieve the documents table within the configured date range. Documents whose description contains any `exclude_keywords` are discarded.

### 2. Deduplicate
For each period end date, retains a single document: consolidated statements are preferred over separate ones, and among duplicates of the same type, the most recently submitted document wins.

### 3. Enrich
Applies field-level overrides from `enrichments.csv` to raw statements before transformation. Only fields declared in `config.toml` under `[enrichment.companies.<TICKER>]` are patched.

### 4. Transform
Converts all raw string fields into typed Python values:
- **Dates:** `"31/12/2024"` → `date(2024, 12, 31)`
- **Datetimes:** `"13 oct. 2025 14:20"` → `datetime(2025, 10, 13, 14, 20)`
- **Numbers:** CNV format (`"1.234,56"`) → `float` or `int`
- **Booleans:** Configured true/false string lists → `bool`
- **Literals:** Reporting period, statement type, and accounting standard are resolved from both document description and statement metadata, with metadata taking precedence.
- **Taxonomy:** CNV Spanish labels are mapped to XBRL concept names. Unmapped labels are kept as-is with a warning.
- **Units:** Numeric values are scaled by the unit-of-measure multiplier declared in the statement.

### 5. Load
Persists clean statements to the configured output:
- **Excel:** One workbook per company with three sheets: `company_metadata`, `statement_metadata`, and `statement_values`.
- **SQLite:** Upserts into `companies`, `statement_metadata`, and `statement_values` tables. In `overwrite` mode uses `INSERT OR REPLACE`; in `update` mode uses `INSERT OR IGNORE`.

---

## Output

After each run, the following files are written to `data/output/`:

| File | Contents |
|---|---|
| `<TICKER>.xlsx` | Per-company Excel workbook (when `--output excel` or `both`) |
| `cnv.db` | SQLite database (when `--output sqlite` or `both`) |
| `pipeline_report.xlsx` | Run summary and full error log |

Log messages are written to both the console (`INFO` and above) and `logs/cnv_etl.log` (`DEBUG` and above).

---

## Enrichment

Some statements may have missing or incorrect metadata fields (e.g. share price, number of employees) that cannot be scraped from the CNV website. The enrichment system allows you to supply these values via a CSV file.

**`data/input/enrichments.csv` format:**
```csv
ticker,document_id,field,value
RIGO,123456,subscribed_shares_at_period_end,500000000
RIGO,123456,equity_share_price_at_period_end,12.50
```

**`config.toml` registry** — declare which fields are eligible for enrichment per company:
```toml
[enrichment.companies.RIGO]
fields = ["subscribed_shares_at_period_end", "equity_share_price_at_period_end"]
```

Enrichable fields are: `merger_or_demerger_in_process`, `treasury_shares`, `insolvency_proceedings`, `public_offering_of_equity_instruments`, `subscribed_shares_at_period_end`, `equity_share_price_at_period_end`, `price_earnings_ratio`, `market_capitalization`, `free_float_percentage`, `number_of_employees`.

---

## Profiling & Stats

Every run saves a JSON metrics snapshot and a cProfile binary to `data/profiling/`. The store automatically prunes old runs beyond the `max_stored_runs` limit set in `config.toml`.

```bash
# Show summary table for the last 5 runs
python -m cnv_etl --stats

# Show summary table for the last 10 runs
python -m cnv_etl --stats 10

# Show summary table + top-20 functions by cumulative time
python -m cnv_etl --stats --profile
```

---

## Logging

All modules use a shared logger obtained via:

```python
from cnv_etl.logging_config import get_logger
logger = get_logger(__name__)
```

| Handler | Level | Destination |
|---|---|---|
| Console | `INFO` | stdout |
| File | `DEBUG` | `logs/cnv_etl.log` |
