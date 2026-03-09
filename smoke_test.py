"""
Smoke test script for the cnv_etl CLI.

Designed for manual runs and VS Code debugger sessions.
Set breakpoints anywhere in the stack — this script is the entry point.

Run from the project root:
    python smoke_test.py

Or via the VS Code launch config defined in .vscode/launch.json.
"""

from cnv_etl.cli import main

# ---------------------------------------------------------------------------
# Configure your smoke test run here
# ---------------------------------------------------------------------------

TICKER    = "RIGO"        # single ticker to keep the run short
DATE_FROM = "2019-01-01"
DATE_TO   = "2026-03-06"
MODE      = "update"      # overwrite | update
OUTPUT    = "sqlite"      # excel | sqlite | both

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main([
        "--company",   TICKER,
        "--date-from", DATE_FROM,
        "--date-to",   DATE_TO,
        "--mode",      MODE,
        "--output",    OUTPUT,
    ])