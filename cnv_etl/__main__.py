"""
Entry point for python -m cnv_etl.

All logic lives in cli.py — this file stays intentionally thin.
"""

from cnv_etl.cli import main

if __name__ == "__main__":
    main()