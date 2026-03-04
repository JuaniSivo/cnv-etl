"""
Central logging configuration for cnv_etl.

All modules obtain their logger via:

    from cnv_etl.logging_config import get_logger
    logger = get_logger(__name__)

This module configures the root "cnv_etl" logger once. Any module-level
logger created with get_logger(__name__) inherits its handlers and level
automatically, so there is no need to call setup_logging() more than once.
"""

import logging
import sys
from pathlib import Path

_LOG_DIR  = Path("logs")
_LOG_FILE = _LOG_DIR / "cnv_etl.log"

_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_configured = False


def setup_logging(level: int = logging.DEBUG) -> None:
    """
    Configure the root cnv_etl logger with a console and a file handler.

    Safe to call multiple times — only runs once.

    Parameters
    ----------
    level : int
        Logging level for both handlers. Defaults to DEBUG.
    """
    global _configured
    if _configured:
        return

    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger("cnv_etl")
    root_logger.setLevel(level)

    # Console handler — INFO and above so the terminal stays readable
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT))

    # File handler — DEBUG and above so nothing is lost
    file_handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT))

    root_logger.addHandler(console)
    root_logger.addHandler(file_handler)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """
    Return a child logger under the cnv_etl namespace.

    Parameters
    ----------
    name : str
        Typically __name__ from the calling module.
        e.g. "cnv_etl.transformers.taxonomy"
    """
    return logging.getLogger(name)