from datetime import date, datetime
import re
from typing import Optional

from cnv_etl.logging_config import get_logger

logger = get_logger(__name__)


def parse_period_end_date(document_description: str, date_string: str) -> date:
    date_from_description = parse_period_end_date_from_description(document_description)
    date_from_metadata    = parse_period_end_date_from_metadata(date_string)

    if date_from_description is None and date_from_metadata is None:
        raise ValueError(
            f"Cannot parse date from period end date nor document description.\n"
            f"- Description: {document_description}\n"
            f"- Metadata:    {date_string}"
        )

    return date_from_metadata or date_from_description  # type: ignore


def parse_period_end_date_from_description(document_description: str) -> Optional[date]:
    """
    Extract period end date from patterns like 'FECHA CIERRE: 2025-11-30'.
    """
    match = re.search(r'FECHA\s+CIERRE\s*:\s*(\d{4})-(\d{2})-(\d{2})', document_description)
    if match:
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            return None
    return None


def parse_period_end_date_from_metadata(date_string: str) -> Optional[date]:
    """
    Parse CNV date format to date object.

    Examples
    --------
    "31/12/2024" → date(2024, 12, 31)
    """
    try:
        day, month, year = date_string.split("/")
        return date(int(year), int(month), int(day))
    except Exception:
        logger.debug(f"Could not parse '{date_string}' as a date.")
        return None


def parse_cnv_datetime(datetime_string: str) -> datetime:
    """
    Parse CNV datetime format to datetime object.

    Examples
    --------
    "13 oct. 2025 14:20" → datetime(2025, 10, 13, 14, 20)
    """
    datetime_string = (
        datetime_string.strip()
        .replace("ene.", "01").replace("feb.", "02").replace("mar.", "03")
        .replace("abr.", "04").replace("may.", "05").replace("jun.", "06")
        .replace("jul.", "07").replace("ago.", "08").replace("sep.", "09")
        .replace("oct.", "10").replace("nov.", "11").replace("dic.", "12")
        .replace(":", " ")
    )

    day, month, year, hour, minute = datetime_string.split(" ")
    return datetime(int(year), int(month), int(day), int(hour), int(minute))