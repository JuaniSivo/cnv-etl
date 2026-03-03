from datetime import date, datetime
import re
from typing import Optional


def parse_period_end_date(document_description: str, date_string: str) -> date:
    date_from_description = parse_period_end_date_from_description(document_description)
    date_from_statement_metadata = parse_period_end_date_from_metadata(date_string)

    if date_from_description is None and date_from_statement_metadata is None:
        raise ValueError(f"Cannot parse date from period end date nor document description.\n- Description: {document_description}\n- Metadata: {date_string}")

    if date_from_statement_metadata is None:
        return date_from_description # type: ignore

    return date_from_statement_metadata

def parse_period_end_date_from_description(document_description: str) -> Optional[date]:
        """
        Extract period end date from description.
        
        Looks for patterns like:
        - "FECHA CIERRE: 2025-11-30"
        - "FECHA CIERRE: 2024-12-31"
        """
        # Pattern: FECHA CIERRE: YYYY-MM-DD
        match = re.search(r'FECHA CIERRE:\s*(\d{4})-(\d{2})-(\d{2})', document_description)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            try:
                return date(year, month, day)
            except ValueError:
                return None
        
        return None

def parse_period_end_date_from_metadata(date_string: str) -> Optional[date]:
    """
    Parse CNV date format to date object.
    
    Examples:
        "31/12/2024" → date(2024, 12, 31)
        "01/01/2023" → date(2023, 1, 1)
    """
    try:
        day, month, year = date_string.split("/")
        return date(int(year), int(month), int(day))
    except:
        print(f"Cannot parse {date_string} to date type.")


def parse_cnv_datetime(datetime_string: str) -> datetime:
    """
    Parse CNV date format to date object.
    
    Examples:
        "13 oct. 2025 14:20" → datetime(2025, 10, 13, 14, 20)
    """

    datetime_string = datetime_string.strip()\
        .replace("ene.", "01")\
        .replace("feb.", "02")\
        .replace("mar.", "03")\
        .replace("abr.", "04")\
        .replace("may.", "05")\
        .replace("jun.", "06")\
        .replace("jul.", "07")\
        .replace("ago.", "08")\
        .replace("sep.", "09")\
        .replace("oct.", "10")\
        .replace("nov.", "11")\
        .replace("dic.", "12")\
        .replace(":", " ")
    
    day, month, year, hour, minute = datetime_string.split(" ")

    return datetime(int(year), int(month), int(day), int(hour), int(minute))