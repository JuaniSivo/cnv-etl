from datetime import date, datetime


def parse_cnv_date(date_string: str) -> date:
    """
    Parse CNV date format to date object.
    
    Examples:
        "31/12/2024" → date(2024, 12, 31)
        "01/01/2023" → date(2023, 1, 1)
    """
    day, month, year = date_string.split("/")

    return date(int(year), int(month), int(day))


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