def parse_cnv_number_to_float(value: str) -> float:
    """
    Parse a CNV-formatted number string to float.

    CNV uses periods as thousands separators and commas as decimal separators.

    Examples
    --------
    "1.234,56" → 1234.56
    "1.234"    → 1234.0
    """
    if not value or value.strip() == "":
        raise ValueError("Empty number string")

    try:
        cleaned = value.strip().replace(".", "").replace(",", ".")
        return float(cleaned)
    except (ValueError, TypeError):
        raise ValueError(f"'{value}' cannot be converted to float")


def parse_cnv_number_to_int(value: str) -> int:
    """
    Parse a CNV-formatted number string to int.

    Raises ValueError if the value has a non-zero fractional part,
    to prevent silent data truncation.

    Examples
    --------
    "1.234"    → 1234
    "1.234,00" → 1234
    "1.234,56" → raises ValueError
    """
    if not value or value.strip() == "":
        raise ValueError("Empty number string")

    try:
        cleaned = value.strip().replace(".", "").replace(",", ".")
        as_float = float(cleaned)
    except (ValueError, TypeError):
        raise ValueError(f"'{value}' cannot be converted to int")

    if as_float != int(as_float):
        raise ValueError(
            f"'{value}' has a non-zero fractional part ({as_float}) "
            f"and cannot be safely converted to int"
        )

    return int(as_float)