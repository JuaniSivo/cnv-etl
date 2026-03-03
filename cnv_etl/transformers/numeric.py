def parse_cnv_number_to_float(
        value: str
    ) -> float:

    if not value or value.strip() == "":
        raise ValueError("Empty number string")
    
    try:
        cleaned = value.strip().replace(".", "").replace(",", ".")
        return float(cleaned)
    except:
        raise ValueError(f"{value} cannot be converted to integer")


def parse_cnv_number_to_int(
        value: str
    ) -> int:

    if not value or value.strip() == "":
        raise ValueError("Empty number string")
    
    try:
        cleaned = value.strip().replace(".", "").replace(",", ".")
        return int(cleaned)
    except:
        raise ValueError(f"{value} cannot be converted to integer")