def parse_cnv_string_to_bool(
        value: str
    ) -> bool:

    if value in ["Si", "Sí"]: return True
    if value in ["No", "No"]: return False

    raise ValueError(f"{value} is not 'Si' or 'No'")