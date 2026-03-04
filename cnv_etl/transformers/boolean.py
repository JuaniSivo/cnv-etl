from cnv_etl.config import BOOL_TRUE_VALUES, BOOL_FALSE_VALUES


def parse_cnv_string_to_bool(value: str) -> bool:
    if value in BOOL_TRUE_VALUES:
        return True
    if value in BOOL_FALSE_VALUES:
        return False

    raise ValueError(
        f"'{value}' is not a recognised boolean string. "
        f"Expected one of {BOOL_TRUE_VALUES + BOOL_FALSE_VALUES}"
    )