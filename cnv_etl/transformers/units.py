from cnv_etl.config import UNITS


def get_multiplier(unit_of_measure: str) -> int:
    """Convert CNV unit descriptions to numeric multipliers."""
    multiplier = UNITS.get(unit_of_measure)

    if multiplier is None:
        raise ValueError(
            f"Unknown unit of measure: '{unit_of_measure}'. "
            f"Valid values are: {list(UNITS.keys())}"
        )

    return multiplier