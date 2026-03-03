def get_multiplier(unit_of_measure: str) -> int:
    """Convert CNV unit descriptions to numeric multipliers."""

    multiplier_dict = {
        "Unit": 1,
        "$": 1,
        "Thousands": 1_000,
        "Miles de $": 1_000,
        "Millions": 1_000_000,
        "Millones de $": 1_000_000,
    }

    return multiplier_dict[unit_of_measure]