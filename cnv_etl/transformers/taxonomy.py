from cnv_etl.config import TAXONOMY
from cnv_etl.logging_config import get_logger

logger = get_logger(__name__)


def map_to_xbrl(cnv_label: str) -> str:
    """
    Map CNV Spanish label to XBRL taxonomy concept.

    Returns the XBRL label if found, otherwise returns the original label.
    This allows for graceful handling of unmapped concepts.
    """
    xbrl_label = TAXONOMY.get(cnv_label.upper(), cnv_label)

    if cnv_label.upper() not in TAXONOMY:
        logger.warning(f"Could not map CNV concept '{cnv_label}' to XBRL")

    return xbrl_label