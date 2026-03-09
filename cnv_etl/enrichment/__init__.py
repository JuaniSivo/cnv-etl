"""
Enrichment subpackage for cnv_etl.

Provides field-level overrides for RawFinancialStatement objects,
applied between the download and transform stages.

Public API
----------
Enricher    : orchestrates enrichment for a company's statements.
BaseSource  : abstract interface for enrichment data sources.
FileSource  : CSV-backed implementation of BaseSource.

Typical usage
-------------
from cnv_etl.enrichment import Enricher, FileSource
from cnv_etl.config import ENRICHMENT_REGISTRY, ENRICHMENTS_FILE_PATH

enricher = Enricher(
    registry=ENRICHMENT_REGISTRY,
    source=FileSource(ENRICHMENTS_FILE_PATH),
)
"""

from cnv_etl.enrichment.base_source import BaseSource
from cnv_etl.enrichment.file_source import FileSource
from cnv_etl.enrichment.enricher import Enricher

__all__ = [
    "BaseSource",
    "FileSource",
    "Enricher",
]