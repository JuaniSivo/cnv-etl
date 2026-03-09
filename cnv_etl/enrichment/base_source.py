"""
Abstract base class for enrichment sources.

Any source that provides field-level overrides for RawFinancialStatement
must implement this interface. Current implementations:
  - FileSource   : reads from a local CSV file

Planned implementations:
  - PdfSource    : extracts values from CNV statement PDFs
"""

from abc import ABC, abstractmethod


class BaseSource(ABC):
    """
    Interface for enrichment data sources.

    A source is responsible for loading override values for a given
    company ticker and returning them in a normalised structure that
    the Enricher can apply directly to RawFinancialStatement objects.

    Return structure of load()
    --------------------------
    {
        "<document_id>": {
            "<field_name>": "<raw_value>",
            ...
        },
        ...
    }

    All values are returned as raw strings. Type parsing is intentionally
    left to the existing transformer layer so that no parsing logic is
    duplicated here.
    """

    @abstractmethod
    def load(self, ticker: str) -> dict[str, dict[str, str]]:
        """
        Load enrichment values for a given company ticker.

        Parameters
        ----------
        ticker : str
            The company ticker to load overrides for (e.g. "RIGO").

        Returns
        -------
        dict[str, dict[str, str]]
            Outer key  : document_id as a string.
            Inner key  : RawFinancialStatement field name.
            Inner value: raw string value to apply.

        Notes
        -----
        Returns an empty dict if no overrides exist for the ticker,
        so callers never need to handle None.
        """
        ...