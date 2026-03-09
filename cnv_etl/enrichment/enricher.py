"""
Enricher — applies field-level overrides to RawFinancialStatement objects.

The Enricher sits between the download and transform stages. It consults
the enrichment registry (from config.toml) to decide whether a company
needs enrichment at all, then delegates value retrieval to a BaseSource
implementation.

Usage
-----
from cnv_etl.enrichment.enricher import Enricher
from cnv_etl.enrichment.file_source import FileSource
from cnv_etl.config import ENRICHMENT_REGISTRY, ENRICHMENTS_FILE_PATH

enricher = Enricher(
    registry=ENRICHMENT_REGISTRY,
    source=FileSource(ENRICHMENTS_FILE_PATH),
)

raw_statements = enricher.enrich(
    ticker="RIGO",
    statements=raw_statements,
    stats=stats,
    report=report,
)
"""

from cnv_etl.enrichment.base_source import BaseSource
from cnv_etl.errors import ETLError, CompanyStats, PipelineReport
from cnv_etl.models.document import RawFinancialStatement
from cnv_etl.logging_config import get_logger

logger = get_logger(__name__)


class Enricher:
    """
    Applies field-level overrides to a list of RawFinancialStatement objects.

    Parameters
    ----------
    registry : dict
        The enrichment registry from config.toml — maps ticker to its
        enrichment config (fields list and source type).
    source : BaseSource
        The data source to retrieve override values from.
    """

    def __init__(self, registry: dict, source: BaseSource) -> None:
        self.registry = registry
        self.source   = source

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def enrich(
        self,
        ticker:     str,
        statements: list[RawFinancialStatement],
        stats:      CompanyStats,
        report:     PipelineReport,
    ) -> list[RawFinancialStatement]:
        """
        Enrich a list of RawFinancialStatement objects for a given ticker.

        For each statement, looks up any override values from the source
        and applies them via RawFinancialStatement.transform(). Only fields
        declared in the registry entry for this ticker are ever patched —
        fields present in the CSV but not in the registry are ignored.

        Statements that fail enrichment are returned unchanged so that the
        transform stage can still attempt to process them. The failure is
        recorded in the PipelineReport.

        Parameters
        ----------
        ticker : str
            Company ticker being processed.
        statements : list[RawFinancialStatement]
            Raw statements fresh from the download stage.
        stats : CompanyStats
            Live stats object for this company — enriched count is updated
            in place.
        report : PipelineReport
            Shared report — enrich-stage errors are recorded here.

        Returns
        -------
        list[RawFinancialStatement]
            Statements with override values applied where available.
            Same length as input — no statements are dropped.
        """
        company_config = self.registry.get(ticker)
        if not company_config:
            logger.debug(f"No enrichment config for {ticker} — skipping.")
            return statements

        declared_fields: list[str] = company_config.get("fields", [])
        if not declared_fields:
            logger.warning(
                f"Enrichment config for {ticker} has no fields declared — skipping."
            )
            return statements

        try:
            all_overrides = self.source.load(ticker)
        except Exception as e:
            report.add_error(ETLError(
                stage="enrich",
                ticker=ticker,
                error_type=type(e).__name__,
                message=f"Source failed to load overrides: {e}",
            ))
            logger.error(
                f"Enrichment source error for {ticker}. "
                f"{type(e).__name__}: {e}. Returning statements unchanged."
            )
            return statements

        enriched_statements = []
        for fs in statements:
            enriched_statements.append(
                self._enrich_statement(
                    fs, ticker, declared_fields, all_overrides, stats, report
                )
            )

        logger.info(
            f"Enrichment complete for {ticker}: "
            f"{stats.statements_enriched}/{len(statements)} statement(s) enriched."
        )
        return enriched_statements

    # ------------------------------------------------------------------ #
    # Private                                                              #
    # ------------------------------------------------------------------ #

    def _enrich_statement(
        self,
        fs:               RawFinancialStatement,
        ticker:           str,
        declared_fields:  list[str],
        all_overrides:    dict[str, dict[str, str]],
        stats:            CompanyStats,
        report:           PipelineReport,
    ) -> RawFinancialStatement:
        """
        Apply overrides to a single RawFinancialStatement.

        Only fields listed in declared_fields are candidates for patching,
        even if the CSV contains additional fields for this document.

        Parameters
        ----------
        fs : RawFinancialStatement
            The statement to enrich.
        ticker : str
            Company ticker, used for error reporting.
        declared_fields : list[str]
            Fields the registry says should be enriched for this ticker.
        all_overrides : dict[str, dict[str, str]]
            Full override map for this ticker from the source,
            keyed by document_id.
        stats : CompanyStats
            Updated in place when a statement is enriched.
        report : PipelineReport
            Enrich-stage errors recorded here.

        Returns
        -------
        RawFinancialStatement
            Patched statement, or the original if no overrides matched
            or an error occurred.
        """
        doc_overrides = all_overrides.get(fs.document_id, {})

        # Only apply fields that are both declared in registry and present
        # in the source data for this specific document.
        updates = {
            field: doc_overrides[field]
            for field in declared_fields
            if field in doc_overrides
        }

        if not updates:
            logger.debug(
                f"  No overrides found for {ticker}/{fs.document_id} "
                f"(declared fields: {declared_fields})."
            )
            return fs

        try:
            enriched_fs = fs.transform(**updates)
            stats.statements_enriched += 1
            logger.info(
                f"  Enriched {ticker}/{fs.document_id}: "
                f"{list(updates.keys())}"
            )
            return enriched_fs
        except Exception as e:
            report.add_error(ETLError(
                stage="enrich",
                ticker=ticker,
                error_type=type(e).__name__,
                message=str(e),
                document_description=fs.document_description,
            ))
            logger.error(
                f"  Failed to enrich {ticker}/{fs.document_id}. "
                f"{type(e).__name__}: {e}. Returning statement unchanged."
            )
            return fs