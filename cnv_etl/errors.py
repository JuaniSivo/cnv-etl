"""
Error data structures for the cnv_etl pipeline.

ETLError captures a single failure at any stage of the pipeline.
PipelineReport accumulates successes and failures across a full run
and can produce a human-readable summary.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Optional


Stage = Literal["fetch", "download", "transform", "load"]


@dataclass
class ETLError:
    """
    Represents a single failure at any stage of the ETL pipeline.

    Attributes
    ----------
    stage : Stage
        Which pipeline stage produced the error.
    ticker : str
        Ticker of the company being processed when the error occurred.
    error_type : str
        The exception class name (e.g. 'ValueError', 'TimeoutError').
    message : str
        The exception message.
    document_description : str, optional
        The document description if the error occurred at document level.
    timestamp : datetime
        When the error was recorded. Defaults to now.
    """
    stage:                Stage
    ticker:               str
    error_type:           str
    message:              str
    document_description: Optional[str]  = None
    timestamp:            datetime       = field(default_factory=datetime.now)

    def to_dict(self) -> dict:
        return {
            "stage":                self.stage,
            "ticker":               self.ticker,
            "error_type":           self.error_type,
            "message":              self.message,
            "document_description": self.document_description,
            "timestamp":            self.timestamp.isoformat(),
        }


@dataclass
class CompanyStats:
    """
    Per-company summary accumulated during a pipeline run.

    Attributes
    ----------
    ticker : str
        Company ticker.
    statements_downloaded : int
        Number of raw statements successfully downloaded.
    statements_transformed : int
        Number of statements successfully transformed.
    statements_loaded : int
        Number of statements successfully loaded (Excel / SQLite).
    duration_seconds : float
        Wall-clock seconds spent processing this company.
    errors : list[ETLError]
        All errors recorded for this company.
    """
    ticker:                  str
    statements_downloaded:   int             = 0
    statements_transformed:  int             = 0
    statements_loaded:       int             = 0
    duration_seconds:        float           = 0.0
    errors:                  list[ETLError]  = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return len(self.errors) > 0

    def to_dict(self) -> dict:
        return {
            "ticker":                 self.ticker,
            "statements_downloaded":  self.statements_downloaded,
            "statements_transformed": self.statements_transformed,
            "statements_loaded":      self.statements_loaded,
            "duration_seconds":       round(self.duration_seconds, 2),
            "errors":                 [e.to_dict() for e in self.errors],
        }


@dataclass
class PipelineReport:
    """
    Accumulates results across a full pipeline run.

    Usage
    -----
    report = PipelineReport()
    report.add_success("GGAL")
    report.add_failure(ETLError(stage="fetch", ticker="BMA", ...))
    print(report.summary())
    """
    started_at:      datetime        = field(default_factory=datetime.now)
    company_stats:   list[CompanyStats] = field(default_factory=list)

    # ------------------------------------------------------------------ #
    # Mutation helpers                                                   #
    # ------------------------------------------------------------------ #

    def start_company(self, ticker: str) -> CompanyStats:
        """Create and register a CompanyStats entry for a new company."""
        stats = CompanyStats(ticker=ticker)
        self.company_stats.append(stats)
        return stats

    def add_error(self, error: ETLError) -> None:
        """
        Append an error to the matching CompanyStats entry.
        If no entry exists yet for the ticker, one is created automatically.
        """
        stats = self._get_or_create(error.ticker)
        stats.errors.append(error)

    # ------------------------------------------------------------------ #
    # Aggregates                                                         #
    # ------------------------------------------------------------------ #

    @property
    def total_companies_attempted(self) -> int:
        return len(self.company_stats)

    @property
    def total_companies_succeeded(self) -> int:
        return sum(1 for s in self.company_stats if not s.failed)

    @property
    def total_companies_failed(self) -> int:
        return sum(1 for s in self.company_stats if s.failed)

    @property
    def total_statements_downloaded(self) -> int:
        return sum(s.statements_downloaded for s in self.company_stats)

    @property
    def total_statements_transformed(self) -> int:
        return sum(s.statements_transformed for s in self.company_stats)

    @property
    def total_statements_loaded(self) -> int:
        return sum(s.statements_loaded for s in self.company_stats)

    @property
    def all_errors(self) -> list[ETLError]:
        return [e for s in self.company_stats for e in s.errors]

    @property
    def duration_seconds(self) -> float:
        return (datetime.now() - self.started_at).total_seconds()

    # ------------------------------------------------------------------ #
    # Summary                                                            #
    # ------------------------------------------------------------------ #

    def summary(self) -> str:
        mins, secs = divmod(int(self.duration_seconds), 60)
        lines = [
            "",
            "=" * 60,
            "  PIPELINE RUN SUMMARY",
            "=" * 60,
            f"  Started:      {self.started_at.strftime('%Y-%m-%d %H:%M:%S')}",
            f"  Duration:     {mins}m {secs}s",
            f"  Companies:    {self.total_companies_succeeded} ok / "
            f"{self.total_companies_failed} failed / "
            f"{self.total_companies_attempted} total",
            f"  Statements:   {self.total_statements_downloaded} downloaded  "
            f"{self.total_statements_transformed} transformed  "
            f"{self.total_statements_loaded} loaded",
            "-" * 60,
        ]

        if not self.all_errors:
            lines.append("  No errors recorded.")
        else:
            lines.append(f"  Errors ({len(self.all_errors)}):")
            for err in self.all_errors:
                doc = f" | {err.document_description}" if err.document_description else ""
                lines.append(
                    f"    [{err.stage.upper():>10}]  {err.ticker}{doc}\n"
                    f"                 {err.error_type}: {err.message}"
                )

        lines.append("=" * 60)
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "started_at":                  self.started_at.isoformat(),
            "duration_seconds":            round(self.duration_seconds, 2),
            "total_companies_attempted":   self.total_companies_attempted,
            "total_companies_succeeded":   self.total_companies_succeeded,
            "total_companies_failed":      self.total_companies_failed,
            "total_statements_downloaded": self.total_statements_downloaded,
            "total_statements_transformed":self.total_statements_transformed,
            "total_statements_loaded":     self.total_statements_loaded,
            "company_stats":               [s.to_dict() for s in self.company_stats],
        }

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    def _get_or_create(self, ticker: str) -> CompanyStats:
        for stats in self.company_stats:
            if stats.ticker == ticker:
                return stats
        stats = CompanyStats(ticker=ticker)
        self.company_stats.append(stats)
        return stats