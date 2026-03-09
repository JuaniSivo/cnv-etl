"""
RunStats — a JSON-serialisable snapshot of a single pipeline run.

Populated from a PipelineReport at the end of each run and persisted
alongside the cProfile .prof binary by ProfilingStore.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional


@dataclass
class CompanyRunStats:
    """Per-company metrics extracted from CompanyStats."""
    ticker:                 str
    statements_downloaded:  int
    statements_transformed: int
    statements_loaded:      int
    duration_seconds:       float
    error_count:            int

    def to_dict(self) -> dict:
        return {
            "ticker":                 self.ticker,
            "statements_downloaded":  self.statements_downloaded,
            "statements_transformed": self.statements_transformed,
            "statements_loaded":      self.statements_loaded,
            "duration_seconds":       round(self.duration_seconds, 2),
            "error_count":            self.error_count,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CompanyRunStats":
        return cls(**d)


@dataclass
class RunStats:
    """
    Complete metrics for a single pipeline run.

    Attributes
    ----------
    started_at : datetime
        When the run began.
    duration_seconds : float
        Total wall-clock seconds for the run.
    companies_attempted : int
    companies_succeeded : int
    companies_failed : int
    statements_downloaded : int
    statements_transformed : int
    statements_loaded : int
    total_errors : int
    company_stats : list[CompanyRunStats]
        Per-company breakdown.
    """
    started_at:             datetime
    duration_seconds:       float
    companies_attempted:    int
    companies_succeeded:    int
    companies_failed:       int
    statements_downloaded:  int
    statements_transformed: int
    statements_loaded:      int
    total_errors:           int
    company_stats:          list[CompanyRunStats] = field(default_factory=list)

    # ------------------------------------------------------------------ #
    # Factories                                                            #
    # ------------------------------------------------------------------ #

    @classmethod
    def from_pipeline_report(cls, report) -> "RunStats":
        """
        Build a RunStats from a completed PipelineReport.

        Parameters
        ----------
        report : PipelineReport
            The report produced at the end of a pipeline run.
        """
        company_stats = [
            CompanyRunStats(
                ticker=cs.ticker,
                statements_downloaded=cs.statements_downloaded,
                statements_transformed=cs.statements_transformed,
                statements_loaded=cs.statements_loaded,
                duration_seconds=cs.duration_seconds,
                error_count=len(cs.errors),
            )
            for cs in report.company_stats
        ]

        return cls(
            started_at=report.started_at,
            duration_seconds=report.duration_seconds,
            companies_attempted=report.total_companies_attempted,
            companies_succeeded=report.total_companies_succeeded,
            companies_failed=report.total_companies_failed,
            statements_downloaded=report.total_statements_downloaded,
            statements_transformed=report.total_statements_transformed,
            statements_loaded=report.total_statements_loaded,
            total_errors=len(report.all_errors),
            company_stats=company_stats,
        )

    @classmethod
    def from_dict(cls, d: dict) -> "RunStats":
        return cls(
            started_at=datetime.fromisoformat(d["started_at"]),
            duration_seconds=d["duration_seconds"],
            companies_attempted=d["companies_attempted"],
            companies_succeeded=d["companies_succeeded"],
            companies_failed=d["companies_failed"],
            statements_downloaded=d["statements_downloaded"],
            statements_transformed=d["statements_transformed"],
            statements_loaded=d["statements_loaded"],
            total_errors=d["total_errors"],
            company_stats=[
                CompanyRunStats.from_dict(cs) for cs in d.get("company_stats", [])
            ],
        )

    @classmethod
    def from_json(cls, path: Path) -> "RunStats":
        """Load a RunStats from a JSON file."""
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    # Serialisation                                                        #
    # ------------------------------------------------------------------ #

    def to_dict(self) -> dict:
        return {
            "started_at":             self.started_at.isoformat(),
            "duration_seconds":       round(self.duration_seconds, 2),
            "companies_attempted":    self.companies_attempted,
            "companies_succeeded":    self.companies_succeeded,
            "companies_failed":       self.companies_failed,
            "statements_downloaded":  self.statements_downloaded,
            "statements_transformed": self.statements_transformed,
            "statements_loaded":      self.statements_loaded,
            "total_errors":           self.total_errors,
            "company_stats":          [cs.to_dict() for cs in self.company_stats],
        }

    def to_json(self, path: Path) -> None:
        """Write this RunStats to a JSON file."""
        path.write_text(
            json.dumps(self.to_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    # ------------------------------------------------------------------ #
    # Display                                                              #
    # ------------------------------------------------------------------ #

    @property
    def started_at_label(self) -> str:
        """Human-readable timestamp for display."""
        return self.started_at.strftime("%Y-%m-%d %H:%M:%S")

    @property
    def duration_label(self) -> str:
        """Human-readable duration for display."""
        mins, secs = divmod(int(self.duration_seconds), 60)
        return f"{mins}m {secs:02d}s"