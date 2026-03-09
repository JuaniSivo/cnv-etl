"""
ProfilingStore — saves, loads, and prunes pipeline run profiles.

Each run produces two files with a shared timestamp stem:
    data/profiling/run_2026-03-04T14-22-01.prof   ← cProfile binary
    data/profiling/run_2026-03-04T14-22-01.json   ← RunStats JSON

Usage
-----
from cnv_etl.profiling.profiler import ProfilingStore
import cProfile

profile = cProfile.Profile()
profile.enable()
# ... run pipeline ...
profile.disable()

store = ProfilingStore()
store.save(run_stats, profile)

for stats in store.load_last_n(5):
    print(stats.started_at_label, stats.duration_label)
"""

import cProfile
import io
import pstats
from datetime import datetime
from pathlib import Path
from typing import Optional

from cnv_etl.config import PROFILING_DATA_DIR, PROFILING_MAX_RUNS
from cnv_etl.profiling.run_stats import RunStats
from cnv_etl.logging_config import get_logger

logger = get_logger(__name__)

_STEM_PREFIX = "run_"


def _stem(dt: datetime) -> str:
    """Build the shared filename stem from a datetime."""
    return _STEM_PREFIX + dt.strftime("%Y-%m-%dT%H-%M-%S")


def _json_path(data_dir: Path, stem: str) -> Path:
    return data_dir / f"{stem}.json"


def _prof_path(data_dir: Path, stem: str) -> Path:
    return data_dir / f"{stem}.prof"


class ProfilingStore:
    """
    Manages profiling artefacts for pipeline runs.

    Parameters
    ----------
    data_dir : Path, optional
        Directory for .prof and .json files.
        Defaults to PROFILING_DATA_DIR from config.toml.
    max_runs : int, optional
        Maximum number of runs to retain.
        Defaults to PROFILING_MAX_RUNS from config.toml.
    """

    def __init__(
        self,
        data_dir: Optional[Path] = None,
        max_runs: Optional[int]  = None,
    ) -> None:
        self.data_dir = data_dir or PROFILING_DATA_DIR
        self.max_runs = max_runs or PROFILING_MAX_RUNS
        self.data_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ #
    # Save                                                                 #
    # ------------------------------------------------------------------ #

    def save(self, run_stats: RunStats, profile: cProfile.Profile) -> None:
        """
        Persist a RunStats (JSON) and cProfile binary (.prof) to disk,
        then prune old runs if the cap is exceeded.

        Parameters
        ----------
        run_stats : RunStats
            Business-level metrics for the run.
        profile : cProfile.Profile
            The cProfile instance collected during the run.
        """
        stem = _stem(run_stats.started_at)

        try:
            run_stats.to_json(_json_path(self.data_dir, stem))
            profile.dump_stats(str(_prof_path(self.data_dir, stem)))
            logger.info(f"Profiling data saved: {stem}")
        except Exception as e:
            logger.error(f"Could not save profiling data. {type(e).__name__}: {e}")
            return

        self._prune()

    # ------------------------------------------------------------------ #
    # Load                                                                 #
    # ------------------------------------------------------------------ #

    def load_last_n(self, n: int) -> list[RunStats]:
        """
        Return the N most recent RunStats, newest first.

        Parameters
        ----------
        n : int
            Number of runs to return.

        Returns
        -------
        list[RunStats]
            Sorted newest → oldest. Empty list if no data exists.
        """
        json_files = sorted(
            self.data_dir.glob(f"{_STEM_PREFIX}*.json"),
            reverse=True,   # lexicographic sort works because stem is ISO-8601
        )

        results = []
        for path in json_files[:n]:
            try:
                results.append(RunStats.from_json(path))
            except Exception as e:
                logger.warning(f"Could not load {path.name}. {type(e).__name__}: {e}")

        return results

    def load_profile(self, started_at: datetime) -> pstats.Stats:
        """
        Load the cProfile Stats for the run that started at the given time.

        Parameters
        ----------
        started_at : datetime
            The started_at timestamp of the run to load.

        Returns
        -------
        pstats.Stats
            Ready to sort and print.

        Raises
        ------
        FileNotFoundError
            If no .prof file exists for the given timestamp.
        """
        path = _prof_path(self.data_dir, _stem(started_at))
        if not path.exists():
            raise FileNotFoundError(f"No profile found for {started_at}: {path}")

        stream = io.StringIO()
        stats  = pstats.Stats(str(path), stream=stream)
        return stats

    # ------------------------------------------------------------------ #
    # Summary display                                                      #
    # ------------------------------------------------------------------ #

    def print_summary(self, n: int) -> None:
        """
        Print a summary table of the last N runs to stdout.

        Parameters
        ----------
        n : int
            Number of runs to display.
        """
        runs = self.load_last_n(n)

        if not runs:
            print("No profiling data found.")
            return

        col_w = [22, 10, 16, 14, 8]
        headers = ["Run", "Duration", "Companies", "Statements", "Errors"]

        header_row = "  ".join(h.ljust(w) for h, w in zip(headers, col_w))
        separator  = "-" * len(header_row)

        print()
        print(f"  Last {len(runs)} run(s)")
        print(separator)
        print(header_row)
        print(separator)

        for rs in runs:
            companies  = f"{rs.companies_succeeded} ok / {rs.companies_failed} fail"
            statements = str(rs.statements_loaded)
            cols = [
                rs.started_at_label,
                rs.duration_label,
                companies,
                statements,
                str(rs.total_errors),
            ]
            print("  ".join(c.ljust(w) for c, w in zip(cols, col_w)))

        print(separator)
        print()

    def print_profile(self, n: int = 20) -> None:
        """
        Print the top N functions by cumulative time from the most
        recent .prof file.

        Parameters
        ----------
        n : int
            Number of functions to show. Default 20.
        """
        json_files = sorted(
            self.data_dir.glob(f"{_STEM_PREFIX}*.json"),
            reverse=True,
        )

        if not json_files:
            print("No profiling data found.")
            return

        # Derive the most recent started_at from the filename stem
        latest_stem    = json_files[0].stem          # e.g. run_2026-03-04T14-22-01
        timestamp_str  = latest_stem[len(_STEM_PREFIX):]  # 2026-03-04T14-22-01
        started_at     = datetime.strptime(timestamp_str, "%Y-%m-%dT%H-%M-%S")

        try:
            stats = self.load_profile(started_at)
            stats.sort_stats("cumulative")
            stats.print_stats(n)
        except FileNotFoundError as e:
            print(f"Could not load profile: {e}")

    # ------------------------------------------------------------------ #
    # Prune                                                                #
    # ------------------------------------------------------------------ #

    def _prune(self) -> None:
        """
        Delete the oldest run pairs (.json + .prof) beyond max_runs.
        Called automatically after each save.
        """
        json_files = sorted(
            self.data_dir.glob(f"{_STEM_PREFIX}*.json"),
            reverse=True,
        )

        to_delete = json_files[self.max_runs:]
        for json_file in to_delete:
            prof_file = json_file.with_suffix(".prof")
            try:
                json_file.unlink()
                if prof_file.exists():
                    prof_file.unlink()
                logger.debug(f"Pruned old profiling run: {json_file.stem}")
            except Exception as e:
                logger.warning(
                    f"Could not prune {json_file.name}. {type(e).__name__}: {e}"
                )