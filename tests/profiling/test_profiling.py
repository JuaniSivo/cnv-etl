"""
Tests for cnv_etl.profiling.

Covers RunStats serialisation and ProfilingStore file lifecycle.
No mocking needed — everything uses tmp_path.
"""

import cProfile
import json
import pytest
from datetime import datetime
from pathlib import Path

from cnv_etl.profiling.run_stats import RunStats, CompanyRunStats
from cnv_etl.profiling.profiler import ProfilingStore, _stem


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_run_stats(started_at: datetime, duration: float = 120.0) -> RunStats:
    return RunStats(
        started_at=started_at,
        duration_seconds=duration,
        companies_attempted=3,
        companies_succeeded=2,
        companies_failed=1,
        statements_downloaded=10,
        statements_transformed=9,
        statements_loaded=9,
        total_errors=2,
        company_stats=[
            CompanyRunStats(
                ticker="GGAL",
                statements_downloaded=5,
                statements_transformed=5,
                statements_loaded=5,
                duration_seconds=60.0,
                error_count=0,
            ),
            CompanyRunStats(
                ticker="BMA",
                statements_downloaded=5,
                statements_transformed=4,
                statements_loaded=4,
                duration_seconds=60.0,
                error_count=2,
            ),
        ],
    )


def _make_profile() -> cProfile.Profile:
    """Return a minimal populated cProfile.Profile."""
    p = cProfile.Profile()
    p.enable()
    _ = sum(range(1000))
    p.disable()
    return p


@pytest.fixture
def store(tmp_path) -> ProfilingStore:
    return ProfilingStore(data_dir=tmp_path, max_runs=3)


# ---------------------------------------------------------------------------
# RunStats serialisation
# ---------------------------------------------------------------------------

class TestRunStatsSerialization:

    def test_to_dict_round_trips(self):
        rs   = _make_run_stats(datetime(2026, 3, 4, 14, 22, 1))
        d    = rs.to_dict()
        back = RunStats.from_dict(d)

        assert back.started_at             == rs.started_at
        assert back.duration_seconds       == rs.duration_seconds
        assert back.companies_attempted    == rs.companies_attempted
        assert back.companies_succeeded    == rs.companies_succeeded
        assert back.companies_failed       == rs.companies_failed
        assert back.statements_downloaded  == rs.statements_downloaded
        assert back.statements_transformed == rs.statements_transformed
        assert back.statements_loaded      == rs.statements_loaded
        assert back.total_errors           == rs.total_errors

    def test_company_stats_round_trip(self):
        rs   = _make_run_stats(datetime(2026, 3, 4, 14, 22, 1))
        back = RunStats.from_dict(rs.to_dict())

        assert len(back.company_stats) == 2
        assert back.company_stats[0].ticker == "GGAL"
        assert back.company_stats[1].error_count == 2

    def test_to_json_writes_valid_json(self, tmp_path):
        rs   = _make_run_stats(datetime(2026, 3, 4, 14, 22, 1))
        path = tmp_path / "test.json"
        rs.to_json(path)

        assert path.exists()
        data = json.loads(path.read_text())
        assert data["companies_attempted"] == 3

    def test_from_json_round_trips(self, tmp_path):
        rs   = _make_run_stats(datetime(2026, 3, 4, 14, 22, 1))
        path = tmp_path / "test.json"
        rs.to_json(path)
        back = RunStats.from_json(path)

        assert back.started_at == rs.started_at
        assert back.total_errors == rs.total_errors

    def test_duration_label_format(self):
        rs = _make_run_stats(datetime(2026, 1, 1), duration=135.0)
        assert rs.duration_label == "2m 15s"

    def test_started_at_label_format(self):
        rs = _make_run_stats(datetime(2026, 3, 4, 14, 22, 1))
        assert rs.started_at_label == "2026-03-04 14:22:01"


# ---------------------------------------------------------------------------
# ProfilingStore — save and load
# ---------------------------------------------------------------------------

class TestProfilingStoreSaveLoad:

    def test_save_creates_both_files(self, store, tmp_path):
        rs = _make_run_stats(datetime(2026, 3, 4, 14, 22, 1))
        store.save(rs, _make_profile())

        stem     = _stem(rs.started_at)
        json_file = tmp_path / f"{stem}.json"
        prof_file = tmp_path / f"{stem}.prof"

        assert json_file.exists()
        assert prof_file.exists()

    def test_load_last_n_returns_correct_count(self, store):
        for i in range(3):
            rs = _make_run_stats(datetime(2026, 3, i + 1, 10, 0, 0))
            store.save(rs, _make_profile())

        loaded = store.load_last_n(2)
        assert len(loaded) == 2

    def test_load_last_n_newest_first(self, store):
        dates = [
            datetime(2026, 3, 1, 10, 0, 0),
            datetime(2026, 3, 2, 10, 0, 0),
            datetime(2026, 3, 3, 10, 0, 0),
        ]
        for dt in dates:
            store.save(_make_run_stats(dt), _make_profile())

        loaded = store.load_last_n(3)
        assert loaded[0].started_at == datetime(2026, 3, 3, 10, 0, 0)
        assert loaded[2].started_at == datetime(2026, 3, 1, 10, 0, 0)

    def test_load_last_n_empty_store_returns_empty(self, store):
        assert store.load_last_n(5) == []

    def test_load_last_n_fewer_than_n_available(self, store):
        store.save(_make_run_stats(datetime(2026, 3, 1)), _make_profile())
        loaded = store.load_last_n(10)
        assert len(loaded) == 1

    def test_load_profile_returns_stats(self, store):
        import pstats
        rs = _make_run_stats(datetime(2026, 3, 4, 14, 22, 1))
        store.save(rs, _make_profile())

        stats = store.load_profile(rs.started_at)
        assert isinstance(stats, pstats.Stats)

    def test_load_profile_missing_raises(self, store):
        with pytest.raises(FileNotFoundError):
            store.load_profile(datetime(2000, 1, 1, 0, 0, 0))


# ---------------------------------------------------------------------------
# ProfilingStore — prune
# ---------------------------------------------------------------------------

class TestProfilingStorePrune:

    def test_prune_keeps_max_runs(self, store, tmp_path):
        # store.max_runs == 3; save 5 runs
        for i in range(5):
            rs = _make_run_stats(datetime(2026, 3, i + 1, 10, 0, 0))
            store.save(rs, _make_profile())

        json_files = list(tmp_path.glob("run_*.json"))
        assert len(json_files) == 3

    def test_prune_deletes_oldest(self, store, tmp_path):
        dates = [datetime(2026, 3, i + 1, 10, 0, 0) for i in range(5)]
        for dt in dates:
            store.save(_make_run_stats(dt), _make_profile())

        remaining = sorted(tmp_path.glob("run_*.json"), reverse=True)
        remaining_stems = [f.stem for f in remaining]

        # The three newest should be kept
        assert _stem(datetime(2026, 3, 5, 10, 0, 0)) in remaining_stems
        assert _stem(datetime(2026, 3, 4, 10, 0, 0)) in remaining_stems
        assert _stem(datetime(2026, 3, 3, 10, 0, 0)) in remaining_stems

        # The two oldest should be gone
        assert _stem(datetime(2026, 3, 2, 10, 0, 0)) not in remaining_stems
        assert _stem(datetime(2026, 3, 1, 10, 0, 0)) not in remaining_stems

    def test_prune_also_deletes_prof_files(self, store, tmp_path):
        for i in range(5):
            rs = _make_run_stats(datetime(2026, 3, i + 1, 10, 0, 0))
            store.save(rs, _make_profile())

        prof_files = list(tmp_path.glob("run_*.prof"))
        assert len(prof_files) == 3

    def test_below_max_runs_nothing_pruned(self, store, tmp_path):
        for i in range(2):   # max_runs is 3
            rs = _make_run_stats(datetime(2026, 3, i + 1, 10, 0, 0))
            store.save(rs, _make_profile())

        assert len(list(tmp_path.glob("run_*.json"))) == 2