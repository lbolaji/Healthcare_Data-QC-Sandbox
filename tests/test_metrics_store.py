import json
from qc.metrics.store import append_snapshot, get_snapshot, get_prior_snapshots

SNAPSHOT = {"row_count": 1000, "dup_rate": 0.01, "null_rate_admit_ts": 0.02, "mean_age": 45.3}

def test_append_and_retrieve(tmp_duckdb):
    append_snapshot(tmp_duckdb, "ohiohealth", "ed", "2026-07-17", SNAPSHOT)
    result = get_snapshot(tmp_duckdb, "ohiohealth", "ed", "2026-07-17")
    assert result["row_count"] == 1000
    assert result["null_rate_admit_ts"] == 0.02

def test_get_snapshot_missing_returns_none(tmp_duckdb):
    result = get_snapshot(tmp_duckdb, "ohiohealth", "ed", "2025-01-01")
    assert result is None

def test_get_prior_snapshots(tmp_duckdb):
    append_snapshot(tmp_duckdb, "ohiohealth", "ed", "2026-07-10", {"row_count": 900})
    append_snapshot(tmp_duckdb, "ohiohealth", "ed", "2026-07-16", {"row_count": 950})
    append_snapshot(tmp_duckdb, "ohiohealth", "ed", "2026-06-17", {"row_count": 800})
    priors = get_prior_snapshots(tmp_duckdb, "ohiohealth", "ed", "2026-07-17")
    assert priors["day"]["row_count"] == 950
    assert priors["week"]["row_count"] == 900
    assert priors["month"]["row_count"] == 800

def test_client_isolation(tmp_duckdb):
    append_snapshot(tmp_duckdb, "ohiohealth", "ed", "2026-07-17", {"row_count": 1000})
    append_snapshot(tmp_duckdb, "ssmhealth", "ed", "2026-07-17", {"row_count": 2000})
    ohio = get_snapshot(tmp_duckdb, "ohiohealth", "ed", "2026-07-17")
    ssm = get_snapshot(tmp_duckdb, "ssmhealth", "ed", "2026-07-17")
    assert ohio["row_count"] == 1000
    assert ssm["row_count"] == 2000
