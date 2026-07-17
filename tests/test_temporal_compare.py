import pandas as pd
from qc.temporal.compare import compute_snapshot, compare


def test_compute_snapshot_row_count():
    df = pd.DataFrame({"age": [30, 45, 60], "admit_ts": pd.to_datetime(["2026-07-17"] * 3)})
    snap = compute_snapshot(df)
    assert snap["row_count"] == 3


def test_compute_snapshot_null_rate():
    df = pd.DataFrame({"age": [30, None, 60]})
    snap = compute_snapshot(df)
    assert abs(snap["null_rate_age"] - 0.333) < 0.01


def test_compare_flags_row_count_drop(run_ctx):
    today = {"row_count": 500}
    priors = {"day": {"row_count": 1000}, "week": None, "month": None}
    thresholds = {"row_count_max_pct_change": 0.30}
    flags = compare(today, priors, thresholds, **run_ctx)
    assert any(f["rule"] == "delta_row_count" and f["severity"] == "error" for f in flags)


def test_compare_no_flags_within_threshold(run_ctx):
    today = {"row_count": 1000}
    priors = {"day": {"row_count": 1050}, "week": None, "month": None}
    thresholds = {"row_count_max_pct_change": 0.30}
    flags = compare(today, priors, thresholds, **run_ctx)
    assert flags == []


def test_compare_no_flags_no_prior(run_ctx):
    today = {"row_count": 1000}
    priors = {"day": None, "week": None, "month": None}
    thresholds = {"row_count_max_pct_change": 0.30}
    flags = compare(today, priors, thresholds, **run_ctx)
    assert flags == []
