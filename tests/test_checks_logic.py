# tests/test_checks_logic.py
import pandas as pd
from qc.checks.logic import check

def test_flags_lwbs_rate_exceeded(ed_df, run_ctx):
    # ed_df has 2 LWBS out of 4 rows = 50% > max_rate 5%
    cfg = {"lwbs": {"condition": "disposition == 'LWBS'", "max_rate": 0.05, "severity": "warn"}}
    flags = check(ed_df, cfg, **run_ctx)
    assert any(f["rule"] == "lwbs" for f in flags)

def test_no_flags_lwbs_rate_within_threshold(run_ctx):
    df = pd.DataFrame({"disposition": ["DISCHARGE"] * 99 + ["LWBS"]})
    cfg = {"lwbs": {"condition": "disposition == 'LWBS'", "max_rate": 0.05, "severity": "warn"}}
    flags = check(df, cfg, **run_ctx)
    assert flags == []

def test_flags_discharge_before_admit_condition(run_ctx):
    df = pd.DataFrame({
        "admit_ts": pd.to_datetime(["2026-07-17 10:00", "2026-07-17 08:00"]),
        "discharge_ts": pd.to_datetime(["2026-07-17 08:00", "2026-07-17 10:00"]),
    })
    cfg = {"discharge_before_admit": {"condition": "discharge_ts < admit_ts", "severity": "error"}}
    flags = check(df, cfg, **run_ctx)
    assert any(f["rule"] == "discharge_before_admit" for f in flags)
    assert len([f for f in flags if f["rule"] == "discharge_before_admit"]) == 1
