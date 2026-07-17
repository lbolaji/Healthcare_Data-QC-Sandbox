# tests/test_checks_timestamp.py
import pandas as pd
from qc.checks.timestamp import check

def test_flags_discharge_before_admit(ed_df, run_ctx):
    cfg = {"columns": ["admit_ts", "discharge_ts"],
           "order": {"before": "admit_ts", "after": "discharge_ts"}}
    flags = check(ed_df, cfg, **run_ctx)
    assert any(f["rule"] == "timestamp_order" for f in flags)

def test_flags_unparseable_timestamp(run_ctx):
    df = pd.DataFrame({"admit_ts": ["not-a-date", "2026-07-17 08:00"]})
    cfg = {"columns": ["admit_ts"]}
    flags = check(df, cfg, **run_ctx)
    assert any(f["rule"] == "invalid_timestamp" for f in flags)

def test_no_flags_valid_timestamps(run_ctx):
    df = pd.DataFrame({
        "admit_ts": pd.to_datetime(["2026-07-17 08:00", "2026-07-17 09:00"]),
        "discharge_ts": pd.to_datetime(["2026-07-17 10:00", "2026-07-17 11:00"]),
    })
    cfg = {"columns": ["admit_ts", "discharge_ts"],
           "order": {"before": "admit_ts", "after": "discharge_ts"}}
    flags = check(df, cfg, **run_ctx)
    assert flags == []
