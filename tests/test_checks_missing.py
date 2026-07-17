# tests/test_checks_missing.py
import pandas as pd
from qc.checks.missing import check

def test_flags_absent_required_column(run_ctx):
    df = pd.DataFrame({"patient_id": ["P1"], "age": [30]})
    cfg = {"columns": ["patient_id", "admit_ts"], "max_null_rate": 0.0}
    flags = check(df, cfg, **run_ctx)
    rules = [f["rule"] for f in flags]
    assert "missing_column" in rules
    columns_flagged = [f["column"] for f in flags]
    assert "admit_ts" in columns_flagged

def test_flags_null_values_above_rate(run_ctx):
    df = pd.DataFrame({"patient_id": [None, "P2", None, "P4"]})
    cfg = {"columns": ["patient_id"], "max_null_rate": 0.0}
    flags = check(df, cfg, **run_ctx)
    assert any(f["rule"] == "null_value" for f in flags)

def test_no_flags_when_clean(run_ctx):
    df = pd.DataFrame({"patient_id": ["P1", "P2"], "admit_ts": pd.to_datetime(["2026-07-17", "2026-07-17"])})
    cfg = {"columns": ["patient_id", "admit_ts"], "max_null_rate": 0.0}
    flags = check(df, cfg, **run_ctx)
    assert flags == []
