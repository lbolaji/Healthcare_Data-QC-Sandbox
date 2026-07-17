# tests/test_checks_types.py
import pandas as pd
from qc.checks.types import check

def test_flags_non_numeric(run_ctx):
    df = pd.DataFrame({"age": ["old", "young", "45"]})
    cfg = {"age": "numeric"}
    flags = check(df, cfg, **run_ctx)
    assert any(f["rule"] == "type_mismatch" and f["column"] == "age" for f in flags)

def test_flags_non_datetime(run_ctx):
    df = pd.DataFrame({"admit_ts": ["not-a-date", "2026-07-17 08:00"]})
    cfg = {"admit_ts": "datetime"}
    flags = check(df, cfg, **run_ctx)
    assert any(f["rule"] == "type_mismatch" and f["column"] == "admit_ts" for f in flags)

def test_no_flags_correct_types(run_ctx):
    df = pd.DataFrame({"age": [45, 30], "admit_ts": pd.to_datetime(["2026-07-17", "2026-07-17"])})
    cfg = {"age": "numeric", "admit_ts": "datetime"}
    flags = check(df, cfg, **run_ctx)
    assert flags == []
