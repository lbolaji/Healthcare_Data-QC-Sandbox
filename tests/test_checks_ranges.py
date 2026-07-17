# tests/test_checks_ranges.py
import pandas as pd
from qc.checks.ranges import check

def test_flags_out_of_numeric_range(run_ctx):
    df = pd.DataFrame({"age": [45, 200, -1, 30]})
    cfg = {"age": {"min": 0, "max": 120}}
    flags = check(df, cfg, **run_ctx)
    assert len([f for f in flags if f["column"] == "age"]) == 2

def test_flags_invalid_category(run_ctx):
    df = pd.DataFrame({"disposition": ["DISCHARGE", "UNKNOWN", "LWBS"]})
    cfg = {"disposition": {"allowed": ["DISCHARGE", "LWBS", "TRANSFER", "AMA"]}}
    flags = check(df, cfg, **run_ctx)
    assert len([f for f in flags if f["column"] == "disposition"]) == 1

def test_no_flags_in_range(run_ctx):
    df = pd.DataFrame({"age": [0, 60, 120]})
    cfg = {"age": {"min": 0, "max": 120}}
    flags = check(df, cfg, **run_ctx)
    assert flags == []
