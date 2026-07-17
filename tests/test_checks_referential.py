# tests/test_checks_referential.py
import pandas as pd
from qc.checks.referential import check

def test_flags_unresolved_fk(run_ctx):
    df = pd.DataFrame({"drg_code": ["123", "999", "456"]})
    cfg = {"drg_code": {"reference": ["123", "456", "789"]}}
    flags = check(df, cfg, **run_ctx)
    assert any(f["rule"] == "referential_integrity" and f["column"] == "drg_code" for f in flags)
    assert len([f for f in flags if f["column"] == "drg_code"]) == 1

def test_no_flags_all_fks_resolve(run_ctx):
    df = pd.DataFrame({"drg_code": ["123", "456"]})
    cfg = {"drg_code": {"reference": ["123", "456", "789"]}}
    flags = check(df, cfg, **run_ctx)
    assert flags == []
