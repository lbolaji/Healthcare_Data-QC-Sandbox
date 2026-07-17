import pandas as pd
import pytest
from qc.checks.registry import register, get_checks, flags_from, run_check

def test_register_and_get_all_domains():
    @register("test_rule_all")
    def check(df, cfg):
        return []
    checks = get_checks("any_domain")
    assert "test_rule_all" in checks

def test_register_domain_scoped():
    @register("test_rule_ed", domains=["ed"])
    def check(df, cfg):
        return []
    assert "test_rule_ed" in get_checks("ed")
    assert "test_rule_ed" not in get_checks("inpatient")

def test_flags_from_returns_correct_schema():
    df = pd.DataFrame({"patient_id": ["A", "B"], "age": [200, 300]})
    bad = df[df["age"] > 120]
    flags = flags_from(bad, rule="range", severity="error", column="age",
                       detail="value exceeds max=120",
                       run_date="2026-07-17", client="ohiohealth", domain="ed")
    assert len(flags) == 2
    assert flags[0]["rule"] == "range"
    assert flags[0]["severity"] == "error"
    assert flags[0]["column"] == "age"
    assert flags[0]["client"] == "ohiohealth"
    assert "row_id" in flags[0]

def test_run_check_dispatches_missing(run_ctx):
    df = pd.DataFrame({"age": [30, 45]})  # missing required col patient_id
    cfg = {"columns": ["patient_id"], "max_null_rate": 0.0}
    flags = run_check("missing", df, cfg, **run_ctx)
    assert any(f["rule"] == "missing_column" for f in flags)

def test_run_check_raises_unknown():
    with pytest.raises(ValueError, match="unknown check"):
        run_check("nonexistent", pd.DataFrame(), {}, run_date="2026-07-17", client="x", domain="y")
