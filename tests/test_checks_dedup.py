# tests/test_checks_dedup.py
import pandas as pd
from qc.checks.dedup import check

def test_flags_duplicate_keys(ed_df, run_ctx):
    cfg = {"key_columns": ["patient_id"]}
    # ed_df has P001 appearing twice (different visit times) — duplicate on patient_id alone
    flags = check(ed_df, cfg, **run_ctx)
    assert any(f["rule"] == "duplicate_key" for f in flags)

def test_no_flags_unique_keys(run_ctx):
    df = pd.DataFrame({"patient_id": ["P1", "P2"], "admit_ts": pd.to_datetime(["2026-07-17 08:00", "2026-07-17 09:00"])})
    cfg = {"key_columns": ["patient_id", "admit_ts"]}
    flags = check(df, cfg, **run_ctx)
    assert flags == []

def test_flags_duplicate_composite_key(run_ctx):
    """Two rows with same patient_id AND same admit_ts — composite key duplicate."""
    df = pd.DataFrame({
        "patient_id": ["P001", "P001", "P002"],
        "admit_ts": pd.to_datetime(["2026-07-17 08:00", "2026-07-17 08:00", "2026-07-17 09:00"]),
    })
    cfg = {"key_columns": ["patient_id", "admit_ts"]}
    flags = check(df, cfg, **run_ctx)
    assert any(f["rule"] == "duplicate_key" for f in flags)
    # Only the two P001/08:00 rows should be flagged, not P002
    assert len([f for f in flags if f["rule"] == "duplicate_key"]) == 2
