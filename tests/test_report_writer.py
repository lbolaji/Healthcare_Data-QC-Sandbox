import json
import os
import pandas as pd
from qc.report.writer import write_artifacts

ISSUES = [
    {"run_date": "2026-07-17", "client": "ohiohealth", "domain": "ed",
     "row_id": 1, "column": "age", "rule": "range", "severity": "error",
     "detail": "value=200 max=120"},
]
DELTA_FLAGS = [
    {"run_date": "2026-07-17", "client": "ohiohealth", "domain": "ed",
     "row_id": None, "column": "row_count", "rule": "delta_row_count",
     "severity": "error", "detail": "day_over_day pct_change=51%"},
]
DOMAIN_SUMMARY = {
    "status": "passed",
    "rows_read": 100,
    "checks_run": ["missing", "ranges"],
    "flags_raised": 1,
    "by_check": {"missing": {"flags": 0, "columns": []}, "ranges": {"flags": 1, "columns": ["age"]}},
}

def test_issues_parquet_written(tmp_output):
    write_artifacts(tmp_output, "ohiohealth", "ed", "2026-07-17", ISSUES, [], DOMAIN_SUMMARY)
    path = os.path.join(tmp_output, "qc_issues", "client=ohiohealth", "domain=ed", "date=2026-07-17", "issues.parquet")
    assert os.path.exists(path)
    df = pd.read_parquet(path)
    assert len(df) == 1
    assert df.iloc[0]["rule"] == "range"

def test_delta_report_written(tmp_output):
    write_artifacts(tmp_output, "ohiohealth", "ed", "2026-07-17", [], DELTA_FLAGS, DOMAIN_SUMMARY)
    path = os.path.join(tmp_output, "qc_delta", "client=ohiohealth", "domain=ed", "date=2026-07-17", "delta_report.json")
    assert os.path.exists(path)
    report = json.loads(open(path).read())
    assert report["client"] == "ohiohealth"
    assert len(report["flags"]) == 1

def test_run_summary_written(tmp_output):
    write_artifacts(tmp_output, "ohiohealth", "ed", "2026-07-17", ISSUES, DELTA_FLAGS, DOMAIN_SUMMARY)
    path = os.path.join(tmp_output, "run_summary", "client=ohiohealth", "date=2026-07-17", "summary.json")
    assert os.path.exists(path)
    summary = json.loads(open(path).read())
    assert summary["client"] == "ohiohealth"
    assert summary["domains"]["ed"]["flags_raised"] == 1
