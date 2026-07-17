# tests/test_integration.py
"""End-to-end integration smoke test for the QC pipeline using moto S3 mocking."""
import io
import os
import json
import duckdb
import pandas as pd
import pytest
import boto3
from moto import mock_aws

from qc.main import main

BUCKET = "test-source-bucket"
RUN_DATE = "2026-07-17"

# Absolute path to the real project config so main() can resolve suites/
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "config.yaml")


@mock_aws
def test_full_pipeline_run(tmp_duckdb, tmp_output, monkeypatch):
    """Full pipeline: S3 → ingest → checks → temporal → write artifacts."""
    # Set AWS credentials so boto3 doesn't try to find real ones
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    # --- Create mock bucket and upload synthetic CSV ---
    # Row with age=200 intentionally violates the range check (max=120 in ed.yaml)
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    df_in = pd.DataFrame({
        "patient_id": ["P001", "P002", "P003"],
        "admit_ts": pd.to_datetime([
            "2026-07-17 08:00", "2026-07-17 09:00", "2026-07-17 10:00"
        ]),
        "discharge_ts": pd.to_datetime([
            "2026-07-17 10:00", "2026-07-17 11:00", "2026-07-17 12:00"
        ]),
        "age": [45, 30, 200],
        "disposition": ["DISCHARGE", "LWBS", "TRANSFER"],
    })
    buf = io.BytesIO()
    df_in.to_csv(buf, index=False)
    buf.seek(0)
    s3.put_object(
        Bucket=BUCKET,
        Key=f"landing/date={RUN_DATE}/client=ohiohealth/domain=ed/data.csv",
        Body=buf.read(),
    )

    # --- Build config pointing to tmp dirs ---
    history_dir = os.path.dirname(tmp_duckdb)
    config = {
        "source": {"bucket": BUCKET, "prefix": "landing"},
        "output": {"path": tmp_output},
        "history": {"path": history_dir},
        "thresholds": {
            "row_count_max_pct_change": 0.30,
            "null_rate_max_abs_change": 0.05,
        },
    }

    # config_path must point to the real config/config.yaml so that
    # main() resolves config_dir and finds suites/ohiohealth/ed.yaml
    exit_code = main(RUN_DATE, config, config_path=CONFIG_PATH)

    # --- Assertions ---

    # 1. Pipeline exited cleanly
    assert exit_code == 0, "expected exit code 0"

    # 2. issues.parquet exists and contains at least one flag
    #    (age=200 must trigger the range check: max=120 in ed.yaml)
    issues_path = os.path.join(
        tmp_output,
        "qc_issues",
        "client=ohiohealth",
        "domain=ed",
        f"date={RUN_DATE}",
        "issues.parquet",
    )
    assert os.path.exists(issues_path), f"issues.parquet not found: {issues_path}"
    issues_df = pd.read_parquet(issues_path)
    assert len(issues_df) > 0, "expected ≥1 issue flag (age=200 should fail range check)"

    # 3. delta_report.json exists
    delta_path = os.path.join(
        tmp_output,
        "qc_delta",
        "client=ohiohealth",
        "domain=ed",
        f"date={RUN_DATE}",
        "delta_report.json",
    )
    assert os.path.exists(delta_path), f"delta_report.json not found: {delta_path}"

    # 4. summary.json exists with correct domain metadata
    summary_path = os.path.join(
        tmp_output,
        "run_summary",
        "client=ohiohealth",
        f"date={RUN_DATE}",
        "summary.json",
    )
    assert os.path.exists(summary_path), f"summary.json not found: {summary_path}"
    with open(summary_path) as f:
        summary = json.load(f)
    assert "ed" in summary["domains"], "expected 'ed' domain in summary"
    assert summary["domains"]["ed"]["rows_read"] == 3

    # 5. DuckDB metrics table was updated with the snapshot
    con = duckdb.connect(tmp_duckdb, read_only=True)
    row = con.execute(
        "SELECT row_count FROM metrics WHERE client='ohiohealth' AND domain='ed' AND run_date=?",
        [RUN_DATE],
    ).fetchone()
    con.close()
    assert row is not None, "DuckDB metrics row not written for ohiohealth/ed"
    assert row[0] == 3, f"expected row_count=3, got {row[0]}"

    # 6. Source S3 was not modified — no writes beyond the original landing key
    keys = [
        o["Key"]
        for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    ]
    assert all(k.startswith("landing/") for k in keys), (
        f"unexpected writes to source S3 bucket: {keys}"
    )
