"""Seed synthetic demo data for local dashboard demo.

Runs the QC pipeline for 4 dates and 2 clients (ohiohealth + ssmhealth)
without touching S3. Uses the local config (data/output, data/history).

Usage:
    python scripts/seed_demo.py
"""
import os
import sys
import json
import random

import pandas as pd

# Resolve project root so imports work regardless of CWD
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from qc.metrics.store import init_db, append_snapshot
from qc.report.writer import write_artifacts
from qc.temporal.compare import compute_snapshot, compare

DATES = ["2026-07-18", "2026-07-19", "2026-07-20", "2026-07-21"]
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "output")
HISTORY_PATH = os.path.join(PROJECT_ROOT, "data", "history")
DB_PATH = os.path.join(HISTORY_PATH, "metrics.duckdb")
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "config.local.yaml")

THRESHOLDS = {
    "row_count_max_pct_change": 0.30,
    "null_rate_max_abs_change": 0.05,
}

CLIENTS = {
    "ohiohealth": {
        "ed": {
            "base_rows": 120,
            "lwbs_rate": 0.06,   # intentionally > 5% threshold → flags
            "null_rate": 0.02,
        },
        "inpatient": {
            "base_rows": 45,
            "lwbs_rate": 0.0,
            "null_rate": 0.01,
        },
    },
    "ssmhealth": {
        "ed": {
            "base_rows": 95,
            "lwbs_rate": 0.03,
            "null_rate": 0.04,
        },
    },
}


def make_ed_df(n_rows: int, lwbs_rate: float, null_rate: float, date: str, seed: int) -> pd.DataFrame:
    rng = random.Random(seed)
    patient_ids = [f"P{i:04d}" for i in range(n_rows)]
    ages = [rng.randint(1, 90) for _ in range(n_rows)]
    # Inject a few range violations — age > 120
    for i in rng.sample(range(n_rows), max(1, n_rows // 40)):
        ages[i] = rng.randint(150, 300)
    dispositions = []
    for _ in range(n_rows):
        r = rng.random()
        if r < lwbs_rate:
            dispositions.append("LWBS")
        elif r < 0.6:
            dispositions.append("DISCHARGE")
        else:
            dispositions.append("TRANSFER")

    base_dt = pd.Timestamp(date + " 08:00")
    admit_ts = [base_dt + pd.Timedelta(minutes=rng.randint(0, 600)) for _ in range(n_rows)]
    discharge_ts = [a + pd.Timedelta(hours=rng.randint(1, 8)) for a in admit_ts]
    # Inject one discharge-before-admit for drama
    discharge_ts[0] = admit_ts[0] - pd.Timedelta(hours=2)

    # Inject nulls into admit_ts
    admit_arr = pd.array(admit_ts, dtype="datetime64[ns]")
    for i in rng.sample(range(n_rows), max(1, int(n_rows * null_rate))):
        admit_arr[i] = pd.NaT

    return pd.DataFrame({
        "patient_id": patient_ids,
        "admit_ts": admit_arr,
        "discharge_ts": discharge_ts,
        "age": ages,
        "disposition": dispositions,
    })


def make_inpatient_df(n_rows: int, null_rate: float, date: str, seed: int) -> pd.DataFrame:
    rng = random.Random(seed)
    patient_ids = [f"IP{i:04d}" for i in range(n_rows)]
    admit_ts = [pd.Timestamp(date + " 08:00") + pd.Timedelta(hours=rng.randint(0, 12)) for _ in range(n_rows)]
    discharge_ts = [a + pd.Timedelta(days=rng.randint(1, 10)) for a in admit_ts]
    ages = [rng.randint(18, 90) for _ in range(n_rows)]
    drg_codes = [rng.choice(["DRG470", "DRG291", "DRG392", "DRG871"]) for _ in range(n_rows)]

    admit_arr = pd.array(admit_ts, dtype="datetime64[ns]")
    for i in rng.sample(range(n_rows), max(1, int(n_rows * null_rate))):
        admit_arr[i] = pd.NaT

    return pd.DataFrame({
        "patient_id": patient_ids,
        "admit_ts": admit_arr,
        "discharge_ts": discharge_ts,
        "age": ages,
        "drg_code": drg_codes,
    })


def run_checks_synthetic(df: pd.DataFrame, client: str, domain: str, run_date: str) -> list[dict]:
    """Run a subset of checks inline without loading suite YAML."""
    from qc.checks import missing, ranges, logic
    ctx = dict(run_date=run_date, client=client, domain=domain)
    flags = []

    # missing check
    flags += missing.check(df, {"columns": ["patient_id", "admit_ts"]}, **ctx)

    # ranges check (age only)
    flags += ranges.check(df, {"age": {"min": 0, "max": 120}}, **ctx)

    # logic check — discharge before admit + LWBS rate
    flags += logic.check(df, {
        "discharge_before_admit": {
            "condition": "discharge_ts < admit_ts",
            "severity": "error",
        },
        "lwbs_rate": {
            "condition": "disposition == 'LWBS'",
            "max_rate": 0.05,
            "severity": "warn",
        },
    }, **ctx) if domain == "ed" else []

    return flags


def seed():
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    os.makedirs(HISTORY_PATH, exist_ok=True)
    init_db(DB_PATH)

    import yaml
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)

    total = 0
    for date_idx, run_date in enumerate(DATES):
        for client, domains in CLIENTS.items():
            for domain, params in domains.items():
                seed_val = hash((run_date, client, domain)) % (2**31)
                n = params["base_rows"] + random.Random(seed_val).randint(-10, 10)

                if domain == "ed":
                    df = make_ed_df(n, params["lwbs_rate"], params["null_rate"], run_date, seed_val)
                else:
                    df = make_inpatient_df(n, params["null_rate"], run_date, seed_val)

                issues = run_checks_synthetic(df, client, domain, run_date)
                snapshot = compute_snapshot(df)
                priors = {}
                # Read prior snapshots from DuckDB if they exist
                try:
                    from qc.metrics.store import get_prior_snapshots
                    priors = get_prior_snapshots(DB_PATH, client, domain, run_date)
                except Exception:
                    pass

                delta_flags = compare(snapshot, priors, THRESHOLDS, run_date=run_date, client=client, domain=domain)
                append_snapshot(DB_PATH, client, domain, run_date, snapshot)

                all_flags = issues + delta_flags
                domain_summary = {
                    "status": "passed" if not any(f["severity"] == "error" for f in all_flags) else "failed",
                    "rows_read": len(df),
                    "flags_raised": len(all_flags),
                    "checks_run": ["missing", "ranges", "logic"],
                    "by_check": {
                        "missing": {"flags": sum(1 for f in issues if f["rule"] == "missing"), "columns": []},
                        "ranges": {"flags": sum(1 for f in issues if f["rule"] == "range"), "columns": ["age"]},
                        "logic": {"flags": sum(1 for f in issues if f["rule"] not in ("missing", "range")), "columns": []},
                    },
                }
                write_artifacts(OUTPUT_PATH, client, domain, run_date, issues, delta_flags, domain_summary)
                total += 1
                print(f"  {run_date}  {client}/{domain}  rows={len(df)}  flags={len(all_flags)}")

    print(f"\nDone. {total} domain-runs seeded.")
    print(f"Output: {OUTPUT_PATH}")
    print(f"History: {DB_PATH}")
    print()
    print("Start the dashboard with:")
    print(f"  QC_OUTPUT_PATH={OUTPUT_PATH} QC_HISTORY_PATH={HISTORY_PATH} QC_CONFIG_PATH={os.path.join(PROJECT_ROOT, 'config/suites')} gunicorn 'qc.dashboard.app:app' --bind 0.0.0.0:8080")


if __name__ == "__main__":
    seed()
