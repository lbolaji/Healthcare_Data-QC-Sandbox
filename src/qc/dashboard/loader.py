# src/qc/dashboard/loader.py
import glob
import json
import logging
import os
import pandas as pd

logger = logging.getLogger(__name__)


def get_output_path() -> str:
    return os.environ.get("QC_OUTPUT_PATH", "/data/output")


def load_latest_summaries() -> list[dict]:
    base = get_output_path()
    pattern = os.path.join(base, "run_summary", "client=*", "date=*", "summary.json")
    rows = []
    for path in sorted(glob.glob(pattern)):
        parts = path.split(os.sep)
        client = parts[-3].split("=", 1)[1]
        date = parts[-2].split("=", 1)[1]
        try:
            with open(path) as f:
                summary = json.load(f)
        except (OSError, json.JSONDecodeError):
            logger.warning("skipping unreadable summary file: %s", path)
            continue
        for domain, meta in summary.get("domains", {}).items():
            rows.append({
                "client": client,
                "domain": domain,
                "run_date": date,
                "status": meta.get("status", "unknown"),
                "rows_read": meta.get("rows_read", 0),
                "flags_raised": meta.get("flags_raised", 0),
            })
    return rows


def load_issues(client: str | None, domain: str | None, run_date: str | None,
                rule: str | None, severity: str | None, page: int = 1, page_size: int = 100) -> tuple[pd.DataFrame, int]:
    base = get_output_path()
    client_glob = f"client={client}" if client else "client=*"
    domain_glob = f"domain={domain}" if domain else "domain=*"
    date_glob = f"date={run_date}" if run_date else "date=*"
    pattern = os.path.join(base, "qc_issues", client_glob, domain_glob, date_glob, "issues.parquet")
    frames = []
    for p in glob.glob(pattern):
        try:
            frames.append(pd.read_parquet(p))
        except Exception:
            logger.warning("skipping unreadable parquet file: %s", p)
    if not frames:
        return pd.DataFrame(), 0
    df = pd.concat(frames, ignore_index=True)
    if rule:
        df = df[df["rule"] == rule]
    if severity:
        df = df[df["severity"] == severity]
    total = len(df)
    start = (page - 1) * page_size
    return df.iloc[start:start + page_size], total


def load_delta_report(client: str, domain: str, run_date: str) -> dict:
    base = get_output_path()
    path = os.path.join(base, "qc_delta", f"client={client}", f"domain={domain}", f"date={run_date}", "delta_report.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        logger.warning("skipping unreadable delta report: %s", path)
        return {}
