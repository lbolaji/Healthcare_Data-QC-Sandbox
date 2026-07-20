# src/qc/dashboard/loader.py
import glob
import json
import logging
import os
from datetime import date as date_cls, timedelta

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


def get_output_path() -> str:
    return os.environ.get("QC_OUTPUT_PATH", "/data/output")


def get_history_path() -> str:
    return os.environ.get("QC_HISTORY_PATH", "/data/history")


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


def load_trends(client: str, domain: str, run_date: str) -> list[dict]:
    """Return a list of metric rows, each with today/day/week/month values.

    Each row: {metric, today, day, week, month, day_delta, week_delta, month_delta}
    Deltas are percentage change for row_count/mean_*, absolute change for rates.
    Returns [] if DuckDB file not found or no data for the given run_date.
    """
    db_path = os.path.join(get_history_path(), "metrics.duckdb")
    if not os.path.exists(db_path):
        return []

    d = date_cls.fromisoformat(run_date)
    dates = {
        "today": str(d),
        "day":   str(d - timedelta(days=1)),
        "week":  str(d - timedelta(days=7)),
        "month": str(d - timedelta(days=30)),
    }

    snapshots = {}
    try:
        con = duckdb.connect(db_path, read_only=True)
        for label, dt in dates.items():
            row = con.execute(
                "SELECT metrics_json FROM metrics WHERE client=? AND domain=? AND run_date=?",
                [client, domain, dt],
            ).fetchone()
            snapshots[label] = json.loads(row[0]) if row else None
        con.close()
    except Exception:
        logger.warning("could not read trends from DuckDB for %s/%s", client, domain)
        return []

    today = snapshots.get("today")
    if not today:
        return []

    rate_metrics = {k for k in today if k.startswith("null_rate_") or k.startswith("dup_rate")}
    pct_metrics = {k for k in today if k not in rate_metrics}

    def delta(today_val, prior_val, is_rate: bool):
        if today_val is None or prior_val is None:
            return None
        if is_rate:
            return round(today_val - prior_val, 4)
        if prior_val == 0:
            return None
        return round((today_val - prior_val) / prior_val * 100, 2)

    rows = []
    for metric, today_val in sorted(today.items()):
        if not isinstance(today_val, (int, float)):
            continue
        is_rate = metric in rate_metrics
        row = {
            "metric": metric,
            "today": today_val,
            "day":   snapshots["day"].get(metric) if snapshots["day"] else None,
            "week":  snapshots["week"].get(metric) if snapshots["week"] else None,
            "month": snapshots["month"].get(metric) if snapshots["month"] else None,
        }
        row["day_delta"]   = delta(today_val, row["day"],   is_rate)
        row["week_delta"]  = delta(today_val, row["week"],  is_rate)
        row["month_delta"] = delta(today_val, row["month"], is_rate)
        row["is_rate"] = is_rate
        rows.append(row)
    return rows


def get_suites_path() -> str:
    return os.environ.get("QC_CONFIG_PATH", "config/suites")


def list_suites() -> list[dict]:
    """Return all suite files as [{client, domain, path}] sorted by client+domain."""
    base = get_suites_path()
    results = []
    for path in sorted(glob.glob(os.path.join(base, "*", "*.yaml"))):
        parts = path.split(os.sep)
        results.append({
            "client": parts[-2],
            "domain": parts[-1].replace(".yaml", ""),
            "path": path,
        })
    return results


def load_suite_config(client: str, domain: str) -> str | None:
    """Return raw YAML text for a suite, or None if not found."""
    path = os.path.join(get_suites_path(), client, f"{domain}.yaml")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        logger.warning("could not read suite: %s", path)
        return None


def save_suite_config(client: str, domain: str, yaml_text: str) -> str | None:
    """Validate and save YAML text. Returns error message string or None on success."""
    import yaml as _yaml
    try:
        _yaml.safe_load(yaml_text)
    except _yaml.YAMLError as exc:
        return f"Invalid YAML: {type(exc).__name__}"
    base = get_suites_path()
    client_dir = os.path.join(base, client)
    os.makedirs(client_dir, exist_ok=True)
    path = os.path.join(client_dir, f"{domain}.yaml")
    try:
        with open(path, "w") as f:
            f.write(yaml_text)
    except OSError:
        return "Could not write file — check permissions"
    return None


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
