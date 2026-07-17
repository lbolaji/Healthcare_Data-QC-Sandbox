import json
import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def write_artifacts(
    output_path: str,
    client: str,
    domain: str,
    run_date: str,
    issues: list[dict],
    delta_flags: list[dict],
    domain_summary: dict,
) -> None:
    _write_issues(output_path, client, domain, run_date, issues)
    _write_delta(output_path, client, domain, run_date, delta_flags)
    _write_summary(output_path, client, domain, run_date, domain_summary)


def _write_issues(output_path, client, domain, run_date, issues):
    dir_path = os.path.join(output_path, "qc_issues", f"client={client}", f"domain={domain}", f"date={run_date}")
    os.makedirs(dir_path, exist_ok=True)
    df = pd.DataFrame(issues) if issues else pd.DataFrame(
        columns=["run_date", "client", "domain", "row_id", "column", "rule", "severity", "detail"]
    )
    df.to_parquet(os.path.join(dir_path, "issues.parquet"), index=False)
    logger.info("wrote %d issues for client=%s domain=%s", len(issues), client, domain)


def _write_delta(output_path, client, domain, run_date, delta_flags):
    dir_path = os.path.join(output_path, "qc_delta", f"client={client}", f"domain={domain}", f"date={run_date}")
    os.makedirs(dir_path, exist_ok=True)
    report = {"client": client, "domain": domain, "run_date": run_date, "flags": delta_flags}
    with open(os.path.join(dir_path, "delta_report.json"), "w") as f:
        json.dump(report, f, indent=2)


def _write_summary(output_path, client, domain, run_date, domain_summary):
    dir_path = os.path.join(output_path, "run_summary", f"client={client}", f"date={run_date}")
    os.makedirs(dir_path, exist_ok=True)
    summary_path = os.path.join(dir_path, "summary.json")
    # merge into existing summary if present (multiple domains per run)
    if os.path.exists(summary_path):
        with open(summary_path) as f:
            existing = json.load(f)
    else:
        existing = {"run_date": run_date, "client": client, "status": "passed", "domains": {}}
    existing["domains"][domain] = domain_summary
    # roll up status
    statuses = [d.get("status", "passed") for d in existing["domains"].values()]
    if all(s == "passed" for s in statuses):
        existing["status"] = "passed"
    elif all(s == "failed" for s in statuses):
        existing["status"] = "failed"
    else:
        existing["status"] = "partial_failure"
    with open(summary_path, "w") as f:
        json.dump(existing, f, indent=2)
