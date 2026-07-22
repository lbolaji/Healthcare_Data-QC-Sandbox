# src/qc/main.py
import logging
import os
import sys
import yaml

from qc.ingest.reader import discover_clients_domains, load_domain
from qc.checks import missing, types, ranges, dedup, timestamp, referential, logic
import qc.checks  # ensure registry populated
from qc.temporal.compare import compute_snapshot, compare
from qc.metrics.store import init_db, append_snapshot, get_prior_snapshots
from qc.report.writer import write_artifacts
from qc.report.publisher import publish_artifacts
from qc.alert.notify import notify_threshold_breach

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

CHECK_MODULES = {
    "missing": missing.check,
    "types": types.check,
    "ranges": ranges.check,
    "dedup": dedup.check,
    "timestamp": timestamp.check,
    "referential": referential.check,
    "logic": logic.check,
}


def load_config(path: str = "config/config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_suite(config_dir: str, client: str, domain: str) -> dict:
    suite_path = os.path.join(config_dir, "suites", client, f"{domain}.yaml")
    if not os.path.exists(suite_path):
        logger.warning("no suite found at %s — skipping checks for %s/%s", suite_path, client, domain)
        return {"checks": []}
    with open(suite_path) as f:
        return yaml.safe_load(f)


def run_checks(df, suite: dict, *, run_date: str, client: str, domain: str) -> tuple[list[dict], dict]:
    all_flags = []
    by_check = {}
    checks_run = []
    ctx = dict(run_date=run_date, client=client, domain=domain)

    for check_block in suite.get("checks", []):
        for check_name, check_cfg in check_block.items():
            if check_name == "delta":
                continue  # handled in temporal comparison
            if check_name == "custom":
                # custom plugin checks handled via registry
                from qc.checks.registry import get_checks
                for rule_cfg in (check_cfg or []):
                    rule_name = rule_cfg.get("rule")
                    plugin_checks = get_checks(domain)
                    if rule_name in plugin_checks:
                        flags = plugin_checks[rule_name](df, rule_cfg, **ctx)
                        all_flags.extend(flags)
                        checks_run.append(rule_name)
                        flagged_cols = list({f["column"] for f in flags if f["column"]})
                        by_check[rule_name] = {"flags": len(flags), "columns": flagged_cols}
                continue
            fn = CHECK_MODULES.get(check_name)
            if fn is None:
                logger.warning("unknown check '%s' — skipping", check_name)
                continue
            flags = fn(df, check_cfg or {}, **ctx)
            all_flags.extend(flags)
            checks_run.append(check_name)
            flagged_cols = list({f["column"] for f in flags if f["column"]})
            by_check[check_name] = {"flags": len(flags), "columns": flagged_cols}

    return all_flags, {"checks_run": checks_run, "by_check": by_check}


def main(run_date: str, config: dict, config_path: str = "config/config.yaml") -> int:
    bucket = config["source"]["bucket"]
    prefix = config["source"]["prefix"]
    output_path = config["output"]["path"]
    history_path = config["history"]["path"]
    publish_cfg = config.get("publish", {})
    db_path = os.path.join(history_path, "metrics.duckdb")
    thresholds = config.get("thresholds", {})
    config_dir = os.path.dirname(os.path.abspath(config_path))

    os.makedirs(output_path, exist_ok=True)
    os.makedirs(history_path, exist_ok=True)
    init_db(db_path)

    pairs = discover_clients_domains(bucket, prefix, run_date)
    if not pairs:
        logger.warning("no client+domain data found for date=%s", run_date)
        return 0

    any_failed = False
    any_succeeded = False

    for client, domain in pairs:
        logger.info("processing client=%s domain=%s", client, domain)
        try:
            df = load_domain(bucket, prefix, client, domain, run_date)
            suite = load_suite(config_dir, client, domain)
            issues, check_meta = run_checks(df, suite, run_date=run_date, client=client, domain=domain)
            today_snapshot = compute_snapshot(df)
            priors = get_prior_snapshots(db_path, client, domain, run_date)
            delta_flags = compare(today_snapshot, priors, thresholds, run_date=run_date, client=client, domain=domain)
            append_snapshot(db_path, client, domain, run_date, today_snapshot)
            all_flags = issues + delta_flags
            domain_summary = {
                "status": "passed" if not any(f["severity"] == "error" for f in all_flags) else "failed",
                "rows_read": len(df),
                "flags_raised": len(all_flags),
                **check_meta,
            }
            write_artifacts(output_path, client, domain, run_date, issues, delta_flags, domain_summary)
            publish_artifacts(output_path, client, domain, run_date, publish_cfg)
            if all_flags:
                notify_threshold_breach(client, domain, run_date, all_flags)
            any_succeeded = True
            logger.info("done client=%s domain=%s rows=%d flags=%d", client, domain, len(df), len(all_flags))
        except Exception as exc:
            logger.error("failed client=%s domain=%s error_type=%s", client, domain, type(exc).__name__)
            write_artifacts(output_path, client, domain, run_date, [], [], {
                "status": "failed",
                "error": type(exc).__name__,
                "rows_read": 0,
                "checks_run": [],
                "flags_raised": 0,
                "by_check": {},
            })
            any_failed = True

    if any_failed and not any_succeeded:
        return 1
    return 0


if __name__ == "__main__":
    import argparse
    cfg = load_config()
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="run date YYYY-MM-DD")
    args = parser.parse_args()
    sys.exit(main(args.date, cfg, config_path="config/config.yaml"))
