# src/qc/checks/logic.py
import pandas as pd
from qc.checks.registry import flags_from


def check(df: pd.DataFrame, cfg: dict, *, run_date: str, client: str, domain: str) -> list[dict]:
    flags = []
    ctx = dict(run_date=run_date, client=client, domain=domain)
    for rule_name, rule_cfg in cfg.items():
        condition = rule_cfg.get("condition")
        severity = rule_cfg.get("severity", "error")
        max_rate = rule_cfg.get("max_rate")
        if not condition:
            continue
        try:
            matched = df.query(condition)
        except Exception as exc:
            flags.append({
                "run_date": run_date, "client": client, "domain": domain,
                "row_id": None, "column": None, "rule": rule_name,
                "severity": "error", "detail": f"condition eval error: {type(exc).__name__}",
            })
            continue
        if max_rate is not None:
            rate = len(matched) / len(df) if len(df) > 0 else 0
            if rate > max_rate:
                flags.append({
                    "run_date": run_date, "client": client, "domain": domain,
                    "row_id": None, "column": None, "rule": rule_name,
                    "severity": severity,
                    "detail": f"rate={rate:.3f} exceeds max_rate={max_rate}",
                })
        else:
            if not matched.empty:
                flags.extend(flags_from(
                    matched, rule=rule_name, severity=severity,
                    column=None, detail=condition, **ctx,
                ))
    return flags
