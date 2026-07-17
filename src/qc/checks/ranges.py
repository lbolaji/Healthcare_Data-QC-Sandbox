# src/qc/checks/ranges.py
import pandas as pd
from qc.checks.registry import flags_from


def check(df: pd.DataFrame, cfg: dict, *, run_date: str, client: str, domain: str) -> list[dict]:
    flags = []
    ctx = dict(run_date=run_date, client=client, domain=domain)
    for col, rule in cfg.items():
        if col not in df.columns:
            continue
        if "min" in rule or "max" in rule:
            numeric = pd.to_numeric(df[col], errors="coerce")
            bad = pd.Series([False] * len(df), index=df.index)
            if "min" in rule:
                bad |= numeric < rule["min"]
            if "max" in rule:
                bad |= numeric > rule["max"]
            if bad.any():
                flags.extend(flags_from(
                    df[bad], rule="range", severity="error", column=col,
                    detail=f"min={rule.get('min')} max={rule.get('max')}", **ctx,
                ))
        if "allowed" in rule:
            bad = ~df[col].isin(rule["allowed"]) & df[col].notna()
            if bad.any():
                flags.extend(flags_from(
                    df[bad], rule="invalid_category", severity="error", column=col,
                    detail=f"allowed={rule['allowed']}", **ctx,
                ))
    return flags
