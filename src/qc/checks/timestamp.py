# src/qc/checks/timestamp.py
import pandas as pd
from qc.checks.registry import flags_from


def check(df: pd.DataFrame, cfg: dict, *, run_date: str, client: str, domain: str) -> list[dict]:
    flags = []
    ctx = dict(run_date=run_date, client=client, domain=domain)
    for col in cfg.get("columns", []):
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce")
        bad = parsed.isna() & df[col].notna()
        if bad.any():
            flags.extend(flags_from(
                df[bad], rule="invalid_timestamp", severity="error",
                column=col, detail="unparseable timestamp", **ctx,
            ))
    order = cfg.get("order", {})
    before_col = order.get("before")
    after_col = order.get("after")
    if before_col and after_col and before_col in df.columns and after_col in df.columns:
        before_ts = pd.to_datetime(df[before_col], errors="coerce")
        after_ts = pd.to_datetime(df[after_col], errors="coerce")
        bad = (after_ts < before_ts) & before_ts.notna() & after_ts.notna()
        if bad.any():
            flags.extend(flags_from(
                df[bad], rule="timestamp_order", severity="error",
                column=f"{after_col}<{before_col}",
                detail=f"{after_col} precedes {before_col}", **ctx,
            ))
    return flags
