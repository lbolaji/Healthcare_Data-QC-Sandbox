# src/qc/checks/dedup.py
import pandas as pd
from qc.checks.registry import flags_from


def check(df: pd.DataFrame, cfg: dict, *, run_date: str, client: str, domain: str) -> list[dict]:
    key_cols = cfg.get("key_columns", [])
    present = [c for c in key_cols if c in df.columns]
    if not present:
        return []
    dup_mask = df.duplicated(subset=present, keep=False)
    if not dup_mask.any():
        return []
    return flags_from(
        df[dup_mask], rule="duplicate_key", severity="error",
        column=",".join(present), detail=f"duplicate on {present}",
        run_date=run_date, client=client, domain=domain,
    )
