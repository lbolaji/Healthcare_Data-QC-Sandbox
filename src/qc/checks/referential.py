# src/qc/checks/referential.py
import pandas as pd
from qc.checks.registry import flags_from


def check(df: pd.DataFrame, cfg: dict, *, run_date: str, client: str, domain: str) -> list[dict]:
    flags = []
    for col, rule in cfg.items():
        if col not in df.columns:
            continue
        ref_set = set(rule.get("reference", []))
        bad = ~df[col].isin(ref_set) & df[col].notna()
        if bad.any():
            flags.extend(flags_from(
                df[bad], rule="referential_integrity", severity="error",
                column=col, detail=f"key not in reference set ({len(ref_set)} values)",
                run_date=run_date, client=client, domain=domain,
            ))
    return flags
