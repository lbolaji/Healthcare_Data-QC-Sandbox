# src/qc/checks/missing.py
import pandas as pd
from qc.checks.registry import flags_from


def check(df: pd.DataFrame, cfg: dict, *, run_date: str, client: str, domain: str) -> list[dict]:
    flags = []
    required = cfg.get("columns", [])
    max_null_rate = cfg.get("max_null_rate", 0.0)

    for col in required:
        if col not in df.columns:
            flags.append({
                "run_date": run_date, "client": client, "domain": domain,
                "row_id": None, "column": col, "rule": "missing_column",
                "severity": "error", "detail": f"required column '{col}' absent",
            })
            continue
        null_mask = df[col].isna()
        null_rate = null_mask.mean()
        if null_rate > max_null_rate:
            flags.extend(flags_from(
                df[null_mask], rule="null_value", severity="error",
                column=col, detail=f"null_rate={null_rate:.3f} max={max_null_rate}",
                run_date=run_date, client=client, domain=domain,
            ))
    return flags
