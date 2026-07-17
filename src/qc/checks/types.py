# src/qc/checks/types.py
import pandas as pd
from qc.checks.registry import flags_from

_TYPE_CHECKS = {
    "numeric": lambda s: pd.to_numeric(s, errors="coerce").isna() & s.notna(),
    "datetime": lambda s: pd.to_datetime(s, errors="coerce").isna() & s.notna(),
    "str": lambda s: ~s.apply(lambda v: isinstance(v, str)) & s.notna(),
}


def check(df: pd.DataFrame, cfg: dict, *, run_date: str, client: str, domain: str) -> list[dict]:
    flags = []
    for col, expected_type in cfg.items():
        if col not in df.columns:
            continue
        validator = _TYPE_CHECKS.get(expected_type)
        if validator is None:
            continue
        bad_mask = validator(df[col])
        if bad_mask.any():
            flags.extend(flags_from(
                df[bad_mask], rule="type_mismatch", severity="error",
                column=col, detail=f"expected {expected_type}",
                run_date=run_date, client=client, domain=domain,
            ))
    return flags
