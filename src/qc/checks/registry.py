import pandas as pd

_REGISTRY: dict[str, dict] = {}


def register(rule_name: str, domains: list[str] | None = None):
    def decorator(fn):
        _REGISTRY[rule_name] = {"fn": fn, "domains": domains}
        return fn
    return decorator


def get_checks(domain: str) -> dict[str, callable]:
    return {
        name: entry["fn"]
        for name, entry in _REGISTRY.items()
        if entry["domains"] is None or domain in entry["domains"]
    }


def flags_from(
    df: pd.DataFrame,
    rule: str,
    severity: str,
    run_date: str,
    client: str,
    domain: str,
    column: str | None = None,
    detail: str | None = None,
) -> list[dict]:
    flags = []
    for idx, row in df.iterrows():
        flags.append({
            "run_date": run_date,
            "client": client,
            "domain": domain,
            "row_id": idx,
            "column": column,
            "rule": rule,
            "severity": severity,
            "detail": detail,
        })
    return flags
