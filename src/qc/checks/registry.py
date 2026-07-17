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


# Lazy imports to avoid circular imports at module load
_CHECK_MODULES: dict = {}

def _load_modules() -> dict:
    if not _CHECK_MODULES:
        from qc.checks import missing, types, ranges, dedup, timestamp, referential, logic
        _CHECK_MODULES.update({
            "missing": missing.check,
            "types": types.check,
            "ranges": ranges.check,
            "dedup": dedup.check,
            "timestamp": timestamp.check,
            "referential": referential.check,
            "logic": logic.check,
        })
    return _CHECK_MODULES


def run_check(name: str, df, cfg: dict, *, run_date: str, client: str, domain: str) -> list[dict]:
    modules = _load_modules()
    fn = modules.get(name)
    if fn is None:
        # also check plugin registry
        plugin = _REGISTRY.get(name)
        if plugin:
            return plugin["fn"](df, cfg, run_date=run_date, client=client, domain=domain)
        raise ValueError(f"unknown check: {name!r}")
    return fn(df, cfg, run_date=run_date, client=client, domain=domain)
