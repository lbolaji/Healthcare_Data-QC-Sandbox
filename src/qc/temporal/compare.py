import pandas as pd


def compute_snapshot(df: pd.DataFrame) -> dict:
    snap: dict = {"row_count": len(df)}
    snap["dup_rate"] = df.duplicated().mean() if len(df) > 0 else 0.0
    for col in df.columns:
        null_rate = df[col].isna().mean()
        snap[f"null_rate_{col}"] = round(float(null_rate), 6)
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.notna().any():
            snap[f"mean_{col}"] = round(float(numeric.mean()), 6)
            snap[f"min_{col}"] = round(float(numeric.min()), 6)
            snap[f"max_{col}"] = round(float(numeric.max()), 6)
    return snap


def compare(
    today: dict,
    priors: dict[str, dict | None],
    thresholds: dict,
    *,
    run_date: str,
    client: str,
    domain: str,
) -> list[dict]:
    flags = []
    period_labels = {"day": "day_over_day", "week": "week_over_week", "month": "month_over_month"}

    for period, prior in priors.items():
        if prior is None:
            continue
        label = period_labels[period]

        today_count = today.get("row_count", 0)
        prior_count = prior.get("row_count", 0)
        if prior_count > 0:
            pct_change = abs(today_count - prior_count) / prior_count
            max_pct = thresholds.get("row_count_max_pct_change", 0.30)
            if pct_change > max_pct:
                flags.append({
                    "run_date": run_date, "client": client, "domain": domain,
                    "row_id": None, "column": "row_count", "rule": "delta_row_count",
                    "severity": "error",
                    "detail": f"{label} pct_change={pct_change:.2%} today={today_count} prior={prior_count}",
                })

        max_null_abs = thresholds.get("null_rate_max_abs_change", 0.05)
        for key in today:
            if not key.startswith("null_rate_"):
                continue
            if key not in prior:
                continue
            abs_change = abs(today[key] - prior[key])
            if abs_change > max_null_abs:
                col = key.replace("null_rate_", "")
                flags.append({
                    "run_date": run_date, "client": client, "domain": domain,
                    "row_id": None, "column": col, "rule": "delta_null_rate",
                    "severity": "warn",
                    "detail": f"{label} abs_change={abs_change:.3f} today={today[key]} prior={prior[key]}",
                })
    return flags
