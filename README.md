# Healthcare Data QC Pipeline

Daily batch QC on healthcare data per client and domain. Observes, measures,
compares over time, and raises flags. Never modifies source data.

## Quick start (local dev)

    python3.11 -m venv .venv && source .venv/bin/activate
    pip install -e ".[dev]"
    pytest tests/

## Run pipeline manually

    python -m qc.main --date 2026-07-20

## Run dashboard

    QC_OUTPUT_PATH=/data/output \
    QC_HISTORY_PATH=/data/history \
    QC_CONFIG_PATH=config/suites \
    gunicorn "qc.dashboard.app:app" --bind 0.0.0.0:8080

Dashboard pages:
- `/` — Summary: status per client+domain, color-coded pass/fail
- `/issues` — Unified issues drilldown: check flags + temporal flags in one table
- `/trends` — D/W/M metric comparison (reads DuckDB history)
- `/config` — Edit check suite YAMLs with live validation

## Adding a new check (config only)

Edit `config/suites/<client>/<domain>.yaml` — no code changes needed.

Example logic rules (LWBS, DTR, DTP):

    checks:
      - logic:
          lwbs:
            description: "Left Without Being Seen"
            condition: "disposition == 'LWBS'"
            max_rate: 0.05
            severity: warn
          dtr_exceeded:
            description: "Door-to-Room > 60 min"
            condition: "door_to_room_minutes > 60"
            severity: warn

## Adding a custom check (plugin)

Drop a function in `src/qc/checks/custom_<name>.py`, decorate with `@register`,
import it in `main.py`. Same contract as built-in checks:

    from qc.checks.registry import register, flags_from

    @register("los_plausibility", domains=["inpatient"])
    def check(df, cfg, *, run_date, client, domain):
        bad = df[(df["discharge_ts"] - df["admit_ts"]).dt.days > cfg["max_los_days"]]
        return flags_from(bad, rule="los_plausibility", severity="warn",
                          run_date=run_date, client=client, domain=domain)

See `config/suites/ohiohealth/inpatient.yaml` for usage.

## S3 source path layout

    s3://<bucket>/landing/date=YYYY-MM-DD/client=<client>/domain=<domain>/

## Output path layout (EBS only — no S3 writes)

    /data/output/
    ├── qc_issues/client=<c>/domain=<d>/date=<date>/issues.parquet
    ├── qc_delta/client=<c>/domain=<d>/date=<date>/delta_report.json
    └── run_summary/client=<c>/date=<date>/summary.json

## EC2 deploy

    bash scripts/bootstrap_ec2.sh
