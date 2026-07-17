# Healthcare Data QC Pipeline

Daily batch QC on healthcare data per client and domain. Observes, measures,
compares over time, and raises flags. Never modifies source data.

## Quick start (local dev)

    python3.11 -m venv .venv && source .venv/bin/activate
    pip install -e ".[dev]"
    pytest tests/

## Run pipeline manually

    python -m qc.main --date 2026-07-17

## Run dashboard

    QC_OUTPUT_PATH=/data/output gunicorn "qc.dashboard.app:app" --bind 0.0.0.0:8080

## Adding a new check (config only)

Edit `config/suites/<client>/<domain>.yaml` — no code changes needed.

## Adding a custom check (plugin)

Drop a function in `src/qc/checks/custom_<name>.py`, decorate with `@register`,
import it in `main.py`. See `config/suites/ohiohealth/inpatient.yaml` for usage.

## EC2 deploy

    bash scripts/bootstrap_ec2.sh
