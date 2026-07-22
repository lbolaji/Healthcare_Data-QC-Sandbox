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

## Power BI via Athena (optional)

The pipeline can publish `issues.parquet` to S3 after each run so Power BI
can query it via Athena. Disabled by default — EBS-only until you enable it.

**1. Enable publishing in `config/config.yaml`:**

    publish:
      enabled: true
      bucket: "your-analytics-bucket"
      prefix: "qc-output"
      athena_database: "healthcare_qc"
      athena_results_bucket: "your-athena-results-bucket"
      athena_workgroup: "primary"

**2. Run the one-time Athena DDL:**

Open `scripts/setup_athena.sql` in the Athena Query Editor, replace the
bucket/prefix placeholders, and run it. Enable partition projection
(instructions in the file) to avoid running `MSCK REPAIR TABLE` after each
new date.

**3. Add the publisher permissions to the EC2 IAM role:**

Attach `deploy/iam_publisher_policy.json` to the EC2 instance role.
Replace `<your-analytics-bucket>` and `<your-athena-results-bucket>`
placeholders with your actual bucket names.

**4. Connect Power BI Desktop:**

- Get Data → Amazon Athena (requires Athena ODBC driver v2)
- Server: `athena.<region>.amazonaws.com`
- Port: `443`
- Database: `healthcare_qc`
- Workgroup: `primary` (or your workgroup from config)
- Table: `qc_issues`

For scheduled refresh via Power BI Service, use an On-premises data gateway
or switch to Power BI Dataflow with S3 connector instead of ODBC.
