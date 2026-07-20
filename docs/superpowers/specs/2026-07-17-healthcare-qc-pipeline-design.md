# Healthcare Data QC Batch Pipeline — Design Spec

**Date:** 2026-07-17
**Author:** Lanre Bolaji
**Status:** Approved

---

## Purpose

Run automated Quality Control (QC) on daily healthcare data per client and domain. The pipeline observes, measures, compares over time, and raises flags. It never cleans or modifies source data. Source stays untouched.

---

## Hosting

Single EC2 sandbox instance. No Kafka, no streaming platform, no managed cluster. Self-contained — easy to stand up, snapshot, tear down, and later promote.

---

## Section 1: Project Structure

```
qc-sandbox/
├── README.md
├── pyproject.toml                  # pandas, pyarrow, duckdb, boto3
├── config/
│   ├── config.yaml                 # S3 source bucket/prefix, output path, poll interval, thresholds
│   └── suites/                     # YAML check suites — one per client+domain, auto-discovered
│       └── <client>/<domain>.yaml
├── src/qc/
│   ├── main.py                     # orchestrates one run: iterates clients → domains → checks
│   ├── ingest/
│   │   └── reader.py               # read-only: discovers clients+domains from S3, loads CSV/Parquet
│   ├── checks/
│   │   ├── missing.py              # required field presence
│   │   ├── types.py                # data type validation
│   │   ├── ranges.py               # numeric/categorical bounds
│   │   ├── dedup.py                # duplicate key detection
│   │   ├── timestamp.py            # invalid/out-of-order timestamps
│   │   ├── referential.py          # FK/reference integrity
│   │   ├── logic.py                # business rules (LWBS rate, discharge before admit, etc.)
│   │   └── registry.py             # plugin registry for custom checks
│   ├── temporal/
│   │   └── compare.py              # D/W/M variation + drift flags
│   ├── metrics/
│   │   └── store.py                # DuckDB history store — read prior snapshots, append today
│   ├── report/
│   │   └── writer.py               # writes all artifacts to /data/output only
│   └── alert/
│       └── notify.py               # stubbed — logs threshold breaches; SNS disabled until configured
├── scripts/
│   ├── poll_and_run.sh             # cron target: polls S3 for new data, fires main.py
│   └── bootstrap_ec2.sh            # installs Python, venv, deps, CloudWatch agent
├── deploy/
│   ├── qc.service                  # systemd unit
│   ├── qc.timer                    # systemd timer — drives poll_and_run.sh
│   └── cloudwatch-agent.json       # log + metric shipping config
├── data/                           # EBS working dir (gitignored)
│   ├── history/                    # DuckDB metrics store (metrics.duckdb)
│   └── output/                     # final output artifacts
└── tests/
```

### Module Responsibilities

| Module | Responsibility |
|---|---|
| `main.py` | Resolves run date, loads config, iterates clients → domains: read → check → compare → write → alert |
| `ingest/reader.py` | Only module that touches source; read-only; discovers clients+domains dynamically from S3 |
| `checks/*` | Pure functions: `(df, rule_config) → flags`. No side effects on data |
| `temporal/compare.py` | Computes today's metrics; queries DuckDB for D-1/D-7/D-30; emits variation flags |
| `metrics/store.py` | Appends today's metric snapshot to DuckDB; reads prior snapshots for comparison |
| `report/writer.py` | Only module that writes to `/data/output` |
| `alert/notify.py` | Stubbed: logs threshold breaches; SNS publish is a no-op until configured |

---

## Section 2: Data Flow

```
S3 source (read-only)
s3://<source-bucket>/landing/date=2026-07-17/client=ohiohealth/domain=ed/
    │
    │  poll_and_run.sh polls S3 every N minutes for today's prefix
    │  sentinel file /data/history/.last_run prevents duplicate runs
    │
    ▼
reader.py
    - lists S3 prefixes → discovers clients and domains dynamically
    - loads each client+domain CSV/Parquet into a DataFrame
    - read-only: never writes back to S3
    │
    ▼  (per client, per domain)
checks/*
    - each module receives (df, rule_config) → returns flag rows
    - registry.py routes custom/plugin checks
    - output: [{client, domain, run_date, row_id, column, rule, severity, detail}]
    │
    ▼
temporal/compare.py
    - computes today's metric snapshot {row_count, null_rates, min/max/mean, dup_rate}
    - queries DuckDB for same client+domain at D-1, D-7, D-30
    - emits variation flags when change exceeds configured tolerance
    │
    ├──▶ metrics/store.py
    │        appends today's snapshot to /data/history/metrics.duckdb
    │
    ▼
report/writer.py
    /data/output/qc_issues/client=ohiohealth/domain=ed/date=2026-07-17/issues.parquet
    /data/output/qc_delta/client=ohiohealth/domain=ed/date=2026-07-17/delta_report.json
    /data/output/run_summary/client=ohiohealth/date=2026-07-17/summary.json
    │
    ▼
alert/notify.py (stubbed)
    - logs threshold breaches to CloudWatch
    - SNS publish is a no-op until configured
```

**One-way data contract:**
- Data flows **in** from S3 source only — never written back
- Only **flag artifacts** flow **out** to `/data/output`
- DuckDB stores only **metrics** (counts, rates, aggregates) — never raw records or PHI

---

## Section 3: Client & Domain Discovery

Clients and domains are discovered dynamically from S3 — no hardcoded list.

```
s3://<source-bucket>/landing/
    date=2026-07-17/client=ohiohealth/domain=ed/
    date=2026-07-17/client=ohiohealth/domain=inpatient/
    date=2026-07-17/client=ssmhealth/domain=ed/
```

`reader.py` lists the S3 prefix for today's date, extracts all `client=` and `domain=` partitions present, and iterates over them. If a client or domain has no data for a given day it is skipped gracefully.

DuckDB metrics history is always scoped per client — OhioHealth drift never bleeds into SSMHealth's baseline:

```
client      | domain    | run_date   | row_count | null_rate_admit_ts | mean_age | dup_rate
ohiohealth  | ed        | 2026-07-17 | 12400     | 0.02               | 45.3     | 0.001
ssmhealth   | ed        | 2026-07-17 | 9800      | 0.03               | 44.1     | 0.000
```

---

## Section 4: QC Checks

All checks flag — never fix. Built-in checks cover:

| Check | Flags when… |
|---|---|
| Missing column/field | A required column/field is absent |
| Empty/null value | Required values are blank/null beyond allowed rate |
| Type | A value doesn't match the expected data type |
| Range/domain | Numeric out of bounds or categorical outside allowed set |
| Duplicate (dedup) | Duplicate keys/records detected |
| Timestamp | Invalid, out-of-order, or out-of-window timestamps |
| Referential integrity | Keys that don't resolve to their reference table |
| Logic/business rule | Domain-specific rule violated (e.g. LWBS rate, discharge before admit) |
| Delta/variation | Today's metrics deviate from D/W/M history beyond threshold |
| Custom/user-defined | Any additional rule added via config or plugin |

### Adding Checks — Two Paths

**Path 1 — Config only (no code):**

Add an expectation to `config/suites/<client>/<domain>.yaml`. Covers the common cases: new column bounds, new not-null fields, new category sets, business rule conditions.

```yaml
# config/suites/ohiohealth/ed.yaml
client: ohiohealth
domain: ed
checks:
  - missing:
      columns: [patient_id, admit_ts, disposition]
  - ranges:
      age: {min: 0, max: 120}
  - logic:
      lwbs:
        description: "Left Without Being Seen rate"
        condition: "disposition == 'LWBS'"
        max_rate: 0.05          # flag if >5% of visits are LWBS
        severity: warn
      discharge_before_admit:
        condition: "discharge_ts < admit_ts"
        severity: error
  - delta:
      row_count:    {max_pct_change: 0.30}
      null_rate:    {max_abs_change: 0.05}
```

**Path 2 — Custom plugin (for logic YAML can't express):**

Drop a function into `src/qc/checks/` and register it. Same contract as built-in checks.

```python
# src/qc/checks/custom_los.py
from qc.checks.registry import register, flags_from

@register("los_plausibility", domains=["inpatient"])
def check(df, cfg, *, run_date, client, domain):
    bad = df[(df["discharge_ts"] - df["admit_ts"]).dt.days > cfg["max_los_days"]]
    return flags_from(bad, rule="los_plausibility", severity="warn",
                      run_date=run_date, client=client, domain=domain)
```

```yaml
# config/suites/ohiohealth/inpatient.yaml
checks:
  - los_plausibility:
      max_los_days: 120
```

Adding a domain = adding a suite YAML + any needed plugins. The engine requires no changes.

---

## Section 5: Temporal Comparison

For every client+domain the engine records a metric snapshot each run:

```
metrics(client, domain, run_date):
  row_count, null_rate[col], distinct_count[col],
  min/max/mean[numeric col], category_share[cat col], dup_rate
```

Each day it compares today vs.:
- **Day-over-day** — yesterday's snapshot
- **Week-over-week** — same weekday last week (7-day rolling)
- **Month-over-month** — same date last month (30-day rolling)

Variation flags are emitted when change exceeds configured tolerance (absolute or %). Results land in `qc.delta.report`. This catches silent upstream problems — a feed halving in size, a null rate spiking — that per-record checks alone miss.

---

## Section 6: Output Artifacts

Three artifacts per client, per domain, per day:

**`issues.parquet`** — per-record/per-column flags:
```
run_date   | client     | domain | row_id | column    | rule     | severity | detail
2026-07-17 | ohiohealth | ed     | 8821   | age       | range    | error    | value=312 max=120
2026-07-17 | ohiohealth | ed     | null   | admit_ts  | missing  | error    | required column absent
```

**`delta_report.json`** — D/W/M variation:
```json
{
  "client": "ohiohealth",
  "domain": "ed",
  "run_date": "2026-07-17",
  "comparisons": {
    "day_over_day": {"row_count": {"today": 6100, "prior": 12400, "pct_change": -0.51, "flagged": true}},
    "week_over_week": {"null_rate_admit_ts": {"today": 0.15, "prior": 0.02, "abs_change": 0.13, "flagged": true}}
  }
}
```

**`run_summary.json`** — run-level pass/fail with full what-failed-and-where detail:
```json
{
  "run_date": "2026-07-17",
  "client": "ohiohealth",
  "status": "partial_failure",
  "domains": {
    "ed": {
      "status": "passed",
      "rows_read": 12400,
      "checks_run": ["missing", "types", "ranges", "dedup", "timestamp", "logic"],
      "flags_raised": 43,
      "by_check": {
        "missing": {"flags": 10, "columns": ["admit_ts", "patient_id"]},
        "ranges":  {"flags": 33, "columns": ["age"]},
        "logic":   {"flags": 0}
      }
    },
    "inpatient": {
      "status": "failed",
      "error": "S3 key not found: landing/client=ohiohealth/date=2026-07-17/domain=inpatient/",
      "rows_read": 0,
      "flags_raised": 0
    }
  }
}
```

---

## Section 7: Error Handling & Observability

**Run-level error handling (`main.py`):**
- Each domain runs in a `try/except` — one domain failing does not abort others
- If a domain read fails (S3 key missing, malformed file): logged, skipped, recorded in `run_summary` as `status: failed`
- If all domains fail: exits non-zero so systemd detects it

**Polling logic (`poll_and_run.sh`):**
- Checks for today's S3 prefix; exits silently if absent — waits for next poll cycle
- Sentinel file `/data/history/.last_run_date` prevents re-running on same day's data
- If `main.py` exits non-zero: sentinel is not written, next poll retries

**PHI boundary — never logged:**
- No raw record values, patient IDs, or field content in any log
- Logs contain only: client, domain, run date, row counts, metric values, rule names, severity counts

**Observability:**
- `main.py` emits structured log lines → CloudWatch agent ships to CloudWatch Logs
- `notify.py` puts custom CloudWatch Metrics (`QC/ErrorRate`, `QC/NullRate`) — stubbed for now
- `run_summary.json` is the authoritative per-run record

---

## Section 8: Testing

**Unit tests — one file per check module:**
```
tests/
├── test_checks_missing.py        # synthetic DataFrame with missing cols → assert correct flags
├── test_checks_types.py
├── test_checks_ranges.py
├── test_checks_dedup.py
├── test_checks_timestamp.py
├── test_checks_referential.py
├── test_checks_logic.py          # LWBS rate, discharge before admit, custom conditions
├── test_temporal_compare.py      # seed DuckDB with known history → assert drift flags
├── test_metrics_store.py         # append snapshot → read back → assert values
├── test_report_writer.py         # assert output file structure and schema
└── test_registry.py              # register a custom check → assert it runs and returns flags
```

**Test data strategy:**
- Small synthetic DataFrames per domain — no real PHI ever in tests
- Known violations baked in; tests assert exact flag output (rule, column, row_id, severity)
- `conftest.py` provides shared fixtures: sample DataFrames, seeded DuckDB instance, temp output dir

**Config-driven test obligations:**
- `test_checks_logic.py` reads suite YAMLs and asserts each declared rule (e.g. LWBS threshold, los_plausibility) fires correctly against synthetic data
- Adding a rule to a suite YAML adds a test obligation — the test reads the config, not hardcoded values
- Users can add domain-specific queries (e.g. "what is the LWBS rate for this client?") as logic check conditions in the suite YAML; tests validate these automatically

**Integration smoke test (`tests/test_integration.py`):**
- Drops synthetic CSV into a temp S3 prefix (or local dir)
- Runs `main.py` end-to-end for one client + one domain
- Asserts all 3 output artifacts exist and are well-formed
- Confirms DuckDB was updated with today's snapshot
- Confirms source data was not modified

---

## Section 9: Lightweight Dashboard (Flask)

A minimal Flask app served from the same EC2 box, reading `/data/output` artifacts and the DuckDB history store directly. No separate database. If approved for BI later, the output format is already Parquet/JSON partitioned by client+domain+date — connecting Tableau/Power BI/Athena is a config change, not a rewrite.

### Structure

```
src/qc/dashboard/
├── app.py              # Flask app — routes + Jinja filters
├── loader.py           # reads /data/output Parquet + JSON + DuckDB into DataFrames
└── templates/
    ├── base.html
    ├── summary.html    # landing page
    ├── issues.html     # unified issues drilldown
    ├── trends.html     # D/W/M metric comparison table
    └── config.html     # YAML suite editor with live validation
```

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `QC_OUTPUT_PATH` | `/data/output` | Where the dashboard reads artifacts |
| `QC_HISTORY_PATH` | `/data/history` | DuckDB location for the Trends page |
| `QC_CONFIG_PATH` | `config/suites` | Suite YAML directory for the Config page |

### Pages

**Summary (`/`):**
- One row per client + domain, latest run date, status (passed/partial_failure/failed), total flags raised
- Color-coded status; links through to Issues for each row

**Issues drilldown (`/issues`):**
- Filter controls: client, domain, run date, rule, severity
- Single unified paginated table — check issues and temporal (delta) flags merged into one view
- Rule column distinguishes check flags (`range`, `logic`, `missing`, …) from temporal flags (`delta_row_count`, `delta_null_rate`)

**Trends (`/trends`):**
- Select client + domain + run date to load a D/W/M comparison table
- Columns: metric, Today, D-1, D-1 Δ, D-7, D-7 Δ, D-30, D-30 Δ
- Delta values color-coded: red (large increase), orange (moderate), green (stable or decrease)
- Reads snapshots directly from DuckDB (`QC_HISTORY_PATH/metrics.duckdb`)

**Check Config (`/config`):**
- Lists all existing suite YAMLs per client + domain in a sidebar
- Loads any suite into a dark-themed editor; supports creating new suites
- Live YAML validation on every keystroke (400ms debounce) — green ✅ when valid, red ❌ with exact error line/column when not; Save button disabled until valid
- Saves to `config/suites/<client>/<domain>.yaml`; changes take effect on next pipeline run
- Quick reference panel shows example logic rules (LWBS, DTR, DTP)

### Access

- Served on port 8080 bound to `0.0.0.0` (restrict via security group to private subnet)
- Access via SSH tunnel or VPN — no public ingress
- No auth for sandbox; add auth before any broader rollout

### Deployment

- `scripts/run_dashboard.sh` starts gunicorn as a systemd service
- `deploy/dashboard.service` — systemd unit alongside `qc.service`
- Start command: `QC_OUTPUT_PATH=/data/output QC_HISTORY_PATH=/data/history gunicorn "qc.dashboard.app:app" --bind 0.0.0.0:8080`

---

## Section 9: EC2 Sandbox Setup

| Item | Choice |
|---|---|
| Instance | t3.medium / t3.large — right-size after first runs |
| OS | Amazon Linux 2023 or Ubuntu LTS |
| Storage | gp3 EBS — app + history store (KMS-encrypted) |
| Runtime | Python 3.11 venv; deps via pyproject.toml (includes Flask/gunicorn) |
| Schedule | systemd timer drives poll_and_run.sh every 15 minutes |
| Trigger | poll_and_run.sh detects new S3 prefix → fires main.py |
| IAM role | Least privilege: read source S3, put CloudWatch logs/metrics, SNS publish (S3 output write deferred — sandbox writes to EBS only) |
| Network | Private subnet; VPC endpoints for S3/CloudWatch/KMS (no public ingress) |
| Observability | CloudWatch agent ships logs + custom QC metrics |

**Deploy flow:**
1. `bootstrap_ec2.sh` — installs Python/deps + CloudWatch agent
2. Clone repo
3. `deploy/qc.timer` enabled via systemd
4. Timer fires `scripts/poll_and_run.sh` every 15 minutes
5. `poll_and_run.sh` checks S3 for today's data → runs `src.qc.main`

---

## Section 10: Compliance Notes

- BAA with AWS in place before any PHI
- KMS encryption on EBS and output S3; TLS in transit
- Private VPC, no public endpoints; VPC endpoints for all AWS APIs
- Least-privilege IAM per the table above
- Audit logging to CloudWatch/CloudTrail
- PHI boundary enforced in code: record payloads never logged — metrics and identifiers only
