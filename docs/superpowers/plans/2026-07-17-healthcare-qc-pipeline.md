# Healthcare Data QC Batch Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a daily batch QC pipeline on a single EC2 instance that reads healthcare data from S3 per client+domain, runs declarative + plugin-based checks, compares metrics against D/W/M history, writes flag artifacts to EBS, and serves a lightweight Flask dashboard.

**Architecture:** `poll_and_run.sh` (systemd timer) detects new S3 data and fires `main.py`, which iterates discovered clients → domains → read → check → compare → write → alert. All check modules are pure functions `(df, rule_config) → flags`. DuckDB on EBS holds only metric snapshots for temporal comparison — never raw data or PHI.

**Tech Stack:** Python 3.11, pandas, pyarrow, duckdb, boto3, PyYAML, Flask, gunicorn, pytest

## Global Constraints

- Python 3.11 venv; all deps declared in `pyproject.toml`
- Source data is **read-only** — no writes back to S3, ever
- Output artifacts written to `/data/output` on EBS only (not S3 at this stage)
- DuckDB at `/data/history/metrics.duckdb` — metrics only, never raw records or PHI
- Logs must never contain record payloads, patient IDs, or field values — counts, rates, identifiers only
- All check modules must have the contract: `check(df: pd.DataFrame, cfg: dict) -> list[dict]`
- Flag dict schema: `{run_date, client, domain, row_id, column, rule, severity, detail}`
- `alert/notify.py` SNS calls are no-ops (stubbed) — do not make real AWS SNS calls
- Flask dashboard binds to `0.0.0.0:8080` private only — no public ingress
- No hardcoded client or domain names anywhere in engine code

---

## File Map

| File | Create/Modify | Purpose |
|---|---|---|
| `pyproject.toml` | Create | Project deps and metadata |
| `config/config.yaml` | Create | S3 source bucket/prefix, output paths, poll interval, thresholds |
| `config/suites/ohiohealth/ed.yaml` | Create | Sample suite for ohiohealth/ed domain |
| `config/suites/ohiohealth/inpatient.yaml` | Create | Sample suite with los_plausibility plugin |
| `src/qc/__init__.py` | Create | Package marker |
| `src/qc/ingest/__init__.py` | Create | Package marker |
| `src/qc/ingest/reader.py` | Create | Discover clients+domains from S3; load CSV/Parquet read-only |
| `src/qc/checks/__init__.py` | Create | Package marker |
| `src/qc/checks/registry.py` | Create | Plugin registry — `@register` decorator + `flags_from` helper |
| `src/qc/checks/missing.py` | Create | Required column/null-rate check |
| `src/qc/checks/types.py` | Create | Data type validation check |
| `src/qc/checks/ranges.py` | Create | Numeric bounds + categorical allowed-set check |
| `src/qc/checks/dedup.py` | Create | Duplicate key detection check |
| `src/qc/checks/timestamp.py` | Create | Invalid/out-of-order timestamp check |
| `src/qc/checks/referential.py` | Create | FK/reference integrity check |
| `src/qc/checks/logic.py` | Create | Business rule checks (LWBS rate, condition expressions) |
| `src/qc/metrics/__init__.py` | Create | Package marker |
| `src/qc/metrics/store.py` | Create | DuckDB read prior snapshots + append today |
| `src/qc/temporal/__init__.py` | Create | Package marker |
| `src/qc/temporal/compare.py` | Create | D/W/M comparison + drift flag emission |
| `src/qc/report/__init__.py` | Create | Package marker |
| `src/qc/report/writer.py` | Create | Write issues.parquet, delta_report.json, run_summary.json |
| `src/qc/alert/__init__.py` | Create | Package marker |
| `src/qc/alert/notify.py` | Create | Stubbed CloudWatch + SNS alerting |
| `src/qc/main.py` | Create | Orchestrator: clients → domains → read→check→compare→write→alert |
| `src/qc/dashboard/__init__.py` | Create | Package marker |
| `src/qc/dashboard/loader.py` | Create | Read /data/output Parquet+JSON into DataFrames |
| `src/qc/dashboard/app.py` | Create | Flask routes — summary and issues pages |
| `src/qc/dashboard/templates/base.html` | Create | Base HTML template |
| `src/qc/dashboard/templates/summary.html` | Create | Landing page — per client+domain run status |
| `src/qc/dashboard/templates/issues.html` | Create | Drilldown — filterable issues + delta panel |
| `scripts/poll_and_run.sh` | Create | S3 poll loop + sentinel guard + main.py trigger |
| `scripts/run_dashboard.sh` | Create | Start gunicorn dashboard server |
| `scripts/bootstrap_ec2.sh` | Create | Provision Python, venv, deps, CloudWatch agent |
| `deploy/qc.service` | Create | systemd unit for QC pipeline |
| `deploy/qc.timer` | Create | systemd timer — fires poll_and_run.sh every 15 min |
| `deploy/dashboard.service` | Create | systemd unit for Flask dashboard |
| `deploy/cloudwatch-agent.json` | Create | CloudWatch log + metric shipping config |
| `tests/conftest.py` | Create | Shared fixtures: DataFrames, seeded DuckDB, temp dirs |
| `tests/test_checks_missing.py` | Create | Unit tests for missing.py |
| `tests/test_checks_types.py` | Create | Unit tests for types.py |
| `tests/test_checks_ranges.py` | Create | Unit tests for ranges.py |
| `tests/test_checks_dedup.py` | Create | Unit tests for dedup.py |
| `tests/test_checks_timestamp.py` | Create | Unit tests for timestamp.py |
| `tests/test_checks_referential.py` | Create | Unit tests for referential.py |
| `tests/test_checks_logic.py` | Create | Unit tests for logic.py — LWBS, discharge_before_admit, config-driven |
| `tests/test_registry.py` | Create | Unit tests for plugin registry |
| `tests/test_metrics_store.py` | Create | Unit tests for DuckDB store |
| `tests/test_temporal_compare.py` | Create | Unit tests for D/W/M comparison |
| `tests/test_report_writer.py` | Create | Unit tests for output artifact structure |
| `tests/test_integration.py` | Create | End-to-end smoke test |

---

## Task 1: Project Scaffold & Config

**Files:**
- Create: `pyproject.toml`
- Create: `config/config.yaml`
- Create: `config/suites/ohiohealth/ed.yaml`
- Create: `config/suites/ohiohealth/inpatient.yaml`
- Create: `src/qc/__init__.py` and all sub-package `__init__.py` files
- Create: `.gitignore`

**Interfaces:**
- Produces: `config/config.yaml` loaded as a dict with keys `source.bucket`, `source.prefix`, `output.path`, `history.path`, `poll_interval_minutes`, `thresholds.row_count_max_pct_change`, `thresholds.null_rate_max_abs_change`

- [ ] **Step 1: Create pyproject.toml**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.backends.legacy:build"

[project]
name = "healthcare-qc"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "pandas>=2.2",
    "pyarrow>=15",
    "duckdb>=0.10",
    "boto3>=1.34",
    "pyyaml>=6.0",
    "flask>=3.0",
    "gunicorn>=21",
]

[project.optional-dependencies]
dev = ["pytest>=8", "pytest-cov", "moto[s3]>=5"]

[tool.setuptools.packages.find]
where = ["src"]
```

- [ ] **Step 2: Create config/config.yaml**

```yaml
source:
  bucket: "your-source-bucket"
  prefix: "landing"

output:
  path: "/data/output"

history:
  path: "/data/history"

poll_interval_minutes: 15

thresholds:
  row_count_max_pct_change: 0.30
  null_rate_max_abs_change: 0.05
  mean_max_pct_change: 0.20
```

- [ ] **Step 3: Create config/suites/ohiohealth/ed.yaml**

```yaml
client: ohiohealth
domain: ed
checks:
  - missing:
      columns: [patient_id, admit_ts, disposition]
      max_null_rate: 0.0
  - ranges:
      age: {min: 0, max: 120}
      los_hours: {min: 0, max: 720}
  - types:
      patient_id: str
      admit_ts: datetime
      age: numeric
  - dedup:
      key_columns: [patient_id, admit_ts]
  - timestamp:
      columns: [admit_ts, discharge_ts]
      order: {before: admit_ts, after: discharge_ts}
  - logic:
      lwbs:
        description: "Left Without Being Seen rate"
        condition: "disposition == 'LWBS'"
        max_rate: 0.05
        severity: warn
      discharge_before_admit:
        condition: "discharge_ts < admit_ts"
        severity: error
  - delta:
      row_count: {max_pct_change: 0.30}
      null_rate: {max_abs_change: 0.05}
```

- [ ] **Step 4: Create config/suites/ohiohealth/inpatient.yaml**

```yaml
client: ohiohealth
domain: inpatient
checks:
  - missing:
      columns: [patient_id, admit_ts, discharge_ts, drg_code]
      max_null_rate: 0.0
  - ranges:
      age: {min: 0, max: 120}
  - types:
      patient_id: str
      admit_ts: datetime
      discharge_ts: datetime
  - dedup:
      key_columns: [patient_id, admit_ts]
  - timestamp:
      columns: [admit_ts, discharge_ts]
      order: {before: admit_ts, after: discharge_ts}
  - logic:
      discharge_before_admit:
        condition: "discharge_ts < admit_ts"
        severity: error
  - custom:
      - rule: los_plausibility
        max_los_days: 120
        severity: warn
  - delta:
      row_count: {max_pct_change: 0.30}
      null_rate: {max_abs_change: 0.05}
```

- [ ] **Step 5: Create all __init__.py package markers**

Create empty files at:
- `src/qc/__init__.py`
- `src/qc/ingest/__init__.py`
- `src/qc/checks/__init__.py`
- `src/qc/metrics/__init__.py`
- `src/qc/temporal/__init__.py`
- `src/qc/report/__init__.py`
- `src/qc/alert/__init__.py`
- `src/qc/dashboard/__init__.py`

- [ ] **Step 6: Create .gitignore**

```
data/
*.pyc
__pycache__/
.venv/
*.egg-info/
dist/
.pytest_cache/
*.duckdb
```

- [ ] **Step 7: Install dependencies**

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Expected: no errors, `pytest --version` returns 8.x

- [ ] **Step 8: Commit**

```bash
git init
git add pyproject.toml config/ src/ .gitignore
git commit -m "feat: project scaffold, config, and suite definitions"
```

---

## Task 2: Check Registry & `flags_from` Helper

**Files:**
- Create: `src/qc/checks/registry.py`
- Create: `tests/test_registry.py`

**Interfaces:**
- Produces:
  - `register(rule_name: str, domains: list[str] | None = None)` — decorator that registers a check function
  - `get_checks(domain: str) -> dict[str, callable]` — returns registered checks applicable to a domain
  - `flags_from(df: pd.DataFrame, rule: str, severity: str, column: str | None = None, detail: str | None = None) -> list[dict]` — builds flag dicts from a filtered DataFrame of bad rows

- [ ] **Step 1: Write failing tests**

```python
# tests/test_registry.py
import pandas as pd
from qc.checks.registry import register, get_checks, flags_from

def test_register_and_get_all_domains():
    @register("test_rule_all")
    def check(df, cfg):
        return []
    checks = get_checks("any_domain")
    assert "test_rule_all" in checks

def test_register_domain_scoped():
    @register("test_rule_ed", domains=["ed"])
    def check(df, cfg):
        return []
    assert "test_rule_ed" in get_checks("ed")
    assert "test_rule_ed" not in get_checks("inpatient")

def test_flags_from_returns_correct_schema():
    df = pd.DataFrame({"patient_id": ["A", "B"], "age": [200, 300]})
    bad = df[df["age"] > 120]
    flags = flags_from(bad, rule="range", severity="error", column="age",
                       detail="value exceeds max=120",
                       run_date="2026-07-17", client="ohiohealth", domain="ed")
    assert len(flags) == 2
    assert flags[0]["rule"] == "range"
    assert flags[0]["severity"] == "error"
    assert flags[0]["column"] == "age"
    assert flags[0]["client"] == "ohiohealth"
    assert "row_id" in flags[0]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_registry.py -v
```

Expected: `ImportError` or `ModuleNotFoundError` — registry doesn't exist yet

- [ ] **Step 3: Implement registry.py**

```python
# src/qc/checks/registry.py
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_registry.py -v
```

Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/qc/checks/registry.py tests/test_registry.py
git commit -m "feat: check plugin registry and flags_from helper"
```

---

## Task 3: Built-in Check Modules

**Files:**
- Create: `src/qc/checks/missing.py`
- Create: `src/qc/checks/types.py`
- Create: `src/qc/checks/ranges.py`
- Create: `src/qc/checks/dedup.py`
- Create: `src/qc/checks/timestamp.py`
- Create: `src/qc/checks/referential.py`
- Create: `src/qc/checks/logic.py`
- Create: `tests/conftest.py`
- Create: `tests/test_checks_missing.py`
- Create: `tests/test_checks_types.py`
- Create: `tests/test_checks_ranges.py`
- Create: `tests/test_checks_dedup.py`
- Create: `tests/test_checks_timestamp.py`
- Create: `tests/test_checks_referential.py`
- Create: `tests/test_checks_logic.py`

**Interfaces:**
- Consumes: `flags_from` from `qc.checks.registry`
- Each check function signature: `check(df, cfg, *, run_date, client, domain) -> list[dict]`
- Produces: `run_check(name, df, cfg, *, run_date, client, domain) -> list[dict]` — dispatcher used by `main.py`

- [ ] **Step 1: Create tests/conftest.py with shared fixtures**

```python
# tests/conftest.py
import os
import tempfile
import pandas as pd
import duckdb
import pytest

@pytest.fixture
def ed_df():
    return pd.DataFrame({
        "patient_id": ["P001", "P002", "P003", "P001"],
        "admit_ts": pd.to_datetime(["2026-07-17 08:00", "2026-07-17 09:00",
                                     "2026-07-17 10:00", "2026-07-17 11:00"]),
        "discharge_ts": pd.to_datetime(["2026-07-17 10:00", "2026-07-16 08:00",
                                         "2026-07-17 12:00", "2026-07-17 13:00"]),
        "age": [45, 200, 32, 28],
        "disposition": ["DISCHARGE", "LWBS", "LWBS", "TRANSFER"],
    })

@pytest.fixture
def run_ctx():
    return {"run_date": "2026-07-17", "client": "ohiohealth", "domain": "ed"}

@pytest.fixture
def tmp_duckdb(tmp_path):
    db_path = str(tmp_path / "metrics.duckdb")
    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE metrics (
            client VARCHAR, domain VARCHAR, run_date DATE,
            row_count INTEGER, dup_rate DOUBLE,
            metrics_json VARCHAR
        )
    """)
    con.close()
    return db_path

@pytest.fixture
def tmp_output(tmp_path):
    (tmp_path / "qc_issues").mkdir()
    (tmp_path / "qc_delta").mkdir()
    (tmp_path / "run_summary").mkdir()
    return str(tmp_path)
```

- [ ] **Step 2: Write failing tests for missing.py**

```python
# tests/test_checks_missing.py
import pandas as pd
from qc.checks.missing import check

def test_flags_absent_required_column(run_ctx):
    df = pd.DataFrame({"patient_id": ["P1"], "age": [30]})
    cfg = {"columns": ["patient_id", "admit_ts"], "max_null_rate": 0.0}
    flags = check(df, cfg, **run_ctx)
    rules = [f["rule"] for f in flags]
    assert "missing_column" in rules
    columns_flagged = [f["column"] for f in flags]
    assert "admit_ts" in columns_flagged

def test_flags_null_values_above_rate(run_ctx):
    df = pd.DataFrame({"patient_id": [None, "P2", None, "P4"]})
    cfg = {"columns": ["patient_id"], "max_null_rate": 0.0}
    flags = check(df, cfg, **run_ctx)
    assert any(f["rule"] == "null_value" for f in flags)

def test_no_flags_when_clean(run_ctx):
    df = pd.DataFrame({"patient_id": ["P1", "P2"], "admit_ts": pd.to_datetime(["2026-07-17", "2026-07-17"])})
    cfg = {"columns": ["patient_id", "admit_ts"], "max_null_rate": 0.0}
    flags = check(df, cfg, **run_ctx)
    assert flags == []
```

- [ ] **Step 3: Implement missing.py**

```python
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
```

- [ ] **Step 4: Run missing tests**

```bash
pytest tests/test_checks_missing.py -v
```

Expected: 3 passed

- [ ] **Step 5: Write failing tests for types.py**

```python
# tests/test_checks_types.py
import pandas as pd
from qc.checks.types import check

def test_flags_non_numeric(run_ctx):
    df = pd.DataFrame({"age": ["old", "young", "45"]})
    cfg = {"age": "numeric"}
    flags = check(df, cfg, **run_ctx)
    assert any(f["rule"] == "type_mismatch" and f["column"] == "age" for f in flags)

def test_flags_non_datetime(run_ctx):
    df = pd.DataFrame({"admit_ts": ["not-a-date", "2026-07-17 08:00"]})
    cfg = {"admit_ts": "datetime"}
    flags = check(df, cfg, **run_ctx)
    assert any(f["rule"] == "type_mismatch" and f["column"] == "admit_ts" for f in flags)

def test_no_flags_correct_types(run_ctx):
    df = pd.DataFrame({"age": [45, 30], "admit_ts": pd.to_datetime(["2026-07-17", "2026-07-17"])})
    cfg = {"age": "numeric", "admit_ts": "datetime"}
    flags = check(df, cfg, **run_ctx)
    assert flags == []
```

- [ ] **Step 6: Implement types.py**

```python
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
```

- [ ] **Step 7: Run types tests**

```bash
pytest tests/test_checks_types.py -v
```

Expected: 3 passed

- [ ] **Step 8: Write failing tests for ranges.py**

```python
# tests/test_checks_ranges.py
import pandas as pd
from qc.checks.ranges import check

def test_flags_out_of_numeric_range(run_ctx):
    df = pd.DataFrame({"age": [45, 200, -1, 30]})
    cfg = {"age": {"min": 0, "max": 120}}
    flags = check(df, cfg, **run_ctx)
    assert len([f for f in flags if f["column"] == "age"]) == 2

def test_flags_invalid_category(run_ctx):
    df = pd.DataFrame({"disposition": ["DISCHARGE", "UNKNOWN", "LWBS"]})
    cfg = {"disposition": {"allowed": ["DISCHARGE", "LWBS", "TRANSFER", "AMA"]}}
    flags = check(df, cfg, **run_ctx)
    assert len([f for f in flags if f["column"] == "disposition"]) == 1

def test_no_flags_in_range(run_ctx):
    df = pd.DataFrame({"age": [0, 60, 120]})
    cfg = {"age": {"min": 0, "max": 120}}
    flags = check(df, cfg, **run_ctx)
    assert flags == []
```

- [ ] **Step 9: Implement ranges.py**

```python
# src/qc/checks/ranges.py
import pandas as pd
from qc.checks.registry import flags_from


def check(df: pd.DataFrame, cfg: dict, *, run_date: str, client: str, domain: str) -> list[dict]:
    flags = []
    ctx = dict(run_date=run_date, client=client, domain=domain)
    for col, rule in cfg.items():
        if col not in df.columns:
            continue
        if "min" in rule or "max" in rule:
            numeric = pd.to_numeric(df[col], errors="coerce")
            bad = pd.Series([False] * len(df), index=df.index)
            if "min" in rule:
                bad |= numeric < rule["min"]
            if "max" in rule:
                bad |= numeric > rule["max"]
            if bad.any():
                flags.extend(flags_from(
                    df[bad], rule="range", severity="error", column=col,
                    detail=f"min={rule.get('min')} max={rule.get('max')}", **ctx,
                ))
        if "allowed" in rule:
            bad = ~df[col].isin(rule["allowed"]) & df[col].notna()
            if bad.any():
                flags.extend(flags_from(
                    df[bad], rule="invalid_category", severity="error", column=col,
                    detail=f"allowed={rule['allowed']}", **ctx,
                ))
    return flags
```

- [ ] **Step 10: Run ranges tests**

```bash
pytest tests/test_checks_ranges.py -v
```

Expected: 3 passed

- [ ] **Step 11: Write failing tests for dedup.py**

```python
# tests/test_checks_dedup.py
import pandas as pd
from qc.checks.dedup import check

def test_flags_duplicate_keys(ed_df, run_ctx):
    cfg = {"key_columns": ["patient_id", "admit_ts"]}
    # ed_df has P001 duplicated
    flags = check(ed_df, cfg, **run_ctx)
    assert any(f["rule"] == "duplicate_key" for f in flags)

def test_no_flags_unique_keys(run_ctx):
    df = pd.DataFrame({"patient_id": ["P1", "P2"], "admit_ts": pd.to_datetime(["2026-07-17 08:00", "2026-07-17 09:00"])})
    cfg = {"key_columns": ["patient_id", "admit_ts"]}
    flags = check(df, cfg, **run_ctx)
    assert flags == []
```

- [ ] **Step 12: Implement dedup.py**

```python
# src/qc/checks/dedup.py
import pandas as pd
from qc.checks.registry import flags_from


def check(df: pd.DataFrame, cfg: dict, *, run_date: str, client: str, domain: str) -> list[dict]:
    key_cols = cfg.get("key_columns", [])
    present = [c for c in key_cols if c in df.columns]
    if not present:
        return []
    dup_mask = df.duplicated(subset=present, keep=False)
    if not dup_mask.any():
        return []
    return flags_from(
        df[dup_mask], rule="duplicate_key", severity="error",
        column=",".join(present), detail=f"duplicate on {present}",
        run_date=run_date, client=client, domain=domain,
    )
```

- [ ] **Step 13: Run dedup tests**

```bash
pytest tests/test_checks_dedup.py -v
```

Expected: 2 passed

- [ ] **Step 14: Write failing tests for timestamp.py**

```python
# tests/test_checks_timestamp.py
import pandas as pd
from qc.checks.timestamp import check

def test_flags_discharge_before_admit(ed_df, run_ctx):
    cfg = {"columns": ["admit_ts", "discharge_ts"],
           "order": {"before": "admit_ts", "after": "discharge_ts"}}
    flags = check(ed_df, cfg, **run_ctx)
    assert any(f["rule"] == "timestamp_order" for f in flags)

def test_flags_unparseable_timestamp(run_ctx):
    df = pd.DataFrame({"admit_ts": ["not-a-date", "2026-07-17 08:00"]})
    cfg = {"columns": ["admit_ts"]}
    flags = check(df, cfg, **run_ctx)
    assert any(f["rule"] == "invalid_timestamp" for f in flags)

def test_no_flags_valid_timestamps(run_ctx):
    df = pd.DataFrame({
        "admit_ts": pd.to_datetime(["2026-07-17 08:00", "2026-07-17 09:00"]),
        "discharge_ts": pd.to_datetime(["2026-07-17 10:00", "2026-07-17 11:00"]),
    })
    cfg = {"columns": ["admit_ts", "discharge_ts"],
           "order": {"before": "admit_ts", "after": "discharge_ts"}}
    flags = check(df, cfg, **run_ctx)
    assert flags == []
```

- [ ] **Step 15: Implement timestamp.py**

```python
# src/qc/checks/timestamp.py
import pandas as pd
from qc.checks.registry import flags_from


def check(df: pd.DataFrame, cfg: dict, *, run_date: str, client: str, domain: str) -> list[dict]:
    flags = []
    ctx = dict(run_date=run_date, client=client, domain=domain)
    for col in cfg.get("columns", []):
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce")
        bad = parsed.isna() & df[col].notna()
        if bad.any():
            flags.extend(flags_from(
                df[bad], rule="invalid_timestamp", severity="error",
                column=col, detail="unparseable timestamp", **ctx,
            ))
    order = cfg.get("order", {})
    before_col = order.get("before")
    after_col = order.get("after")
    if before_col and after_col and before_col in df.columns and after_col in df.columns:
        before_ts = pd.to_datetime(df[before_col], errors="coerce")
        after_ts = pd.to_datetime(df[after_col], errors="coerce")
        bad = (after_ts < before_ts) & before_ts.notna() & after_ts.notna()
        if bad.any():
            flags.extend(flags_from(
                df[bad], rule="timestamp_order", severity="error",
                column=f"{after_col}<{before_col}",
                detail=f"{after_col} precedes {before_col}", **ctx,
            ))
    return flags
```

- [ ] **Step 16: Run timestamp tests**

```bash
pytest tests/test_checks_timestamp.py -v
```

Expected: 3 passed

- [ ] **Step 17: Write failing tests for referential.py**

```python
# tests/test_checks_referential.py
import pandas as pd
from qc.checks.referential import check

def test_flags_unresolved_fk(run_ctx):
    df = pd.DataFrame({"drg_code": ["123", "999", "456"]})
    cfg = {"drg_code": {"reference": ["123", "456", "789"]}}
    flags = check(df, cfg, **run_ctx)
    assert any(f["rule"] == "referential_integrity" and f["column"] == "drg_code" for f in flags)
    assert len([f for f in flags if f["column"] == "drg_code"]) == 1

def test_no_flags_all_fks_resolve(run_ctx):
    df = pd.DataFrame({"drg_code": ["123", "456"]})
    cfg = {"drg_code": {"reference": ["123", "456", "789"]}}
    flags = check(df, cfg, **run_ctx)
    assert flags == []
```

- [ ] **Step 18: Implement referential.py**

```python
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
```

- [ ] **Step 19: Run referential tests**

```bash
pytest tests/test_checks_referential.py -v
```

Expected: 2 passed

- [ ] **Step 20: Write failing tests for logic.py**

```python
# tests/test_checks_logic.py
import pandas as pd
from qc.checks.logic import check

def test_flags_lwbs_rate_exceeded(ed_df, run_ctx):
    # ed_df has 2 LWBS out of 4 rows = 50% > max_rate 5%
    cfg = {"lwbs": {"condition": "disposition == 'LWBS'", "max_rate": 0.05, "severity": "warn"}}
    flags = check(ed_df, cfg, **run_ctx)
    assert any(f["rule"] == "lwbs" for f in flags)

def test_no_flags_lwbs_rate_within_threshold(run_ctx):
    df = pd.DataFrame({"disposition": ["DISCHARGE"] * 99 + ["LWBS"]})
    cfg = {"lwbs": {"condition": "disposition == 'LWBS'", "max_rate": 0.05, "severity": "warn"}}
    flags = check(df, cfg, **run_ctx)
    assert flags == []

def test_flags_discharge_before_admit_condition(run_ctx):
    df = pd.DataFrame({
        "admit_ts": pd.to_datetime(["2026-07-17 10:00", "2026-07-17 08:00"]),
        "discharge_ts": pd.to_datetime(["2026-07-17 08:00", "2026-07-17 10:00"]),
    })
    cfg = {"discharge_before_admit": {"condition": "discharge_ts < admit_ts", "severity": "error"}}
    flags = check(df, cfg, **run_ctx)
    assert any(f["rule"] == "discharge_before_admit" for f in flags)
    assert len([f for f in flags if f["rule"] == "discharge_before_admit"]) == 1
```

- [ ] **Step 21: Implement logic.py**

```python
# src/qc/checks/logic.py
import pandas as pd
from qc.checks.registry import flags_from


def check(df: pd.DataFrame, cfg: dict, *, run_date: str, client: str, domain: str) -> list[dict]:
    flags = []
    ctx = dict(run_date=run_date, client=client, domain=domain)
    for rule_name, rule_cfg in cfg.items():
        condition = rule_cfg.get("condition")
        severity = rule_cfg.get("severity", "error")
        max_rate = rule_cfg.get("max_rate")
        if not condition:
            continue
        try:
            matched = df.query(condition)
        except Exception as exc:
            flags.append({
                "run_date": run_date, "client": client, "domain": domain,
                "row_id": None, "column": None, "rule": rule_name,
                "severity": "error", "detail": f"condition eval error: {exc}",
            })
            continue
        if max_rate is not None:
            rate = len(matched) / len(df) if len(df) > 0 else 0
            if rate > max_rate:
                flags.append({
                    "run_date": run_date, "client": client, "domain": domain,
                    "row_id": None, "column": None, "rule": rule_name,
                    "severity": severity,
                    "detail": f"rate={rate:.3f} exceeds max_rate={max_rate}",
                })
        else:
            if not matched.empty:
                flags.extend(flags_from(
                    matched, rule=rule_name, severity=severity,
                    column=None, detail=condition, **ctx,
                ))
    return flags
```

- [ ] **Step 22: Run logic tests**

```bash
pytest tests/test_checks_logic.py -v
```

Expected: 3 passed

- [ ] **Step 23: Run all check tests**

```bash
pytest tests/test_checks_*.py tests/test_registry.py -v
```

Expected: all pass

- [ ] **Step 24: Commit**

```bash
git add src/qc/checks/ tests/conftest.py tests/test_checks_*.py tests/test_registry.py
git commit -m "feat: built-in check modules with full test coverage"
```

---

## Task 4: Metrics Store (DuckDB)

**Files:**
- Create: `src/qc/metrics/store.py`
- Create: `tests/test_metrics_store.py`

**Interfaces:**
- Consumes: nothing from prior tasks
- Produces:
  - `append_snapshot(db_path: str, client: str, domain: str, run_date: str, snapshot: dict) -> None`
  - `get_snapshot(db_path: str, client: str, domain: str, run_date: str) -> dict | None`
  - `get_prior_snapshots(db_path: str, client: str, domain: str, run_date: str) -> dict[str, dict | None]` — returns `{"day": ..., "week": ..., "month": ...}`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_metrics_store.py
import json
from qc.metrics.store import append_snapshot, get_snapshot, get_prior_snapshots

SNAPSHOT = {"row_count": 1000, "dup_rate": 0.01, "null_rate_admit_ts": 0.02, "mean_age": 45.3}

def test_append_and_retrieve(tmp_duckdb):
    append_snapshot(tmp_duckdb, "ohiohealth", "ed", "2026-07-17", SNAPSHOT)
    result = get_snapshot(tmp_duckdb, "ohiohealth", "ed", "2026-07-17")
    assert result["row_count"] == 1000
    assert result["null_rate_admit_ts"] == 0.02

def test_get_snapshot_missing_returns_none(tmp_duckdb):
    result = get_snapshot(tmp_duckdb, "ohiohealth", "ed", "2025-01-01")
    assert result is None

def test_get_prior_snapshots(tmp_duckdb):
    append_snapshot(tmp_duckdb, "ohiohealth", "ed", "2026-07-10", {"row_count": 900})
    append_snapshot(tmp_duckdb, "ohiohealth", "ed", "2026-07-16", {"row_count": 950})
    append_snapshot(tmp_duckdb, "ohiohealth", "ed", "2026-06-17", {"row_count": 800})
    priors = get_prior_snapshots(tmp_duckdb, "ohiohealth", "ed", "2026-07-17")
    assert priors["day"]["row_count"] == 950
    assert priors["week"]["row_count"] == 900
    assert priors["month"]["row_count"] == 800

def test_client_isolation(tmp_duckdb):
    append_snapshot(tmp_duckdb, "ohiohealth", "ed", "2026-07-17", {"row_count": 1000})
    append_snapshot(tmp_duckdb, "ssmhealth", "ed", "2026-07-17", {"row_count": 2000})
    ohio = get_snapshot(tmp_duckdb, "ohiohealth", "ed", "2026-07-17")
    ssm = get_snapshot(tmp_duckdb, "ssmhealth", "ed", "2026-07-17")
    assert ohio["row_count"] == 1000
    assert ssm["row_count"] == 2000
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_metrics_store.py -v
```

Expected: `ImportError`

- [ ] **Step 3: Implement store.py**

```python
# src/qc/metrics/store.py
import json
from datetime import date, timedelta
import duckdb


def _ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            client VARCHAR NOT NULL,
            domain VARCHAR NOT NULL,
            run_date DATE NOT NULL,
            row_count INTEGER,
            dup_rate DOUBLE,
            metrics_json VARCHAR,
            PRIMARY KEY (client, domain, run_date)
        )
    """)


def append_snapshot(db_path: str, client: str, domain: str, run_date: str, snapshot: dict) -> None:
    con = duckdb.connect(db_path)
    _ensure_table(con)
    con.execute(
        "INSERT OR REPLACE INTO metrics VALUES (?, ?, ?, ?, ?, ?)",
        [client, domain, run_date,
         snapshot.get("row_count"), snapshot.get("dup_rate"),
         json.dumps(snapshot)],
    )
    con.close()


def get_snapshot(db_path: str, client: str, domain: str, run_date: str) -> dict | None:
    con = duckdb.connect(db_path, read_only=True)
    _ensure_table(con)
    row = con.execute(
        "SELECT metrics_json FROM metrics WHERE client=? AND domain=? AND run_date=?",
        [client, domain, run_date],
    ).fetchone()
    con.close()
    return json.loads(row[0]) if row else None


def get_prior_snapshots(db_path: str, client: str, domain: str, run_date: str) -> dict[str, dict | None]:
    d = date.fromisoformat(run_date)
    return {
        "day":   get_snapshot(db_path, client, domain, str(d - timedelta(days=1))),
        "week":  get_snapshot(db_path, client, domain, str(d - timedelta(days=7))),
        "month": get_snapshot(db_path, client, domain, str(d - timedelta(days=30))),
    }
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_metrics_store.py -v
```

Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/qc/metrics/store.py tests/test_metrics_store.py
git commit -m "feat: DuckDB metrics store with client-scoped D/W/M snapshots"
```

---

## Task 5: Temporal Comparison

**Files:**
- Create: `src/qc/temporal/compare.py`
- Create: `tests/test_temporal_compare.py`

**Interfaces:**
- Consumes: `get_prior_snapshots` from `qc.metrics.store`
- Produces:
  - `compute_snapshot(df: pd.DataFrame) -> dict` — computes metric snapshot from a DataFrame
  - `compare(today: dict, priors: dict[str, dict | None], thresholds: dict, *, run_date: str, client: str, domain: str) -> list[dict]` — returns variation flags

- [ ] **Step 1: Write failing tests**

```python
# tests/test_temporal_compare.py
import pandas as pd
from qc.temporal.compare import compute_snapshot, compare

def test_compute_snapshot_row_count():
    df = pd.DataFrame({"age": [30, 45, 60], "admit_ts": pd.to_datetime(["2026-07-17"] * 3)})
    snap = compute_snapshot(df)
    assert snap["row_count"] == 3

def test_compute_snapshot_null_rate():
    df = pd.DataFrame({"age": [30, None, 60]})
    snap = compute_snapshot(df)
    assert abs(snap["null_rate_age"] - 0.333) < 0.01

def test_compare_flags_row_count_drop(run_ctx):
    today = {"row_count": 500}
    priors = {"day": {"row_count": 1000}, "week": None, "month": None}
    thresholds = {"row_count_max_pct_change": 0.30}
    flags = compare(today, priors, thresholds, **run_ctx)
    assert any(f["rule"] == "delta_row_count" and f["severity"] == "error" for f in flags)

def test_compare_no_flags_within_threshold(run_ctx):
    today = {"row_count": 1000}
    priors = {"day": {"row_count": 1050}, "week": None, "month": None}
    thresholds = {"row_count_max_pct_change": 0.30}
    flags = compare(today, priors, thresholds, **run_ctx)
    assert flags == []

def test_compare_no_flags_no_prior(run_ctx):
    today = {"row_count": 1000}
    priors = {"day": None, "week": None, "month": None}
    thresholds = {"row_count_max_pct_change": 0.30}
    flags = compare(today, priors, thresholds, **run_ctx)
    assert flags == []
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_temporal_compare.py -v
```

Expected: `ImportError`

- [ ] **Step 3: Implement compare.py**

```python
# src/qc/temporal/compare.py
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
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_temporal_compare.py -v
```

Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add src/qc/temporal/compare.py tests/test_temporal_compare.py
git commit -m "feat: temporal comparison — D/W/M drift detection"
```

---

## Task 6: S3 Reader (Ingest)

**Files:**
- Create: `src/qc/ingest/reader.py`

**Interfaces:**
- Consumes: `boto3` S3 client
- Produces:
  - `discover_clients_domains(bucket: str, prefix: str, run_date: str) -> list[tuple[str, str]]` — returns `[("ohiohealth", "ed"), ("ssmhealth", "inpatient"), ...]`
  - `load_domain(bucket: str, prefix: str, client: str, domain: str, run_date: str) -> pd.DataFrame` — loads all CSV/Parquet files for that client+domain+date into a single DataFrame

Note: `reader.py` is tested via the integration test (Task 10) using `moto` S3 mocking. No unit test file for this module.

- [ ] **Step 1: Implement reader.py**

```python
# src/qc/ingest/reader.py
import io
import logging
import boto3
import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def _s3_client():
    return boto3.client("s3")


def discover_clients_domains(bucket: str, prefix: str, run_date: str) -> list[tuple[str, str]]:
    s3 = _s3_client()
    search_prefix = f"{prefix}/date={run_date}/"
    paginator = s3.get_paginator("list_objects_v2")
    results = []
    seen = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            part = cp["Prefix"].rstrip("/").split("/")[-1]
            if part.startswith("client="):
                client_name = part.split("=", 1)[1]
                domain_prefix = f"{search_prefix}{part}/"
                for dpage in paginator.paginate(Bucket=bucket, Prefix=domain_prefix, Delimiter="/"):
                    for dp in dpage.get("CommonPrefixes", []):
                        dpart = dp["Prefix"].rstrip("/").split("/")[-1]
                        if dpart.startswith("domain="):
                            domain_name = dpart.split("=", 1)[1]
                            key = (client_name, domain_name)
                            if key not in seen:
                                seen.add(key)
                                results.append(key)
    logger.info("discovered %d client+domain pairs for date=%s", len(results), run_date)
    return results


def load_domain(bucket: str, prefix: str, client: str, domain: str, run_date: str) -> pd.DataFrame:
    s3 = _s3_client()
    key_prefix = f"{prefix}/date={run_date}/client={client}/domain={domain}/"
    paginator = s3.get_paginator("list_objects_v2")
    frames = []
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            if key.endswith(".parquet"):
                frames.append(pq.read_table(io.BytesIO(body)).to_pandas())
            elif key.endswith(".csv"):
                frames.append(pd.read_csv(io.BytesIO(body)))
            else:
                logger.warning("skipping unsupported file type: %s", key)
    if not frames:
        raise FileNotFoundError(f"no data files at s3://{bucket}/{key_prefix}")
    df = pd.concat(frames, ignore_index=True)
    logger.info("loaded client=%s domain=%s rows=%d", client, domain, len(df))
    return df
```

- [ ] **Step 2: Commit**

```bash
git add src/qc/ingest/reader.py
git commit -m "feat: S3 reader — dynamic client+domain discovery and read-only data loading"
```

---

## Task 7: Report Writer

**Files:**
- Create: `src/qc/report/writer.py`
- Create: `tests/test_report_writer.py`

**Interfaces:**
- Consumes: flag lists from checks, delta flags from compare, snapshot dict from store, config dict
- Produces:
  - `write_artifacts(output_path: str, client: str, domain: str, run_date: str, issues: list[dict], delta_flags: list[dict], domain_summary: dict) -> None`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_report_writer.py
import json
import os
import pandas as pd
from qc.report.writer import write_artifacts

ISSUES = [
    {"run_date": "2026-07-17", "client": "ohiohealth", "domain": "ed",
     "row_id": 1, "column": "age", "rule": "range", "severity": "error",
     "detail": "value=200 max=120"},
]
DELTA_FLAGS = [
    {"run_date": "2026-07-17", "client": "ohiohealth", "domain": "ed",
     "row_id": None, "column": "row_count", "rule": "delta_row_count",
     "severity": "error", "detail": "day_over_day pct_change=51%"},
]
DOMAIN_SUMMARY = {
    "status": "passed",
    "rows_read": 100,
    "checks_run": ["missing", "ranges"],
    "flags_raised": 1,
    "by_check": {"missing": {"flags": 0, "columns": []}, "ranges": {"flags": 1, "columns": ["age"]}},
}

def test_issues_parquet_written(tmp_output):
    write_artifacts(tmp_output, "ohiohealth", "ed", "2026-07-17", ISSUES, [], DOMAIN_SUMMARY)
    path = os.path.join(tmp_output, "qc_issues", "client=ohiohealth", "domain=ed", "date=2026-07-17", "issues.parquet")
    assert os.path.exists(path)
    df = pd.read_parquet(path)
    assert len(df) == 1
    assert df.iloc[0]["rule"] == "range"

def test_delta_report_written(tmp_output):
    write_artifacts(tmp_output, "ohiohealth", "ed", "2026-07-17", [], DELTA_FLAGS, DOMAIN_SUMMARY)
    path = os.path.join(tmp_output, "qc_delta", "client=ohiohealth", "domain=ed", "date=2026-07-17", "delta_report.json")
    assert os.path.exists(path)
    report = json.loads(open(path).read())
    assert report["client"] == "ohiohealth"
    assert len(report["flags"]) == 1

def test_run_summary_written(tmp_output):
    write_artifacts(tmp_output, "ohiohealth", "ed", "2026-07-17", ISSUES, DELTA_FLAGS, DOMAIN_SUMMARY)
    path = os.path.join(tmp_output, "run_summary", "client=ohiohealth", "date=2026-07-17", "summary.json")
    assert os.path.exists(path)
    summary = json.loads(open(path).read())
    assert summary["client"] == "ohiohealth"
    assert summary["domains"]["ed"]["flags_raised"] == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_report_writer.py -v
```

Expected: `ImportError`

- [ ] **Step 3: Implement writer.py**

```python
# src/qc/report/writer.py
import json
import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def write_artifacts(
    output_path: str,
    client: str,
    domain: str,
    run_date: str,
    issues: list[dict],
    delta_flags: list[dict],
    domain_summary: dict,
) -> None:
    _write_issues(output_path, client, domain, run_date, issues)
    _write_delta(output_path, client, domain, run_date, delta_flags)
    _write_summary(output_path, client, domain, run_date, domain_summary)


def _write_issues(output_path, client, domain, run_date, issues):
    dir_path = os.path.join(output_path, "qc_issues", f"client={client}", f"domain={domain}", f"date={run_date}")
    os.makedirs(dir_path, exist_ok=True)
    df = pd.DataFrame(issues) if issues else pd.DataFrame(
        columns=["run_date", "client", "domain", "row_id", "column", "rule", "severity", "detail"]
    )
    df.to_parquet(os.path.join(dir_path, "issues.parquet"), index=False)
    logger.info("wrote %d issues for client=%s domain=%s", len(issues), client, domain)


def _write_delta(output_path, client, domain, run_date, delta_flags):
    dir_path = os.path.join(output_path, "qc_delta", f"client={client}", f"domain={domain}", f"date={run_date}")
    os.makedirs(dir_path, exist_ok=True)
    report = {"client": client, "domain": domain, "run_date": run_date, "flags": delta_flags}
    with open(os.path.join(dir_path, "delta_report.json"), "w") as f:
        json.dump(report, f, indent=2)


def _write_summary(output_path, client, domain, run_date, domain_summary):
    dir_path = os.path.join(output_path, "run_summary", f"client={client}", f"date={run_date}")
    os.makedirs(dir_path, exist_ok=True)
    summary_path = os.path.join(dir_path, "summary.json")
    # merge into existing summary if present (multiple domains per run)
    if os.path.exists(summary_path):
        with open(summary_path) as f:
            existing = json.load(f)
    else:
        existing = {"run_date": run_date, "client": client, "status": "passed", "domains": {}}
    existing["domains"][domain] = domain_summary
    # roll up status
    statuses = [d.get("status", "passed") for d in existing["domains"].values()]
    if all(s == "passed" for s in statuses):
        existing["status"] = "passed"
    elif all(s == "failed" for s in statuses):
        existing["status"] = "failed"
    else:
        existing["status"] = "partial_failure"
    with open(summary_path, "w") as f:
        json.dump(existing, f, indent=2)
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_report_writer.py -v
```

Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/qc/report/writer.py tests/test_report_writer.py
git commit -m "feat: report writer — issues.parquet, delta_report.json, run_summary.json"
```

---

## Task 8: Alert (Stubbed) & Main Orchestrator

**Files:**
- Create: `src/qc/alert/notify.py`
- Create: `src/qc/main.py`

**Interfaces:**
- Consumes: all prior modules
- Produces: `main(run_date: str, config: dict) -> int` — returns 0 on success, 1 if any domain failed

- [ ] **Step 1: Implement notify.py (stubbed)**

```python
# src/qc/alert/notify.py
import logging

logger = logging.getLogger(__name__)


def notify_threshold_breach(client: str, domain: str, run_date: str, flags: list[dict]) -> None:
    error_count = sum(1 for f in flags if f.get("severity") == "error")
    warn_count = sum(1 for f in flags if f.get("severity") == "warn")
    logger.warning(
        "THRESHOLD_BREACH client=%s domain=%s run_date=%s errors=%d warnings=%d "
        "[SNS notify stubbed — configure SNS ARN to enable]",
        client, domain, run_date, error_count, warn_count,
    )
    # SNS publish intentionally disabled for sandbox:
    # sns = boto3.client("sns")
    # sns.publish(TopicArn=SNS_TOPIC_ARN, Message=...)
```

- [ ] **Step 2: Implement main.py**

```python
# src/qc/main.py
import logging
import os
import sys
import yaml
import importlib
import pkgutil

from qc.ingest.reader import discover_clients_domains, load_domain
from qc.checks import missing, types, ranges, dedup, timestamp, referential, logic
import qc.checks  # ensure registry populated
from qc.temporal.compare import compute_snapshot, compare
from qc.metrics.store import append_snapshot, get_prior_snapshots
from qc.report.writer import write_artifacts
from qc.alert.notify import notify_threshold_breach

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

CHECK_MODULES = {
    "missing": missing.check,
    "types": types.check,
    "ranges": ranges.check,
    "dedup": dedup.check,
    "timestamp": timestamp.check,
    "referential": referential.check,
    "logic": logic.check,
}


def load_config(path: str = "config/config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_suite(config_dir: str, client: str, domain: str) -> dict:
    suite_path = os.path.join(config_dir, "suites", client, f"{domain}.yaml")
    if not os.path.exists(suite_path):
        logger.warning("no suite found at %s — skipping checks for %s/%s", suite_path, client, domain)
        return {"checks": []}
    with open(suite_path) as f:
        return yaml.safe_load(f)


def run_checks(df, suite: dict, *, run_date: str, client: str, domain: str) -> tuple[list[dict], dict]:
    all_flags = []
    by_check = {}
    checks_run = []
    ctx = dict(run_date=run_date, client=client, domain=domain)

    for check_block in suite.get("checks", []):
        for check_name, check_cfg in check_block.items():
            if check_name == "delta":
                continue  # handled in temporal comparison
            if check_name == "custom":
                # custom plugin checks handled via registry
                from qc.checks.registry import get_checks
                for rule_cfg in (check_cfg or []):
                    rule_name = rule_cfg.get("rule")
                    plugin_checks = get_checks(domain)
                    if rule_name in plugin_checks:
                        flags = plugin_checks[rule_name](df, rule_cfg, **ctx)
                        all_flags.extend(flags)
                        checks_run.append(rule_name)
                        flagged_cols = list({f["column"] for f in flags if f["column"]})
                        by_check[rule_name] = {"flags": len(flags), "columns": flagged_cols}
                continue
            fn = CHECK_MODULES.get(check_name)
            if fn is None:
                logger.warning("unknown check '%s' — skipping", check_name)
                continue
            flags = fn(df, check_cfg or {}, **ctx)
            all_flags.extend(flags)
            checks_run.append(check_name)
            flagged_cols = list({f["column"] for f in flags if f["column"]})
            by_check[check_name] = {"flags": len(flags), "columns": flagged_cols}

    return all_flags, {"checks_run": checks_run, "by_check": by_check}


def main(run_date: str, config: dict) -> int:
    bucket = config["source"]["bucket"]
    prefix = config["source"]["prefix"]
    output_path = config["output"]["path"]
    history_path = config["history"]["path"]
    db_path = os.path.join(history_path, "metrics.duckdb")
    thresholds = config.get("thresholds", {})
    config_dir = os.path.dirname(os.path.abspath("config/config.yaml"))

    os.makedirs(output_path, exist_ok=True)
    os.makedirs(history_path, exist_ok=True)

    pairs = discover_clients_domains(bucket, prefix, run_date)
    if not pairs:
        logger.warning("no client+domain data found for date=%s", run_date)
        return 0

    any_failed = False

    for client, domain in pairs:
        logger.info("processing client=%s domain=%s", client, domain)
        try:
            df = load_domain(bucket, prefix, client, domain, run_date)
        except Exception as exc:
            logger.error("failed to load client=%s domain=%s: %s", client, domain, exc)
            write_artifacts(output_path, client, domain, run_date, [], [], {
                "status": "failed",
                "error": str(exc),
                "rows_read": 0,
                "checks_run": [],
                "flags_raised": 0,
                "by_check": {},
            })
            any_failed = True
            continue

        suite = load_suite(config_dir, client, domain)
        issues, check_meta = run_checks(df, suite, run_date=run_date, client=client, domain=domain)

        today_snapshot = compute_snapshot(df)
        priors = get_prior_snapshots(db_path, client, domain, run_date)
        delta_flags = compare(today_snapshot, priors, thresholds, run_date=run_date, client=client, domain=domain)
        append_snapshot(db_path, client, domain, run_date, today_snapshot)

        all_flags = issues + delta_flags
        domain_summary = {
            "status": "passed" if not any(f["severity"] == "error" for f in all_flags) else "failed",
            "rows_read": len(df),
            "flags_raised": len(all_flags),
            **check_meta,
        }

        write_artifacts(output_path, client, domain, run_date, issues, delta_flags, domain_summary)

        if all_flags:
            notify_threshold_breach(client, domain, run_date, all_flags)

        logger.info(
            "done client=%s domain=%s rows=%d flags=%d",
            client, domain, len(df), len(all_flags),
        )

    return 1 if any_failed else 0


if __name__ == "__main__":
    import argparse
    cfg = load_config()
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="run date YYYY-MM-DD")
    args = parser.parse_args()
    sys.exit(main(args.date, cfg))
```

- [ ] **Step 3: Commit**

```bash
git add src/qc/alert/notify.py src/qc/main.py
git commit -m "feat: main orchestrator and stubbed alert module"
```

---

## Task 9: Scripts & Deploy Files

**Files:**
- Create: `scripts/poll_and_run.sh`
- Create: `scripts/run_dashboard.sh`
- Create: `scripts/bootstrap_ec2.sh`
- Create: `deploy/qc.service`
- Create: `deploy/qc.timer`
- Create: `deploy/dashboard.service`
- Create: `deploy/cloudwatch-agent.json`

- [ ] **Step 1: Create scripts/poll_and_run.sh**

```bash
#!/usr/bin/env bash
set -euo pipefail

BUCKET="${QC_SOURCE_BUCKET:?QC_SOURCE_BUCKET not set}"
PREFIX="${QC_SOURCE_PREFIX:-landing}"
SENTINEL_FILE="${QC_HISTORY_PATH:-/data/history}/.last_run_date"
TODAY=$(date -u +%Y-%m-%d)
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Check if already ran today
if [[ -f "$SENTINEL_FILE" ]] && [[ "$(cat "$SENTINEL_FILE")" == "$TODAY" ]]; then
    echo "[$TODAY] already ran today — skipping"
    exit 0
fi

# Check if today's data has landed in S3
KEY_COUNT=$(aws s3 ls "s3://${BUCKET}/${PREFIX}/date=${TODAY}/" 2>/dev/null | wc -l || echo 0)
if [[ "$KEY_COUNT" -eq 0 ]]; then
    echo "[$TODAY] no data found at s3://${BUCKET}/${PREFIX}/date=${TODAY}/ — waiting"
    exit 0
fi

echo "[$TODAY] data found — starting QC run"
source "${REPO_DIR}/.venv/bin/activate"
python -m qc.main --date "$TODAY"
STATUS=$?

if [[ $STATUS -eq 0 ]]; then
    echo "$TODAY" > "$SENTINEL_FILE"
    echo "[$TODAY] QC run completed successfully"
else
    echo "[$TODAY] QC run failed with exit code $STATUS — will retry next poll"
fi

exit $STATUS
```

- [ ] **Step 2: Create scripts/run_dashboard.sh**

```bash
#!/usr/bin/env bash
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${REPO_DIR}/.venv/bin/activate"
exec gunicorn "qc.dashboard.app:app" \
    --bind 0.0.0.0:8080 \
    --workers 2 \
    --timeout 60 \
    --access-logfile - \
    --error-logfile -
```

- [ ] **Step 3: Create scripts/bootstrap_ec2.sh**

```bash
#!/usr/bin/env bash
set -euo pipefail

echo "=== Installing system deps ==="
sudo dnf install -y python3.11 python3.11-pip git || \
    sudo apt-get install -y python3.11 python3.11-venv git

echo "=== Creating data directories ==="
sudo mkdir -p /data/history /data/output
sudo chown -R ec2-user:ec2-user /data

echo "=== Setting up Python venv ==="
cd /opt/qc-sandbox
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e ".[dev]"

echo "=== Installing CloudWatch agent ==="
wget -q https://s3.amazonaws.com/amazoncloudwatch-agent/amazon_linux/amd64/latest/amazon-cloudwatch-agent.rpm
sudo rpm -U ./amazon-cloudwatch-agent.rpm
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
    -a fetch-config -m ec2 -s \
    -c file:/opt/qc-sandbox/deploy/cloudwatch-agent.json

echo "=== Enabling systemd services ==="
sudo cp deploy/qc.service deploy/qc.timer deploy/dashboard.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now qc.timer
sudo systemctl enable --now dashboard.service

echo "=== Bootstrap complete ==="
```

- [ ] **Step 4: Create deploy/qc.service**

```ini
[Unit]
Description=Healthcare Data QC Pipeline
After=network.target

[Service]
Type=oneshot
User=ec2-user
WorkingDirectory=/opt/qc-sandbox
EnvironmentFile=/opt/qc-sandbox/.env
ExecStart=/opt/qc-sandbox/scripts/poll_and_run.sh
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 5: Create deploy/qc.timer**

```ini
[Unit]
Description=Healthcare Data QC Pipeline — poll every 15 minutes

[Timer]
OnBootSec=5min
OnUnitInactiveSec=15min
AccuracySec=1min

[Install]
WantedBy=timers.target
```

- [ ] **Step 6: Create deploy/dashboard.service**

```ini
[Unit]
Description=Healthcare Data QC Dashboard
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/opt/qc-sandbox
EnvironmentFile=/opt/qc-sandbox/.env
ExecStart=/opt/qc-sandbox/scripts/run_dashboard.sh
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 7: Create deploy/cloudwatch-agent.json**

```json
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/syslog",
            "log_group_name": "/qc-pipeline/system",
            "log_stream_name": "{instance_id}"
          }
        ]
      },
      "systemd_journal": {
        "log_group_name": "/qc-pipeline/app",
        "log_stream_name": "{instance_id}"
      }
    }
  },
  "metrics": {
    "namespace": "QC/Pipeline",
    "metrics_collected": {
      "cpu": {"measurement": ["cpu_usage_idle"], "metrics_collection_interval": 60},
      "mem": {"measurement": ["mem_used_percent"], "metrics_collection_interval": 60}
    }
  }
}
```

- [ ] **Step 8: Make scripts executable and commit**

```bash
chmod +x scripts/poll_and_run.sh scripts/run_dashboard.sh scripts/bootstrap_ec2.sh
git add scripts/ deploy/
git commit -m "feat: deployment scripts and systemd units"
```

---

## Task 10: Integration Smoke Test

**Files:**
- Create: `tests/test_integration.py`

**Interfaces:**
- Consumes: `main` from `qc.main`, `moto` for S3 mocking, all prior modules

- [ ] **Step 1: Write integration test**

```python
# tests/test_integration.py
import io
import os
import json
import pandas as pd
import pytest
import boto3
from moto import mock_aws
from qc.main import main

BUCKET = "test-source-bucket"
RUN_DATE = "2026-07-17"

@pytest.fixture
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

@pytest.fixture
def s3_with_data(aws_credentials):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        df = pd.DataFrame({
            "patient_id": ["P001", "P002", "P003"],
            "admit_ts": pd.to_datetime(["2026-07-17 08:00", "2026-07-17 09:00", "2026-07-17 10:00"]),
            "discharge_ts": pd.to_datetime(["2026-07-17 10:00", "2026-07-17 11:00", "2026-07-17 12:00"]),
            "age": [45, 30, 200],
            "disposition": ["DISCHARGE", "LWBS", "TRANSFER"],
        })
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        s3.put_object(
            Bucket=BUCKET,
            Key=f"landing/date={RUN_DATE}/client=ohiohealth/domain=ed/data.csv",
            Body=buf.read(),
        )
        yield s3

def test_full_pipeline_run(s3_with_data, tmp_duckdb, tmp_output, monkeypatch):
    config = {
        "source": {"bucket": BUCKET, "prefix": "landing"},
        "output": {"path": tmp_output},
        "history": {"path": os.path.dirname(tmp_duckdb)},
        "thresholds": {"row_count_max_pct_change": 0.30, "null_rate_max_abs_change": 0.05},
    }
    # patch config dir so suite is found
    monkeypatch.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    with mock_aws():
        import boto3
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        df = pd.DataFrame({
            "patient_id": ["P001", "P002", "P003"],
            "admit_ts": pd.to_datetime(["2026-07-17 08:00", "2026-07-17 09:00", "2026-07-17 10:00"]),
            "discharge_ts": pd.to_datetime(["2026-07-17 10:00", "2026-07-17 11:00", "2026-07-17 12:00"]),
            "age": [45, 30, 200],
            "disposition": ["DISCHARGE", "LWBS", "TRANSFER"],
        })
        import io
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        boto3.client("s3", region_name="us-east-1").put_object(
            Bucket=BUCKET,
            Key=f"landing/date={RUN_DATE}/client=ohiohealth/domain=ed/data.csv",
            Body=buf.read(),
        )
        exit_code = main(RUN_DATE, config)

    assert exit_code == 0

    issues_path = os.path.join(tmp_output, "qc_issues", "client=ohiohealth", "domain=ed", f"date={RUN_DATE}", "issues.parquet")
    assert os.path.exists(issues_path)
    issues_df = pd.read_parquet(issues_path)
    assert len(issues_df) > 0

    delta_path = os.path.join(tmp_output, "qc_delta", "client=ohiohealth", "domain=ed", f"date={RUN_DATE}", "delta_report.json")
    assert os.path.exists(delta_path)

    summary_path = os.path.join(tmp_output, "run_summary", "client=ohiohealth", f"date={RUN_DATE}", "summary.json")
    assert os.path.exists(summary_path)
    summary = json.loads(open(summary_path).read())
    assert "ed" in summary["domains"]
    assert summary["domains"]["ed"]["rows_read"] == 3

    # Source was not modified — verify no unexpected writes
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    # only original key should exist
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])]
    assert all(k.startswith("landing/") for k in keys), "unexpected writes to S3"
```

- [ ] **Step 2: Run integration test**

```bash
pytest tests/test_integration.py -v
```

Expected: 1 passed

- [ ] **Step 3: Run full test suite**

```bash
pytest tests/ -v --tb=short
```

Expected: all tests pass

- [ ] **Step 4: Commit**

```bash
git add tests/test_integration.py
git commit -m "test: end-to-end integration smoke test"
```

---

## Task 11: Flask Dashboard

**Files:**
- Create: `src/qc/dashboard/loader.py`
- Create: `src/qc/dashboard/app.py`
- Create: `src/qc/dashboard/templates/base.html`
- Create: `src/qc/dashboard/templates/summary.html`
- Create: `src/qc/dashboard/templates/issues.html`

**Interfaces:**
- Consumes: `/data/output` directory structure from `writer.py`
- Produces: Flask app at port 8080, routes `/` and `/issues`

- [ ] **Step 1: Implement loader.py**

```python
# src/qc/dashboard/loader.py
import glob
import json
import os
import pandas as pd


def get_output_path() -> str:
    return os.environ.get("QC_OUTPUT_PATH", "/data/output")


def load_latest_summaries() -> list[dict]:
    base = get_output_path()
    pattern = os.path.join(base, "run_summary", "client=*", "date=*", "summary.json")
    rows = []
    for path in sorted(glob.glob(pattern)):
        parts = path.split(os.sep)
        client = parts[-3].split("=", 1)[1]
        date = parts[-2].split("=", 1)[1]
        with open(path) as f:
            summary = json.load(f)
        for domain, meta in summary.get("domains", {}).items():
            rows.append({
                "client": client,
                "domain": domain,
                "run_date": date,
                "status": meta.get("status", "unknown"),
                "rows_read": meta.get("rows_read", 0),
                "flags_raised": meta.get("flags_raised", 0),
            })
    return rows


def load_issues(client: str | None, domain: str | None, run_date: str | None,
                rule: str | None, severity: str | None, page: int = 1, page_size: int = 100) -> tuple[pd.DataFrame, int]:
    base = get_output_path()
    client_glob = f"client={client}" if client else "client=*"
    domain_glob = f"domain={domain}" if domain else "domain=*"
    date_glob = f"date={run_date}" if run_date else "date=*"
    pattern = os.path.join(base, "qc_issues", client_glob, domain_glob, date_glob, "issues.parquet")
    frames = [pd.read_parquet(p) for p in glob.glob(pattern)]
    if not frames:
        return pd.DataFrame(), 0
    df = pd.concat(frames, ignore_index=True)
    if rule:
        df = df[df["rule"] == rule]
    if severity:
        df = df[df["severity"] == severity]
    total = len(df)
    start = (page - 1) * page_size
    return df.iloc[start:start + page_size], total


def load_delta_report(client: str, domain: str, run_date: str) -> dict:
    base = get_output_path()
    path = os.path.join(base, "qc_delta", f"client={client}", f"domain={domain}", f"date={run_date}", "delta_report.json")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)
```

- [ ] **Step 2: Implement app.py**

```python
# src/qc/dashboard/app.py
from flask import Flask, render_template, request
from qc.dashboard.loader import load_latest_summaries, load_issues, load_delta_report

app = Flask(__name__)


@app.route("/")
def summary():
    rows = load_latest_summaries()
    return render_template("summary.html", rows=rows)


@app.route("/issues")
def issues():
    client = request.args.get("client") or None
    domain = request.args.get("domain") or None
    run_date = request.args.get("run_date") or None
    rule = request.args.get("rule") or None
    severity = request.args.get("severity") or None
    page = int(request.args.get("page", 1))

    df, total = load_issues(client, domain, run_date, rule, severity, page)
    delta = {}
    if client and domain and run_date:
        delta = load_delta_report(client, domain, run_date)

    return render_template(
        "issues.html",
        rows=df.to_dict("records") if not df.empty else [],
        total=total,
        page=page,
        page_size=100,
        delta=delta,
        filters={"client": client, "domain": domain, "run_date": run_date,
                 "rule": rule, "severity": severity},
    )
```

- [ ] **Step 3: Create templates/base.html**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Healthcare QC Dashboard</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 0; padding: 0; background: #f5f5f5; color: #222; }
    header { background: #1a3a5c; color: white; padding: 12px 24px; display: flex; align-items: center; gap: 16px; }
    header a { color: white; text-decoration: none; font-weight: bold; }
    nav a { color: #aed4ff; margin-right: 16px; text-decoration: none; }
    nav a:hover { text-decoration: underline; }
    main { padding: 24px; }
    table { border-collapse: collapse; width: 100%; background: white; border-radius: 6px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,.1); }
    th { background: #1a3a5c; color: white; padding: 10px 14px; text-align: left; font-size: 13px; }
    td { padding: 9px 14px; border-bottom: 1px solid #eee; font-size: 13px; }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: #f0f6ff; }
    .status-passed { color: #1a7a40; font-weight: bold; }
    .status-failed { color: #c0392b; font-weight: bold; }
    .status-partial_failure { color: #e67e22; font-weight: bold; }
    .badge { padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: bold; }
    .badge-error { background: #fde8e8; color: #c0392b; }
    .badge-warn { background: #fef3e0; color: #e67e22; }
    .filters { background: white; padding: 16px; border-radius: 6px; margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,.1); display: flex; gap: 12px; flex-wrap: wrap; align-items: flex-end; }
    .filters label { font-size: 12px; font-weight: bold; display: block; margin-bottom: 4px; }
    .filters input, .filters select { padding: 6px 10px; border: 1px solid #ccc; border-radius: 4px; font-size: 13px; }
    .filters button { padding: 6px 16px; background: #1a3a5c; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 13px; }
    .pagination { margin-top: 12px; display: flex; gap: 8px; align-items: center; }
    .pagination a { padding: 4px 10px; background: #1a3a5c; color: white; text-decoration: none; border-radius: 4px; font-size: 13px; }
    .delta-panel { background: white; padding: 16px; border-radius: 6px; margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,.1); }
    .delta-panel h3 { margin: 0 0 12px; font-size: 14px; color: #1a3a5c; }
    .flagged { color: #c0392b; font-weight: bold; }
  </style>
</head>
<body>
  <header>
    <a href="/">Healthcare QC</a>
    <nav>
      <a href="/">Summary</a>
      <a href="/issues">Issues</a>
    </nav>
  </header>
  <main>{% block content %}{% endblock %}</main>
</body>
</html>
```

- [ ] **Step 4: Create templates/summary.html**

```html
{% extends "base.html" %}
{% block content %}
<h2 style="margin-top:0">Run Summary</h2>
{% if rows %}
<table>
  <thead>
    <tr>
      <th>Client</th><th>Domain</th><th>Run Date</th>
      <th>Status</th><th>Rows Read</th><th>Flags Raised</th><th>Actions</th>
    </tr>
  </thead>
  <tbody>
  {% for row in rows | sort(attribute='run_date', reverse=True) %}
    <tr>
      <td>{{ row.client }}</td>
      <td>{{ row.domain }}</td>
      <td>{{ row.run_date }}</td>
      <td class="status-{{ row.status }}">{{ row.status }}</td>
      <td>{{ row.rows_read | default(0) }}</td>
      <td>{{ row.flags_raised | default(0) }}</td>
      <td>
        <a href="/issues?client={{ row.client }}&domain={{ row.domain }}&run_date={{ row.run_date }}"
           style="font-size:12px;color:#1a3a5c">View Issues →</a>
      </td>
    </tr>
  {% endfor %}
  </tbody>
</table>
{% else %}
<p>No runs found. Run the pipeline first.</p>
{% endif %}
{% endblock %}
```

- [ ] **Step 5: Create templates/issues.html**

```html
{% extends "base.html" %}
{% block content %}
<h2 style="margin-top:0">Issues Drilldown</h2>

<form class="filters" method="get" action="/issues">
  <div><label>Client</label><input name="client" value="{{ filters.client or '' }}" placeholder="ohiohealth"></div>
  <div><label>Domain</label><input name="domain" value="{{ filters.domain or '' }}" placeholder="ed"></div>
  <div><label>Run Date</label><input name="run_date" value="{{ filters.run_date or '' }}" placeholder="2026-07-17"></div>
  <div><label>Rule</label><input name="rule" value="{{ filters.rule or '' }}" placeholder="range"></div>
  <div><label>Severity</label>
    <select name="severity">
      <option value="">All</option>
      <option value="error" {% if filters.severity == 'error' %}selected{% endif %}>error</option>
      <option value="warn" {% if filters.severity == 'warn' %}selected{% endif %}>warn</option>
    </select>
  </div>
  <button type="submit">Filter</button>
</form>

{% if delta and delta.flags %}
<div class="delta-panel">
  <h3>Delta Report — {{ filters.client }} / {{ filters.domain }} / {{ filters.run_date }}</h3>
  <table>
    <thead><tr><th>Column</th><th>Rule</th><th>Detail</th><th>Severity</th></tr></thead>
    <tbody>
    {% for f in delta.flags %}
      <tr>
        <td>{{ f.column or '—' }}</td>
        <td>{{ f.rule }}</td>
        <td class="{% if f.severity == 'error' %}flagged{% endif %}">{{ f.detail }}</td>
        <td><span class="badge badge-{{ f.severity }}">{{ f.severity }}</span></td>
      </tr>
    {% endfor %}
    </tbody>
  </table>
</div>
{% endif %}

<p style="font-size:13px;color:#666">{{ total }} total issues {% if rows %}(showing page {{ page }}){% endif %}</p>

{% if rows %}
<table>
  <thead>
    <tr><th>Run Date</th><th>Client</th><th>Domain</th><th>Row ID</th>
        <th>Column</th><th>Rule</th><th>Severity</th><th>Detail</th></tr>
  </thead>
  <tbody>
  {% for row in rows %}
    <tr>
      <td>{{ row.run_date }}</td>
      <td>{{ row.client }}</td>
      <td>{{ row.domain }}</td>
      <td>{{ row.row_id if row.row_id is not none else '—' }}</td>
      <td>{{ row.column or '—' }}</td>
      <td>{{ row.rule }}</td>
      <td><span class="badge badge-{{ row.severity }}">{{ row.severity }}</span></td>
      <td style="max-width:300px;word-break:break-word">{{ row.detail or '' }}</td>
    </tr>
  {% endfor %}
  </tbody>
</table>
<div class="pagination">
  {% if page > 1 %}<a href="?{{ request.query_string.decode() | replace('page=' ~ page, 'page=' ~ (page-1)) }}">← Prev</a>{% endif %}
  <span style="font-size:13px">Page {{ page }} of {{ ((total - 1) // 100 + 1) }}</span>
  {% if page * 100 < total %}<a href="?{{ request.query_string.decode() | replace('page=' ~ page, 'page=' ~ (page+1)) }}">Next →</a>{% endif %}
</div>
{% else %}
<p>No issues found for the selected filters.</p>
{% endif %}
{% endblock %}
```

- [ ] **Step 6: Run all tests to confirm dashboard didn't break anything**

```bash
pytest tests/ -v --tb=short
```

Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add src/qc/dashboard/ 
git commit -m "feat: lightweight Flask dashboard — summary landing and issues drilldown"
```

---

## Task 12: Final Wiring & README

**Files:**
- Create: `README.md`
- Create: `.env.example`

- [ ] **Step 1: Create .env.example**

```bash
QC_SOURCE_BUCKET=your-source-bucket-name
QC_SOURCE_PREFIX=landing
QC_OUTPUT_PATH=/data/output
QC_HISTORY_PATH=/data/history
```

- [ ] **Step 2: Run complete test suite one final time**

```bash
pytest tests/ -v --tb=short --cov=src/qc --cov-report=term-missing
```

Expected: all tests pass; coverage report shows checks, store, compare, writer, registry covered

- [ ] **Step 3: Create README.md**

```markdown
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
```

- [ ] **Step 4: Final commit**

```bash
git add README.md .env.example
git commit -m "docs: README and env example — project complete"
```

---

## Self-Review Checklist

- [x] **Spec coverage:** All 10 spec sections mapped to tasks — ingest (T6), checks (T3), temporal (T5), metrics (T4), report (T7), alert (T8), main (T8), scripts (T9), dashboard (T11), deploy (T9)
- [x] **No placeholders:** All code blocks complete; no TBD/TODO in task steps
- [x] **Type consistency:** `check(df, cfg, *, run_date, client, domain)` contract consistent across all check modules; `flags_from` signature consistent; `append_snapshot`/`get_prior_snapshots` signatures match usage in `main.py`
- [x] **Client+domain partitioning:** Enforced at reader, store, writer, dashboard — no cross-client data bleed
- [x] **PHI boundary:** No raw values logged in any module — counts and rates only
- [x] **SNS stubbed:** `notify.py` contains no active SNS calls
- [x] **Output to EBS only:** `writer.py` writes to `/data/output`, no S3 writes
