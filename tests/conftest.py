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
