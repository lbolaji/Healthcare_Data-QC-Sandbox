import json
from datetime import date, timedelta
import duckdb


def init_db(db_path: str) -> None:
    con = duckdb.connect(db_path)
    _ensure_table(con)
    con.close()


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
        """INSERT INTO metrics VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT (client, domain, run_date) DO UPDATE SET
           row_count = EXCLUDED.row_count,
           dup_rate = EXCLUDED.dup_rate,
           metrics_json = EXCLUDED.metrics_json""",
        [client, domain, run_date,
         snapshot.get("row_count"), snapshot.get("dup_rate"),
         json.dumps(snapshot)],
    )
    con.close()


def get_snapshot(db_path: str, client: str, domain: str, run_date: str) -> dict | None:
    con = duckdb.connect(db_path, read_only=True)
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
