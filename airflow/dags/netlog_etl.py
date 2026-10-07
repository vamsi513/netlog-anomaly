"""Airflow DAG for the log ETL.

The DAG only sequences the same functions the command line uses; it holds no
pipeline logic of its own. Each task is idempotent, so a re-run of a cleared
task produces the same database state rather than duplicating rows.

Configuration comes from two Airflow Variables, with environment variables as
a fallback so the DAG can be run from a test harness without a UI:

    netlog_database_url  connection string for Postgres
    netlog_log_path      path to the log file to load

Run it without a scheduler with:

    AIRFLOW_HOME=... airflow dags test netlog_etl
"""

from __future__ import annotations

import os
from pathlib import Path

import pendulum
from airflow.sdk import Variable, dag, task

WINDOW_SECONDS = 60


def _setting(name: str, env_var: str, default: str | None = None) -> str:
    value = Variable.get(name, default=None) or os.environ.get(env_var) or default
    if not value:
        raise ValueError(f"Set the Airflow Variable {name!r} or the environment variable {env_var}")
    return value


@dag(
    dag_id="netlog_etl",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["netlog"],
    doc_md=__doc__,
)
def netlog_etl() -> None:
    @task
    def init_db() -> str:
        from netlog_anomaly import db

        url = _setting("netlog_database_url", "NETLOG_DATABASE_URL")
        with db.connect(url) as conn:
            db.apply_schema(conn)
            conn.commit()
        return url

    @task
    def load(url: str) -> dict[str, int]:
        from netlog_anomaly import db
        from netlog_anomaly.etl import load_file

        path = Path(_setting("netlog_log_path", "NETLOG_LOG_PATH"))
        with db.connect(url) as conn:
            stats = load_file(conn, path)
        return {
            "lines_read": stats.lines_read,
            "rows_inserted": stats.rows_inserted,
            "rows_duplicate": stats.rows_duplicate,
            "lines_quarantined": stats.lines_quarantined,
        }

    @task
    def aggregate(url: str) -> int:
        from netlog_anomaly import db
        from netlog_anomaly.etl import refresh_windows

        with db.connect(url) as conn:
            windows, _ = refresh_windows(conn, WINDOW_SECONDS)
        return windows

    @task
    def report(url: str, windows: int) -> str:
        from netlog_anomaly import db
        from netlog_anomaly.evaluate import evaluate, format_report

        if windows < 10:
            # Too little data to split meaningfully; say so rather than failing
            # on an empty side of the split.
            return f"Only {windows} windows built; skipping evaluation."
        with db.connect(url) as conn:
            text = format_report(evaluate(conn, window_seconds=WINDOW_SECONDS))
        print(text)
        return text

    url = init_db()
    loaded = load(url)
    windows = aggregate(url)
    loaded >> windows
    report(url, windows)


netlog_etl()
