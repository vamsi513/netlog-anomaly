"""Database connection handling and SQL loading."""

from __future__ import annotations

import os
from importlib import resources

import psycopg

ENV_VAR = "NETLOG_DATABASE_URL"


def database_url() -> str:
    """Connection string for the pipeline database.

    Raises RuntimeError rather than falling back to a default, so a missing
    configuration fails loudly instead of writing to the wrong database.
    """
    url = os.environ.get(ENV_VAR)
    if not url:
        raise RuntimeError(
            f"{ENV_VAR} is not set. Copy .env.example and export it, for example:\n"
            f"  export {ENV_VAR}=postgresql://localhost:5432/netlog"
        )
    return url


def connect(url: str | None = None) -> psycopg.Connection:
    """Open a connection. The caller owns the transaction."""
    return psycopg.connect(url or database_url())


def read_sql(name: str) -> str:
    """Read a packaged SQL file by file name."""
    return resources.files("netlog_anomaly.sql").joinpath(name).read_text(encoding="utf-8")


def apply_schema(conn: psycopg.Connection) -> None:
    """Create the tables if they do not already exist."""
    with conn.cursor() as cur:
        cur.execute(read_sql("schema.sql"))


def reset_schema(conn: psycopg.Connection) -> None:
    """Drop every table this project owns, then recreate it."""
    with conn.cursor() as cur:
        cur.execute(
            """
            DROP TABLE IF EXISTS window_template_counts, window_features,
                quarantined_lines, log_events, event_templates, ingest_runs CASCADE
            """
        )
    apply_schema(conn)
