"""Shared fixtures. The database tests run against a real Postgres instance.

Each test gets a freshly created dedicated schema inside the configured
database, so the tests never touch tables the pipeline itself has written.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

from netlog_anomaly import db

TEST_SCHEMA = "netlog_test"
FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def database_url() -> str:
    url = os.environ.get("NETLOG_TEST_DATABASE_URL") or os.environ.get(db.ENV_VAR)
    if not url:
        pytest.fail(
            f"These tests need a real Postgres instance. Set {db.ENV_VAR} "
            "(or NETLOG_TEST_DATABASE_URL) to a connection string.",
            pytrace=False,
        )
    return url


@pytest.fixture
def conn(database_url: str) -> Iterator[psycopg.Connection]:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
            cur.execute(f"CREATE SCHEMA {TEST_SCHEMA}")
            cur.execute(f"SET search_path TO {TEST_SCHEMA}")
        db.apply_schema(connection)
        connection.commit()
        try:
            yield connection
        finally:
            connection.rollback()
            with connection.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
            connection.commit()


@pytest.fixture
def sample_log() -> Path:
    return FIXTURES / "sample_bgl.log"


@pytest.fixture
def malformed_log() -> Path:
    return FIXTURES / "malformed_bgl.log"


@pytest.fixture
def windows_log() -> Path:
    return FIXTURES / "windows_bgl.log"
