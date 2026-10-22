"""Shared fixtures. The database tests run against a real Postgres instance.

Each test gets a freshly created dedicated schema inside the configured
database, so the tests never touch tables the pipeline itself has written.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import psycopg
import pytest

from netlog_anomaly import db

if TYPE_CHECKING:
    from netlog_anomaly.streaming.config import Settings as StreamSettings

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


# Streaming fixtures. The streaming tests need Kafka and OpenSearch as well as
# Postgres, and those are brought up by docker-compose.stream.yml rather than
# installed, so they skip instead of failing: a checkout without the stack
# running should still get a green batch suite.

STREAM_DATA = Path(__file__).resolve().parents[1] / "data" / "BGL.log"


@pytest.fixture(scope="session")
def stream_settings() -> StreamSettings:
    from netlog_anomaly.streaming.config import Settings

    settings = Settings.from_env()
    _require_kafka(settings)
    _require_opensearch(settings)
    return settings


def _require_kafka(settings: StreamSettings) -> None:
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    try:
        metadata = admin.list_topics(timeout=10)
    except KafkaException as exc:
        pytest.skip(f"no Kafka at {settings.bootstrap_servers}: {exc}")
    if len(metadata.brokers) == 0:
        pytest.skip(f"no brokers reachable at {settings.bootstrap_servers}")


def _require_opensearch(settings: StreamSettings) -> None:
    from opensearchpy.exceptions import OpenSearchException

    from netlog_anomaly.streaming.sink import client

    try:
        if not client(settings).ping():
            pytest.skip(f"no OpenSearch at {settings.opensearch_url}")
    except OpenSearchException as exc:
        pytest.skip(f"no OpenSearch at {settings.opensearch_url}: {exc}")


@pytest.fixture(scope="session")
def bgl_log() -> Path:
    """The real dataset, which scripts/download_data.sh fetches."""
    if not STREAM_DATA.exists():
        pytest.skip(f"{STREAM_DATA} is missing; run scripts/download_data.sh")
    return STREAM_DATA


@pytest.fixture
def batch_conn(database_url: str) -> Iterator[psycopg.Connection]:
    """A connection to the batch tables themselves, not a throwaway schema.

    The parity test compares streamed windows against the windows the batch
    pipeline actually produced, so it has to read the real tables. It only ever
    reads them.
    """
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
            cur.execute("SET default_transaction_read_only = on")
            cur.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = 'window_features'"
            )
            row = cur.fetchone()
            if row is None or row[0] == 0:
                pytest.skip("the batch tables are not present in this database")
            cur.execute("SELECT count(*) FROM window_features WHERE window_seconds = 60")
            row = cur.fetchone()
            if row is None or row[0] == 0:
                pytest.skip("no 60 second windows loaded; run the batch pipeline first")
        yield connection


@pytest.fixture
def stream_topic(stream_settings: StreamSettings) -> Iterator[str]:
    """A topic of this test's own, deleted afterwards.

    Each test gets its own so one test's retained records and committed
    offsets cannot change another's result.
    """
    from confluent_kafka.admin import AdminClient

    from netlog_anomaly.streaming.producer import ensure_topic

    name = f"test-raw-{uuid4().hex[:10]}"
    ensure_topic(stream_settings, topic=name, partitions=6, replication_factor=3)
    try:
        yield name
    finally:
        admin = AdminClient({"bootstrap.servers": stream_settings.bootstrap_servers})
        for future in admin.delete_topics([name]).values():
            with suppress(Exception):
                future.result(timeout=30)


@pytest.fixture
def stream_index(stream_settings: StreamSettings) -> Iterator[str]:
    """An index of this test's own, deleted afterwards."""
    from netlog_anomaly.streaming.sink import client

    name = f"test-windows-{uuid4().hex[:10]}"
    os_client = client(stream_settings)
    try:
        yield name
    finally:
        with suppress(Exception):
            if os_client.indices.exists(index=name):
                os_client.indices.delete(index=name)


@pytest.fixture
def stream_group() -> str:
    """A consumer group of this test's own, so offsets never carry over."""
    return f"test-group-{uuid4().hex[:10]}"
