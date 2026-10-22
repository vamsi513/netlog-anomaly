"""Settings for streaming mode, read from the environment.

Defaults point at the stack in docker-compose.stream.yml. The bootstrap list
names all three brokers rather than one: a client given a single broker that
happens to be the one that is down cannot reach the cluster at all, which is
the opposite of what three brokers are for.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

BOOTSTRAP_ENV = "NETLOG_KAFKA_BOOTSTRAP"
TOPIC_ENV = "NETLOG_KAFKA_TOPIC"
GROUP_ENV = "NETLOG_KAFKA_GROUP"
OPENSEARCH_ENV = "NETLOG_OPENSEARCH_URL"
INDEX_ENV = "NETLOG_OPENSEARCH_INDEX"
WINDOW_ENV = "NETLOG_STREAM_WINDOW_SECONDS"
LATENESS_ENV = "NETLOG_STREAM_ALLOWED_LATENESS"
STATS_ENV = "NETLOG_STREAM_STATS"

DEFAULT_BOOTSTRAP = "localhost:19092,localhost:19093,localhost:19094"
DEFAULT_TOPIC = "bgl-raw"
DEFAULT_GROUP = "netlog-windows"
DEFAULT_OPENSEARCH_URL = "http://localhost:9200"
DEFAULT_INDEX = "netlog-windows"
DEFAULT_WINDOW_SECONDS = 60
DEFAULT_ALLOWED_LATENESS = 0

# Shipped with the package rather than written at runtime, so a consumer can
# score without a Postgres connection. netlog_anomaly.streaming.stats writes it.
DEFAULT_STATS_PATH = Path(__file__).with_name("training_stats.json")


@dataclass(frozen=True, slots=True)
class Settings:
    """Where the stream lives and how it is windowed."""

    bootstrap_servers: str = DEFAULT_BOOTSTRAP
    topic: str = DEFAULT_TOPIC
    group_id: str = DEFAULT_GROUP
    opensearch_url: str = DEFAULT_OPENSEARCH_URL
    index: str = DEFAULT_INDEX
    window_seconds: int = DEFAULT_WINDOW_SECONDS
    allowed_lateness: int = DEFAULT_ALLOWED_LATENESS
    stats_path: Path = DEFAULT_STATS_PATH

    @classmethod
    def from_env(cls) -> Settings:
        stats = os.environ.get(STATS_ENV)
        return cls(
            bootstrap_servers=os.environ.get(BOOTSTRAP_ENV, DEFAULT_BOOTSTRAP),
            topic=os.environ.get(TOPIC_ENV, DEFAULT_TOPIC),
            group_id=os.environ.get(GROUP_ENV, DEFAULT_GROUP),
            opensearch_url=os.environ.get(OPENSEARCH_ENV, DEFAULT_OPENSEARCH_URL),
            index=os.environ.get(INDEX_ENV, DEFAULT_INDEX),
            window_seconds=int(os.environ.get(WINDOW_ENV, DEFAULT_WINDOW_SECONDS)),
            allowed_lateness=int(os.environ.get(LATENESS_ENV, DEFAULT_ALLOWED_LATENESS)),
            stats_path=Path(stats) if stats else DEFAULT_STATS_PATH,
        )
