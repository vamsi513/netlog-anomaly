"""Replay a BGL log file into the raw Kafka topic.

Partitioning is the one decision here that matters. The key is the window the
event belongs to::

    key = str(floor(epoch / window_seconds))

Kafka's default partitioner hashes the key, so every event of one window lands
on one partition, and therefore on one consumer. Keying by node instead would
spread a single window across up to six partitions and six consumers, none of
which could produce the window's event count, distinct node count or anomaly
flag on its own; they would need a shuffle, or a second aggregation stage over
partial windows, to put it back together. Keying by window buys exact
single-pass aggregation at the cost of skew: one busy minute is one hot
partition, and a window larger than one consumer can hold has nowhere to go.

The producer parses each line before publishing, for two reasons: the window
key needs the event time, and a line the parser rejects would otherwise reach
the topic and have to be rejected again by every consumer. The value published
is still the raw line, so the topic stays a raw feed and the consumer does its
own parsing rather than trusting a schema the producer invented.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from netlog_anomaly.etl import iter_lines
from netlog_anomaly.parser import ParseError, parse_line
from netlog_anomaly.streaming.config import Settings
from netlog_anomaly.streaming.windows import window_start

UNTHROTTLED = 0.0


@dataclass(frozen=True, slots=True)
class ProduceStats:
    """What one replay published."""

    topic: str
    lines_read: int
    published: int
    rejected: int
    elapsed_seconds: float

    @property
    def events_per_second(self) -> float:
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.published / self.elapsed_seconds


def producer_config(settings: Settings) -> dict[str, object]:
    """Durable, idempotent publishing.

    acks=all with min.insync.replicas=2 on the topic means a published event is
    on at least two brokers before the producer calls it done. Idempotence
    stops a retry after a timeout from writing the same event twice, which
    would otherwise inflate an event count that the parity test checks.
    """
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 10,
        "batch.size": 262_144,
    }


def ensure_topic(
    settings: Settings,
    topic: str | None = None,
    partitions: int = 6,
    replication_factor: int = 3,
    min_insync_replicas: int = 2,
) -> None:
    """Create the topic if it is missing, and wait until it is visible.

    docker-compose.stream.yml creates the default topic on start up. This is
    for the tests, which each want a topic of their own so one test's offsets
    and retained records cannot change another's result.
    """
    name = topic or settings.topic
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    if name in admin.list_topics(timeout=30).topics:
        return

    new_topic = NewTopic(
        name,
        num_partitions=partitions,
        replication_factor=replication_factor,
        config={"min.insync.replicas": str(min_insync_replicas)},
    )
    for future in admin.create_topics([new_topic]).values():
        try:
            future.result(timeout=30)
        except KafkaException as exc:
            # A concurrent creation is not a failure; anything else is.
            if "already exists" not in str(exc):
                raise

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        metadata = admin.list_topics(timeout=10)
        partition_count = len(metadata.topics[name].partitions) if name in metadata.topics else 0
        if partition_count == partitions:
            return
        time.sleep(0.2)
    raise TimeoutError(f"topic {name!r} did not become available with {partitions} partitions")


def replay(
    settings: Settings,
    path: Path,
    topic: str | None = None,
    limit: int | None = None,
    speed: float = UNTHROTTLED,
    on_progress: Callable[[int], None] | None = None,
) -> ProduceStats:
    """Publish a log file to the topic, optionally paced by event time.

    speed is a multiple of real time: 60 replays one minute of log per second
    of wall clock. The default, 0, publishes as fast as the cluster accepts,
    which is the only way a seven month log finishes in a test; throughput
    numbers are measured in that mode and the docs say so.
    """
    if speed < 0:
        raise ValueError(f"speed must not be negative, got {speed}")

    name = topic or settings.topic
    producer = Producer(producer_config(settings))

    lines_read = 0
    published = 0
    rejected = 0
    first_epoch: int | None = None
    started = time.monotonic()

    for line_no, line in iter_lines(path, limit=limit):
        lines_read += 1
        try:
            event = parse_line(line, line_no)
        except ParseError:
            # The batch loader quarantines these rather than counting them in a
            # window, so keeping them out of the topic keeps the two in step.
            rejected += 1
            continue

        if speed > 0:
            if first_epoch is None:
                first_epoch = event.epoch
            due = (event.epoch - first_epoch) / speed
            behind = due - (time.monotonic() - started)
            if behind > 0:
                time.sleep(behind)

        key = str(window_start(event.epoch, settings.window_seconds))
        while True:
            try:
                producer.produce(name, key=key.encode(), value=line.rstrip("\n").encode())
                break
            except BufferError:
                # The local queue is full; let delivery callbacks drain it.
                producer.poll(0.5)

        published += 1
        if on_progress is not None and published % 100_000 == 0:
            on_progress(published)

    remaining = producer.flush(timeout=120)
    if remaining:
        raise TimeoutError(f"{remaining} messages still unsent after flush")

    return ProduceStats(
        topic=name,
        lines_read=lines_read,
        published=published,
        rejected=rejected,
        elapsed_seconds=time.monotonic() - started,
    )
