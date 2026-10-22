"""Helpers shared by the streaming integration tests.

Reading every document out of an index and asking the batch tables what a slice
should contain are needed by the parity, failure and duplicate tests alike, and
three copies of either would drift.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import psycopg
from confluent_kafka import Consumer, TopicPartition

from netlog_anomaly.streaming.config import Settings
from netlog_anomaly.streaming.consumer import consumer_config
from netlog_anomaly.streaming.sink import client

WINDOW_SECONDS = 60
SOURCE_FILE = "BGL.log"


@dataclass(frozen=True, slots=True)
class BatchWindow:
    """One window as the batch tables have it."""

    event_count: int
    is_anomalous: bool
    last_line: int


@dataclass(frozen=True, slots=True)
class StreamedWindow:
    """One window as the index has it."""

    version: int
    event_count: int
    is_anomalous: bool
    late_events: int


def fetch_streamed(settings: Settings, index: str) -> dict[int, StreamedWindow]:
    """Every document in the index, keyed by its window start epoch.

    The key comes from the document id, which is the window start: the id alone
    places every document on the window grid.
    """
    os_client = client(settings)
    os_client.indices.refresh(index=index)
    found: dict[int, StreamedWindow] = {}
    response = os_client.search(
        index=index,
        body={"query": {"match_all": {}}, "sort": ["_doc"], "version": True},
        size=1000,
        scroll="2m",
    )
    scroll_id = response.get("_scroll_id")
    try:
        while True:
            hits = response["hits"]["hits"]
            if not hits:
                break
            for hit in hits:
                start = int(datetime.fromisoformat(hit["_id"]).timestamp())
                source = hit["_source"]
                found[start] = StreamedWindow(
                    version=int(hit["_version"]),
                    event_count=int(source["event_count"]),
                    is_anomalous=bool(source["is_anomalous"]),
                    late_events=int(source["late_events"]),
                )
            response = os_client.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = response.get("_scroll_id")
    finally:
        if scroll_id:
            os_client.clear_scroll(scroll_id=scroll_id)
    return found


def fetch_batch(conn: psycopg.Connection, lo_epoch: int, hi_epoch: int) -> dict[int, BatchWindow]:
    """The batch windows over an epoch range, with the last line each contains.

    event_count and is_anomalous come from window_features, the table the batch
    pipeline writes. last_line comes from log_events and is used only to decide
    which windows a line-numbered slice completed.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH sliced AS (
                SELECT div(epoch, %(w)s) * %(w)s AS ws, max(line_no) AS last_line
                  FROM log_events
                 WHERE source_file = %(file)s
                   AND epoch >= %(lo)s AND epoch < %(hi)s
                 GROUP BY 1
            )
            SELECT s.ws, w.event_count, w.is_anomalous, s.last_line
              FROM sliced s
              JOIN window_features w
                ON w.window_seconds = %(w)s
               AND w.window_start = to_timestamp(s.ws)
             ORDER BY s.ws
            """,
            {"w": WINDOW_SECONDS, "file": SOURCE_FILE, "lo": lo_epoch, "hi": hi_epoch},
        )
        return {
            int(ws): BatchWindow(
                event_count=int(event_count),
                is_anomalous=bool(is_anomalous),
                last_line=int(last_line),
            )
            for ws, event_count, is_anomalous, last_line in cur.fetchall()
        }


def complete_windows(
    conn: psycopg.Connection, streamed: dict[int, StreamedWindow], slice_lines: int
) -> tuple[dict[int, BatchWindow], list[int]]:
    """Split the batch windows touching a slice into complete and partial.

    A window is complete when no line of the file outside the slice falls
    inside it. A slice cut at a line number always ends mid window, and that
    window's batch row counts the whole file, so comparing it would compare
    two different inputs.
    """
    starts = sorted(streamed)
    batch = fetch_batch(conn, starts[0], starts[-1] + WINDOW_SECONDS)
    complete = {ws: window for ws, window in batch.items() if window.last_line <= slice_lines}
    partial = sorted(set(batch) - set(complete))
    return complete, partial


def compare(
    streamed: dict[int, StreamedWindow], complete: dict[int, BatchWindow]
) -> tuple[list[int], list[int], list[str]]:
    """Missing windows, unexpected windows and per-field mismatches."""
    missing = sorted(set(complete) - set(streamed))
    unexpected = sorted(set(streamed) - set(complete))
    mismatches: list[str] = []
    for ws in sorted(set(complete) & set(streamed)):
        expected, actual = complete[ws], streamed[ws]
        if actual.event_count != expected.event_count:
            mismatches.append(
                f"{datetime.fromtimestamp(ws).isoformat()} event_count "
                f"stream={actual.event_count} batch={expected.event_count}"
            )
        if actual.is_anomalous != expected.is_anomalous:
            mismatches.append(
                f"{datetime.fromtimestamp(ws).isoformat()} is_anomalous "
                f"stream={actual.is_anomalous} batch={expected.is_anomalous}"
            )
    return missing, unexpected, mismatches


def rewind(settings: Settings, group_id: str, topic: str) -> int:
    """Commit offset 0 for every partition of the topic, as that group.

    Done through a consumer in the group rather than by deleting the group, so
    the next run is the same group reading the same range again.
    """
    consumer = Consumer(consumer_config(settings, group_id))
    try:
        metadata = consumer.list_topics(topic, timeout=30).topics[topic]
        partitions = [TopicPartition(topic, p, 0) for p in sorted(metadata.partitions)]
        consumer.commit(offsets=partitions, asynchronous=False)
    finally:
        consumer.close()
    return len(partitions)


def duplicate_window_starts(settings: Settings, index: str) -> list[tuple[str, int]]:
    """Window starts that appear in more than one document.

    Asked of OpenSearch as an aggregation over the field rather than inferred
    from the document ids. The ids make duplicates impossible by construction,
    which is exactly the claim under test, so the test must not assume it.
    """
    os_client = client(settings)
    os_client.indices.refresh(index=index)
    response = os_client.search(
        index=index,
        body={
            "size": 0,
            "aggs": {
                "starts": {
                    "terms": {"field": "window_start", "size": 50_000, "min_doc_count": 2},
                }
            },
        },
    )
    return [
        (bucket["key_as_string"], int(bucket["doc_count"]))
        for bucket in response["aggregations"]["starts"]["buckets"]
    ]
