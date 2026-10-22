"""Streamed windows against the windows the batch pipeline stored.

The two paths share the parser and the window arithmetic and share nothing
else: batch aggregates with one SQL GROUP BY over a loaded table, streaming
folds events into per partition state as they arrive and closes each window on
a watermark. Agreeing on every window of a fixed slice is the only evidence
that the second path reproduces the first.

The comparison is restricted to the windows the slice completes. A slice cut at
a line number ends in the middle of a window, and the batch table counts that
window over the whole file, so comparing it would be comparing different
inputs. A window is complete when no line of the file outside the slice falls
inside it, which is asked of the database rather than assumed from the log
being in time order.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import psycopg

from netlog_anomaly.streaming.config import Settings
from netlog_anomaly.streaming.consumer import WindowConsumer
from netlog_anomaly.streaming.producer import replay
from netlog_anomaly.streaming.sink import client

SLICE_LINES = 200_000
WINDOW_SECONDS = 60
SOURCE_FILE = "BGL.log"


@dataclass(frozen=True, slots=True)
class BatchWindow:
    """One window as the batch tables have it."""

    event_count: int
    is_anomalous: bool
    last_line: int


def fetch_streamed(settings: Settings, index: str) -> dict[str, dict[str, object]]:
    """Every document in the index, keyed by document id."""
    os_client = client(settings)
    os_client.indices.refresh(index=index)
    documents: dict[str, dict[str, object]] = {}
    response = os_client.search(
        index=index, body={"query": {"match_all": {}}, "sort": ["_doc"]}, size=1000, scroll="2m"
    )
    scroll_id = response.get("_scroll_id")
    try:
        while True:
            hits = response["hits"]["hits"]
            if not hits:
                break
            for hit in hits:
                documents[hit["_id"]] = hit["_source"]
            response = os_client.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = response.get("_scroll_id")
    finally:
        if scroll_id:
            os_client.clear_scroll(scroll_id=scroll_id)
    return documents


def fetch_batch(conn: psycopg.Connection, lo_epoch: int, hi_epoch: int) -> dict[int, BatchWindow]:
    """The batch windows over an epoch range, with the last line each contains.

    event_count and is_anomalous come from window_features, the table the batch
    pipeline writes. last_line comes from log_events and is only used to decide
    which windows the slice completed.
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


def test_streamed_windows_match_the_batch_windows(
    stream_settings: Settings,
    stream_topic: str,
    stream_index: str,
    stream_group: str,
    bgl_log: object,
    batch_conn: psycopg.Connection,
) -> None:
    produced = replay(stream_settings, bgl_log, topic=stream_topic, limit=SLICE_LINES)  # type: ignore[arg-type]
    assert produced.published > 0

    consumer = WindowConsumer(
        stream_settings,
        member="parity",
        group_id=stream_group,
        topic=stream_topic,
        index=stream_index,
        refresh=True,
        verbose=False,
    )
    consumed = consumer.run(idle_timeout=8.0)
    assert consumed.records == produced.published, (
        f"produced {produced.published} records but consumed {consumed.records}"
    )

    streamed = fetch_streamed(stream_settings, stream_index)
    assert len(streamed) == consumed.windows_written

    # The document id is the window start, so the keys alone place every
    # streamed window on the grid.
    streamed_by_start = {
        int(datetime.fromisoformat(document_id).timestamp()): source
        for document_id, source in streamed.items()
    }
    starts = sorted(streamed_by_start)
    batch = fetch_batch(batch_conn, starts[0], starts[-1] + WINDOW_SECONDS)

    complete = {ws: window for ws, window in batch.items() if window.last_line <= SLICE_LINES}
    partial = sorted(set(batch) - set(complete))

    # Every complete window must have been streamed, and no streamed window
    # may be missing from the batch tables.
    missing = sorted(set(complete) - set(streamed_by_start))
    unexpected = sorted(set(streamed_by_start) - set(batch))
    assert not missing, f"{len(missing)} complete batch windows were never streamed: {missing[:5]}"
    assert not unexpected, f"{len(unexpected)} streamed windows are not in the batch tables"

    mismatches: list[str] = []
    for ws in sorted(complete):
        document = streamed_by_start[ws]
        expected = complete[ws]
        if int(document["event_count"]) != expected.event_count:
            mismatches.append(
                f"{document['window_start']} event_count "
                f"stream={document['event_count']} batch={expected.event_count}"
            )
        if bool(document["is_anomalous"]) != expected.is_anomalous:
            mismatches.append(
                f"{document['window_start']} is_anomalous "
                f"stream={document['is_anomalous']} batch={expected.is_anomalous}"
            )

    print(
        f"\nparity over the first {SLICE_LINES} lines of {SOURCE_FILE}: "
        f"{len(complete)} complete windows compared, {len(mismatches)} mismatches, "
        f"{len(partial)} partial windows excluded, "
        f"{consumed.late_events} late events"
    )
    assert not mismatches, "\n".join(mismatches)
    assert len(complete) > 100, f"only {len(complete)} windows compared; the slice is too small"


def test_only_the_cut_window_is_incomplete(
    stream_settings: Settings,
    bgl_log: object,
    batch_conn: psycopg.Connection,
) -> None:
    """How much of the slice the parity test has to leave out.

    If this ever started excluding a large share of the slice, the parity test
    would still pass while comparing far less than it appears to, so the amount
    excluded is asserted rather than printed and forgotten.
    """
    with batch_conn.cursor() as cur:
        cur.execute(
            "SELECT min(epoch), max(epoch) FROM log_events "
            "WHERE source_file = %s AND line_no <= %s",
            (SOURCE_FILE, SLICE_LINES),
        )
        row = cur.fetchone()
    assert row is not None and row[0] is not None
    lo = int(row[0]) // WINDOW_SECONDS * WINDOW_SECONDS
    hi = int(row[1]) + WINDOW_SECONDS

    batch = fetch_batch(batch_conn, lo, hi)
    partial = [ws for ws, window in batch.items() if window.last_line > SLICE_LINES]
    print(
        f"\n{len(batch)} windows touch the slice, {len(partial)} of them extend past line "
        f"{SLICE_LINES}"
    )
    assert len(partial) <= 2, f"{len(partial)} windows extend past the slice: {sorted(partial)[:5]}"
