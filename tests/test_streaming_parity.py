"""Streamed windows against the windows the batch pipeline stored.

The two paths share the parser and the window arithmetic and share nothing
else: batch aggregates with one SQL GROUP BY over a loaded table, streaming
folds events into per partition state as they arrive and closes each window on
a watermark. Agreeing on every window of a fixed slice is the only evidence
that the second path reproduces the first.
"""

from __future__ import annotations

import psycopg
from streaming_helpers import (
    SOURCE_FILE,
    WINDOW_SECONDS,
    compare,
    complete_windows,
    fetch_batch,
    fetch_streamed,
)

from netlog_anomaly.streaming.config import Settings
from netlog_anomaly.streaming.consumer import WindowConsumer
from netlog_anomaly.streaming.producer import replay

SLICE_LINES = 200_000


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

    complete, partial = complete_windows(batch_conn, streamed, SLICE_LINES)
    missing, unexpected, mismatches = compare(streamed, complete)

    print(
        f"\nparity over the first {SLICE_LINES} lines of {SOURCE_FILE}: "
        f"{len(complete)} complete windows compared, {len(mismatches)} mismatches, "
        f"{len(missing)} missing, {len(unexpected)} unexpected, "
        f"{len(partial)} partial windows excluded, {consumed.late_events} late events"
    )
    assert not missing, f"{len(missing)} complete batch windows were never streamed: {missing[:5]}"
    assert set(unexpected) <= set(partial), (
        f"streamed windows that are neither complete nor partial: "
        f"{sorted(set(unexpected) - set(partial))[:5]}"
    )
    assert not mismatches, "\n".join(mismatches)
    assert len(complete) > 100, f"only {len(complete)} windows compared; the slice is too small"


def test_only_the_cut_window_is_incomplete(
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
