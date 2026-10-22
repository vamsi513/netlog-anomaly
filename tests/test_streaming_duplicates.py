"""Re-delivering the same records must not create a second document.

The consumer group is reset to the beginning of the topic and run again over
exactly the same records. That is the shape a crash takes: the offsets a
consumer had not committed are read a second time, and the windows they feed
are rebuilt and written again. The document count may not move, because the
document id is the window start and a rewrite replaces rather than appends.

The version number is checked as well as the count. A count that stayed the
same because the second pass quietly wrote nothing would prove nothing about
idempotency, so the test insists the documents really were written again.
"""

from __future__ import annotations

from streaming_helpers import fetch_streamed, rewind

from netlog_anomaly.streaming.config import Settings
from netlog_anomaly.streaming.consumer import WindowConsumer
from netlog_anomaly.streaming.producer import replay
from netlog_anomaly.streaming.scoring import WindowScorer
from netlog_anomaly.streaming.sink import IndexedWindow, bulk_write, client, ensure_index
from netlog_anomaly.streaming.windows import ClosedWindow

SLICE_LINES = 120_000


def run_consumer(settings: Settings, group_id: str, topic: str, index: str, member: str) -> object:
    consumer = WindowConsumer(
        settings,
        member=member,
        group_id=group_id,
        topic=topic,
        index=index,
        refresh=True,
        verbose=False,
    )
    return consumer.run(idle_timeout=8.0)


def test_replaying_the_same_range_does_not_change_the_document_count(
    stream_settings: Settings,
    stream_topic: str,
    stream_index: str,
    stream_group: str,
    bgl_log: object,
) -> None:
    produced = replay(stream_settings, bgl_log, topic=stream_topic, limit=SLICE_LINES)  # type: ignore[arg-type]

    first = run_consumer(stream_settings, stream_group, stream_topic, stream_index, "first")
    after_first = fetch_streamed(stream_settings, stream_index)
    assert after_first, "the first pass indexed nothing"
    assert first.records == produced.published  # type: ignore[attr-defined]

    partitions = rewind(stream_settings, stream_group, stream_topic)
    assert partitions == 6

    second = run_consumer(stream_settings, stream_group, stream_topic, stream_index, "second")
    after_second = fetch_streamed(stream_settings, stream_index)

    assert second.records == produced.published, (  # type: ignore[attr-defined]
        "the second pass did not read the same range again"
    )
    assert set(after_second) == set(after_first), "the id set changed on re-delivery"
    assert len(after_second) == len(after_first), (
        f"document count moved from {len(after_first)} to {len(after_second)}"
    )

    rewritten = sum(
        1 for start, window in after_second.items() if window.version > after_first[start].version
    )
    changed_counts = {
        start: (after_first[start].event_count, window.event_count)
        for start, window in after_second.items()
        if window.event_count != after_first[start].event_count
    }

    print(
        f"\nduplicate delivery over {produced.published} records: "
        f"{len(after_first)} documents before, {len(after_second)} after, "
        f"{rewritten} rewritten in place"
    )
    assert not changed_counts, f"event counts changed on re-delivery: {changed_counts}"
    assert rewritten == len(after_first), (
        f"only {rewritten} of {len(after_first)} documents were written again; "
        "the second pass cannot have re-indexed the same windows"
    )


def test_a_window_written_twice_keeps_one_document(
    stream_settings: Settings,
    stream_index: str,
) -> None:
    """The sink's own idempotency, without Kafka in the way."""
    os_client = client(stream_settings)
    ensure_index(os_client, stream_index)
    scorer = WindowScorer.from_file(stream_settings.stats_path)
    window = ClosedWindow(
        start=1117838520,
        window_seconds=60,
        event_count=64,
        distinct_nodes=1,
        distinct_templates=1,
        info_count=64,
        warning_count=0,
        error_count=0,
        severe_count=0,
        fatal_count=0,
        failure_count=0,
        error_rate=0.0,
        alert_count=0,
        is_anomalous=False,
        late_events=0,
    )
    record = IndexedWindow(scored=scorer.score(window))

    for _ in range(3):
        assert bulk_write(os_client, stream_index, [record], refresh=True) == 1

    os_client.indices.refresh(index=stream_index)
    assert os_client.count(index=stream_index)["count"] == 1
    stored = os_client.get(index=stream_index, id=window.window_id)
    assert stored["_version"] == 3
    assert stored["_source"]["event_count"] == 64
