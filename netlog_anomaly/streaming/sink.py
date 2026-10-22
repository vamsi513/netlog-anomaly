"""Write scored windows to OpenSearch, idempotently.

The document id is the window start, so the same window always writes to the
same document. A window delivered twice is therefore indexed twice and counted
once: the second write replaces the first with identical content instead of
adding a row. That is what makes the delivery guarantee tolerable.

The guarantee is at-least-once delivery with idempotent writes. It is not
exactly-once. The consumer commits its Kafka offsets only after the bulk write
has succeeded, so a crash between the write and the commit replays the same
records and rewrites the same documents. Nothing in this design prevents the
duplicate delivery; the deterministic id makes the duplicate harmless.

One index holds one window size. The id is the window start alone, so indexing
60 second and 300 second windows into the same index would have them overwrite
each other; the window size is stored on every document and the index name is
configurable, which is how two sizes are kept apart.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from opensearchpy import OpenSearch, helpers

from netlog_anomaly.streaming.config import Settings
from netlog_anomaly.streaming.scoring import ScoredWindow

INDEX_SETTINGS: dict[str, object] = {
    # One shard, no replica: a single node cannot place a replica, and asking
    # for one leaves the index yellow forever.
    "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
    "mappings": {
        # Strict, so a typo in a field name fails the write instead of
        # silently creating a second field nobody queries.
        "dynamic": "strict",
        "properties": {
            "window_start": {"type": "date"},
            "window_seconds": {"type": "integer"},
            "event_count": {"type": "integer"},
            "distinct_nodes": {"type": "integer"},
            "distinct_templates": {"type": "integer"},
            "info_count": {"type": "integer"},
            "warning_count": {"type": "integer"},
            "error_count": {"type": "integer"},
            "severe_count": {"type": "integer"},
            "fatal_count": {"type": "integer"},
            "failure_count": {"type": "integer"},
            "error_rate": {"type": "float"},
            "alert_count": {"type": "integer"},
            "is_anomalous": {"type": "boolean"},
            "late_events": {"type": "integer"},
            "severity_flag": {"type": "boolean"},
            "zscore": {"type": "float"},
            "zscore_flag": {"type": "boolean"},
            "flagged": {"type": "boolean"},
            "produced_at": {"type": "date"},
            "indexed_at": {"type": "date"},
            "ingest_lag_ms": {"type": "long"},
        },
    },
}


@dataclass(frozen=True, slots=True)
class IndexedWindow:
    """A scored window plus when its latest event was published.

    produced_at_ms is the Kafka record timestamp of the newest event in the
    window, which is wall clock time at the producer. Paired with the moment
    the bulk write is issued it gives the end to end delay for that window,
    measured rather than estimated.
    """

    scored: ScoredWindow
    produced_at_ms: int | None = None


def client(settings: Settings) -> OpenSearch:
    """A client for the local stack. No TLS and no credentials; see the docs."""
    return OpenSearch(
        hosts=[settings.opensearch_url],
        http_compress=True,
        timeout=30,
        max_retries=3,
        retry_on_timeout=True,
    )


def ensure_index(os_client: OpenSearch, index: str) -> None:
    """Create the index with its mapping if it does not exist."""
    if not os_client.indices.exists(index=index):
        os_client.indices.create(index=index, body=INDEX_SETTINGS)


def document(record: IndexedWindow, indexed_at: datetime | None = None) -> dict[str, object]:
    """One window as the document body."""
    scored = record.scored
    window = scored.window
    moment = indexed_at or datetime.now(tz=UTC)

    lag: int | None = None
    produced_at: str | None = None
    if record.produced_at_ms is not None:
        produced_at = datetime.fromtimestamp(record.produced_at_ms / 1000, tz=UTC).isoformat()
        lag = int(moment.timestamp() * 1000) - record.produced_at_ms

    body: dict[str, object] = {
        "window_start": window.start_time.isoformat(),
        "window_seconds": window.window_seconds,
        "event_count": window.event_count,
        "distinct_nodes": window.distinct_nodes,
        "distinct_templates": window.distinct_templates,
        "info_count": window.info_count,
        "warning_count": window.warning_count,
        "error_count": window.error_count,
        "severe_count": window.severe_count,
        "fatal_count": window.fatal_count,
        "failure_count": window.failure_count,
        "error_rate": window.error_rate,
        "alert_count": window.alert_count,
        "is_anomalous": window.is_anomalous,
        "late_events": window.late_events,
        "severity_flag": scored.severity_flag,
        "zscore": scored.zscore,
        "zscore_flag": scored.zscore_flag,
        "flagged": scored.flagged,
        "indexed_at": moment.isoformat(),
    }
    if produced_at is not None:
        body["produced_at"] = produced_at
        body["ingest_lag_ms"] = lag
    return body


def actions(index: str, records: list[IndexedWindow]) -> list[dict[str, object]]:
    """Bulk index actions, one per window, keyed on the window start.

    The action is `index`, not `create`: `create` would reject a re-delivered
    window as a conflict, and a conflict the consumer has to ignore is harder
    to reason about than a write that is simply idempotent.
    """
    indexed_at = datetime.now(tz=UTC)
    return [
        {
            "_op_type": "index",
            "_index": index,
            "_id": record.scored.window.window_id,
            "_source": document(record, indexed_at=indexed_at),
        }
        for record in records
    ]


def bulk_write(
    os_client: OpenSearch,
    index: str,
    records: list[IndexedWindow],
    refresh: bool = False,
) -> int:
    """Index every record, or raise. Returns the number written.

    raise_on_error is left at its default so a partial failure propagates: the
    consumer must not commit offsets for a batch that did not land in full.
    """
    if not records:
        return 0
    written, errors = helpers.bulk(
        os_client,
        actions(index, records),
        refresh=refresh,
        stats_only=False,
    )
    if errors:
        raise RuntimeError(f"bulk write reported {len(errors)} failures: {errors[:3]}")
    return int(written)
