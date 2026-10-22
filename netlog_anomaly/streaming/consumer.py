"""One member of the consumer group: aggregate, score, index, then commit.

Each consumer keeps a separate WindowAggregator per assigned partition. One
aggregator for all of them would share a watermark, and a partition racing
ahead would then close another partition's windows before their events had
arrived. Per partition state also makes a rebalance clean: the state for a
revoked partition is dropped, and the member that takes the partition over
rebuilds it from the log.

Offsets are committed only after the bulk write for that batch has succeeded,
and never past a record whose window is still open. Those two rules together
are what the failure test checks:

* Commit after write, so a crash between the two replays the records rather
  than skipping them. The replayed windows rewrite the same documents, because
  the document id is the window start.
* Never commit past an open window, so a crash cannot leave a window that no
  longer exists in memory and is no longer in anyone's replay range. The safe
  commit point for a partition is the lowest offset still feeding an open
  window, or the next unread offset when nothing is open.

Nothing is committed when a partition is revoked. The new owner re-reads from
the last commit, rebuilds those windows and rewrites their documents. That
costs a little duplicated work and guarantees no gap, which is the trade this
design is making everywhere: at-least-once delivery with idempotent writes.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from netlog_anomaly.parser import ParseError, parse_line
from netlog_anomaly.streaming.config import Settings
from netlog_anomaly.streaming.scoring import WindowScorer
from netlog_anomaly.streaming.sink import IndexedWindow, bulk_write, client, ensure_index
from netlog_anomaly.streaming.windows import WindowAggregator, window_start

LATE_EVENTS_LOGGED = 20

# A commit rejected for one of these reasons means the group moved on while the
# batch was being processed: the member's generation is stale, or a rebalance is
# under way. The offsets simply were not accepted, so the records stay unread
# for whoever owns the partition next. Nothing is lost and the windows they feed
# are rebuilt and rewritten over the same document ids, so the run continues.
REBALANCE_COMMIT_ERRORS = frozenset(
    {
        KafkaError.ILLEGAL_GENERATION,
        KafkaError.REBALANCE_IN_PROGRESS,
        KafkaError.UNKNOWN_MEMBER_ID,
    }
)


def consumer_config(settings: Settings, group_id: str | None = None) -> dict[str, Any]:
    """Manual commits, cooperative rebalancing, read from the beginning.

    enable.auto.commit is off because an automatic commit on a timer would
    commit offsets for records whose windows had not been written yet, which is
    exactly the window loss this design is built to avoid.
    """
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": group_id or settings.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        # Cooperative rebalancing keeps the partitions a member is already
        # aggregating instead of revoking everything on every join, so a
        # member joining or leaving disturbs the least state it can.
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms": 10_000,
        "heartbeat.interval.ms": 3_000,
        "max.poll.interval.ms": 300_000,
    }


@dataclass(slots=True)
class PartitionState:
    """Per partition window state and the offsets feeding it."""

    aggregator: WindowAggregator
    # Lowest offset that contributed to each still-open window.
    first_offset: dict[int, int] = field(default_factory=dict)
    # Newest record timestamp seen for each still-open window, in epoch ms.
    produced_at: dict[int, int] = field(default_factory=dict)
    next_offset: int = -1

    def safe_commit_offset(self) -> int | None:
        """The offset it is safe to commit for this partition.

        The lowest offset still feeding an open window, because a crash must
        replay that window's events; or the next unread offset when every
        window the partition has produced is written.
        """
        if self.first_offset:
            return min(self.first_offset.values())
        if self.next_offset < 0:
            return None
        return self.next_offset


@dataclass(frozen=True, slots=True)
class ConsumeStats:
    """What one consumer run did. Printed as JSON so a test can read it."""

    member: str
    records: int
    parse_rejected: int
    windows_written: int
    bulk_writes: int
    late_events: int
    commit_conflicts: int
    elapsed_seconds: float
    processing_seconds: float
    # Wall clock, so several members on one machine can be placed on a common
    # timeline; monotonic clocks cannot be compared across processes.
    first_record_at: float | None
    last_work_at: float | None
    partitions_seen: tuple[int, ...]

    @property
    def events_per_second(self) -> float:
        """Records per second over the time records were actually arriving.

        elapsed_seconds includes the rebalance before the first record and the
        idle wait that ends a bounded replay, neither of which is work: using
        it would understate throughput by whatever the idle timeout happened to
        be set to. processing_seconds spans the first record to the last batch
        that had records in it, plus the final drain, and so counts every fetch
        wait in between but not the wait that proved the replay was over.
        """
        if self.processing_seconds <= 0:
            return 0.0
        return self.records / self.processing_seconds

    def as_json(self) -> str:
        return json.dumps(
            {
                "member": self.member,
                "records": self.records,
                "parse_rejected": self.parse_rejected,
                "windows_written": self.windows_written,
                "bulk_writes": self.bulk_writes,
                "late_events": self.late_events,
                "commit_conflicts": self.commit_conflicts,
                "elapsed_seconds": round(self.elapsed_seconds, 4),
                "processing_seconds": round(self.processing_seconds, 4),
                "first_record_at": self.first_record_at,
                "last_work_at": self.last_work_at,
                "events_per_second": round(self.events_per_second, 1),
                "partitions_seen": list(self.partitions_seen),
            }
        )


class WindowConsumer:
    """Consumes raw log records and indexes the windows they complete."""

    def __init__(
        self,
        settings: Settings,
        member: str = "consumer",
        group_id: str | None = None,
        topic: str | None = None,
        index: str | None = None,
        refresh: bool = False,
        verbose: bool = True,
    ) -> None:
        self.settings = settings
        self.member = member
        self.topic = topic or settings.topic
        self.index = index or settings.index
        self.refresh = refresh
        self.verbose = verbose

        self.scorer = WindowScorer.from_file(settings.stats_path)
        self.os_client = client(settings)
        self.consumer = Consumer(consumer_config(settings, group_id))

        self._states: dict[int, PartitionState] = {}
        self._records = 0
        self._parse_rejected = 0
        self._windows_written = 0
        self._bulk_writes = 0
        self._late_events = 0
        self._late_logged = 0
        self._commit_conflicts = 0
        self._partitions_seen: set[int] = set()
        self._first_record_at: float | None = None
        self._last_work_at: float | None = None
        self._first_record_wall: float | None = None
        self._last_work_wall: float | None = None
        self._drain_seconds = 0.0

    # Rebalance -------------------------------------------------------------

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        consumer.incremental_assign(partitions)
        if self.verbose and partitions:
            assigned = sorted(tp.partition for tp in partitions)
            self._log(f"assigned partitions {assigned}")

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Drop the state for revoked partitions without committing.

        Their open windows have not been written, so the offsets that feed
        them must stay uncommitted for the next owner to re-read.
        """
        for tp in partitions:
            state = self._states.pop(tp.partition, None)
            if state is not None and self.verbose:
                self._log(
                    f"revoked partition {tp.partition}, dropping "
                    f"{state.aggregator.open_windows} open windows for replay"
                )
        consumer.incremental_unassign(partitions)

    # Main loop -------------------------------------------------------------

    def run(self, idle_timeout: float = 10.0, max_runtime: float = 1800.0) -> ConsumeStats:
        """Consume until the topic goes quiet, then close the open windows.

        idle_timeout ends a bounded replay: once nothing has arrived for that
        long, every remaining window is closed and written. A consumer meant to
        run indefinitely would never do this, because an idle topic is not
        evidence that a window is complete; a finished replay is.
        """
        ensure_index(self.os_client, self.index)
        self.consumer.subscribe([self.topic], on_assign=self._on_assign, on_revoke=self._on_revoke)

        started = time.monotonic()
        last_record = time.monotonic()
        try:
            while True:
                now = time.monotonic()
                if now - started > max_runtime:
                    self._log(f"stopping after {max_runtime:.0f}s of runtime")
                    break

                batch = self.consumer.consume(num_messages=5000, timeout=1.0)
                healthy = [message for message in batch if not self._failed(message)]

                if healthy:
                    last_record = time.monotonic()
                    if self._first_record_at is None:
                        self._first_record_at = last_record
                        self._first_record_wall = time.time()
                    for message in healthy:
                        self._absorb(message)
                    self._flush()
                    self._last_work_at = time.monotonic()
                    self._last_work_wall = time.time()
                elif time.monotonic() - last_record > idle_timeout:
                    break

            self._drain()
        finally:
            # close() leaves the group, so the remaining members rebalance
            # instead of waiting for this one's session to time out.
            self.consumer.close()

        return ConsumeStats(
            member=self.member,
            records=self._records,
            parse_rejected=self._parse_rejected,
            windows_written=self._windows_written,
            bulk_writes=self._bulk_writes,
            late_events=self._late_events,
            commit_conflicts=self._commit_conflicts,
            elapsed_seconds=time.monotonic() - started,
            processing_seconds=self._processing_seconds(),
            first_record_at=self._first_record_wall,
            last_work_at=self._last_work_wall,
            partitions_seen=tuple(sorted(self._partitions_seen)),
        )

    def _processing_seconds(self) -> float:
        if self._first_record_at is None or self._last_work_at is None:
            return 0.0
        return max(self._last_work_at - self._first_record_at, 0.0) + self._drain_seconds

    # Internals -------------------------------------------------------------

    def _failed(self, message: Any) -> bool:
        error = message.error()
        if error is None:
            return False
        if error.code() == KafkaError._PARTITION_EOF:
            return True
        raise KafkaException(error)

    def _absorb(self, message: Any) -> None:
        partition = message.partition()
        self._partitions_seen.add(partition)
        state = self._states.get(partition)
        if state is None:
            state = PartitionState(
                aggregator=WindowAggregator(
                    window_seconds=self.settings.window_seconds,
                    allowed_lateness=self.settings.allowed_lateness,
                )
            )
            self._states[partition] = state

        offset = message.offset()
        state.next_offset = offset + 1
        self._records += 1

        try:
            event = parse_line(message.value().decode("utf-8", errors="replace"), offset)
        except ParseError:
            # The producer filters these out, so one arriving here means the
            # topic was written by something else. Count it, do not stop.
            self._parse_rejected += 1
            return

        late = state.aggregator.add(event)
        if late is not None:
            self._late_events += 1
            if self.verbose and self._late_logged < LATE_EVENTS_LOGGED:
                self._late_logged += 1
                self._log(
                    f"late event at {late.epoch} for window {late.window_start} "
                    f"behind watermark {late.watermark}"
                )
            return

        start = window_start(event.epoch, self.settings.window_seconds)
        if start not in state.first_offset or offset < state.first_offset[start]:
            state.first_offset[start] = offset

        _, timestamp = message.timestamp()
        if timestamp is not None and timestamp > state.produced_at.get(start, -1):
            state.produced_at[start] = timestamp

    def _collect(self, drain: bool) -> list[IndexedWindow]:
        records: list[IndexedWindow] = []
        for state in self._states.values():
            closed = state.aggregator.drain() if drain else state.aggregator.ready()
            for window in closed:
                produced_at = state.produced_at.pop(window.start, None)
                state.first_offset.pop(window.start, None)
                records.append(
                    IndexedWindow(
                        scored=self.scorer.score(window),
                        produced_at_ms=produced_at,
                    )
                )
        return records

    def _commit_safe(self) -> None:
        offsets = [
            TopicPartition(self.topic, partition, offset)
            for partition, state in self._states.items()
            if (offset := state.safe_commit_offset()) is not None
        ]
        if not offsets:
            return
        try:
            self.consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as exc:
            if exc.args[0].code() not in REBALANCE_COMMIT_ERRORS:
                raise
            self._commit_conflicts += 1
            self._log(f"commit rejected during a rebalance: {exc.args[0].name()}")

    def _flush(self, drain: bool = False) -> None:
        """Write the windows this batch completed, then commit.

        The order is the whole point: if the write raises, nothing is
        committed and the records are read again.
        """
        records = self._collect(drain=drain)
        if records:
            written = bulk_write(self.os_client, self.index, records, refresh=self.refresh)
            self._windows_written += written
            self._bulk_writes += 1
        self._commit_safe()

    def _drain(self) -> None:
        started = time.monotonic()
        self._flush(drain=True)
        # Counted on its own rather than by moving _last_work_at forward: that
        # would swallow the idle wait that ran between the two.
        self._drain_seconds = time.monotonic() - started
        if self._first_record_wall is not None:
            self._last_work_wall = time.time()
        if self._late_events:
            self._log(f"{self._late_events} late events, none folded into a written window")

    def _log(self, text: str) -> None:
        if self.verbose:
            print(f"[{self.member}] {text}", flush=True)
