"""Tumbling event-time windows, a watermark, and an explicit late-event policy.

Window assignment is the same arithmetic the batch pipeline uses in
aggregate_windows.sql::

    to_timestamp(div(epoch, window_seconds) * window_seconds)

so a window built here starts on the same instant as the batch window with the
same contents. test_streaming_windows.py checks that against the SQL rather
than taking it on trust.

Closing a window is where streaming differs from batch. Events arrive in
roughly, not exactly, event-time order, so a window cannot be emitted the
moment an event from the next one appears. The watermark is the mechanism:

    watermark = highest event time seen - allowed_lateness

and a window [start, start + window_seconds) closes once the watermark reaches
its end. allowed_lateness buys tolerance for out-of-order arrival at the cost
of delaying every window by that much.

An event whose window has already closed cannot be folded into the result that
was already emitted. Those events are counted, and the count is reported with
the run, because a silently dropped event is an undetectable wrong answer.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime

from netlog_anomaly.parser import LogEvent

WINDOW_SECONDS = 60
ALLOWED_LATENESS_SECONDS = 0

# The severities the batch aggregation counts into error_rate. WARNING is not
# one of them; aggregate_windows.sql says the same thing in SQL.
ERROR_SEVERITIES = frozenset({"ERROR", "SEVERE", "FATAL", "FAILURE"})


def window_start(epoch: int, window_seconds: int = WINDOW_SECONDS) -> int:
    """Floor an event time onto the window grid.

    Integer floor division, matching Postgres div() for non-negative epochs.
    The parser already rejects epochs outside [2000-01-01, 2050-01-01], so a
    negative value cannot reach here.
    """
    if window_seconds <= 0:
        raise ValueError(f"window_seconds must be positive, got {window_seconds}")
    return epoch // window_seconds * window_seconds


def window_id(start_epoch: int) -> str:
    """The document id for a window: its start instant in UTC, ISO-8601.

    Derived from the window start alone, so the same window always produces the
    same id however many times it is delivered.
    """
    return datetime.fromtimestamp(start_epoch, tz=UTC).isoformat()


@dataclass(slots=True)
class WindowState:
    """Accumulated state for one open window.

    The distinct-value sets are held in memory for as long as the window is
    open. A 60 second BGL window holds a few hundred nodes and a few dozen
    templates, so the sets are small; a window size or a log where they were
    not would need a sketch instead.
    """

    start: int
    event_count: int = 0
    nodes: set[str] = field(default_factory=set)
    templates: set[str] = field(default_factory=set)
    severity_counts: Counter[str] = field(default_factory=Counter)
    alert_count: int = 0

    def add(self, event: LogEvent) -> None:
        self.event_count += 1
        self.nodes.add(event.node)
        self.templates.add(event.template_id)
        self.severity_counts[event.severity] += 1
        if event.is_alert:
            self.alert_count += 1

    def close(self, window_seconds: int, late_events: int) -> ClosedWindow:
        error_like = sum(self.severity_counts[name] for name in ERROR_SEVERITIES)
        return ClosedWindow(
            start=self.start,
            window_seconds=window_seconds,
            event_count=self.event_count,
            distinct_nodes=len(self.nodes),
            distinct_templates=len(self.templates),
            info_count=self.severity_counts["INFO"],
            warning_count=self.severity_counts["WARNING"],
            error_count=self.severity_counts["ERROR"],
            severe_count=self.severity_counts["SEVERE"],
            fatal_count=self.severity_counts["FATAL"],
            failure_count=self.severity_counts["FAILURE"],
            error_rate=error_like / self.event_count,
            alert_count=self.alert_count,
            is_anomalous=self.alert_count > 0,
            late_events=late_events,
        )


@dataclass(frozen=True, slots=True)
class ClosedWindow:
    """A finished window, in the same columns the batch pipeline stores.

    is_anomalous is the dataset's own alert label aggregated over the window,
    exactly as bool_or(is_alert) does in batch. It is a label, not a detector
    output, and nothing in the scoring path reads it.
    """

    start: int
    window_seconds: int
    event_count: int
    distinct_nodes: int
    distinct_templates: int
    info_count: int
    warning_count: int
    error_count: int
    severe_count: int
    fatal_count: int
    failure_count: int
    error_rate: float
    alert_count: int
    is_anomalous: bool
    late_events: int

    @property
    def window_id(self) -> str:
        return window_id(self.start)

    @property
    def start_time(self) -> datetime:
        return datetime.fromtimestamp(self.start, tz=UTC)


@dataclass(frozen=True, slots=True)
class LateEvent:
    """An event that arrived after its window had already been emitted."""

    epoch: int
    window_start: int
    watermark: int


class WindowAggregator:
    """Assigns events to tumbling windows and closes them on the watermark.

    One aggregator instance owns the windows for one set of partitions. The
    watermark is taken over everything the instance has seen, which is sound
    because the producer keys every event of a window onto one partition: a
    window cannot be split across two aggregators, so no aggregator needs to
    wait on another one's progress.
    """

    def __init__(
        self,
        window_seconds: int = WINDOW_SECONDS,
        allowed_lateness: int = ALLOWED_LATENESS_SECONDS,
    ) -> None:
        if window_seconds <= 0:
            raise ValueError(f"window_seconds must be positive, got {window_seconds}")
        if allowed_lateness < 0:
            raise ValueError(f"allowed_lateness must not be negative, got {allowed_lateness}")
        self.window_seconds = window_seconds
        self.allowed_lateness = allowed_lateness
        self._open: dict[int, WindowState] = {}
        self._max_event_time: int | None = None
        # Exclusive upper bound of the windows already emitted. An event
        # landing below it belongs to a result that has left this process.
        self._closed_through = 0
        self._late_total = 0
        self._late_by_window: Counter[int] = Counter()

    @property
    def watermark(self) -> int | None:
        """How far event time has provably advanced, or None before any event."""
        if self._max_event_time is None:
            return None
        return self._max_event_time - self.allowed_lateness

    @property
    def open_windows(self) -> int:
        return len(self._open)

    @property
    def late_events(self) -> int:
        return self._late_total

    @property
    def late_events_by_window(self) -> dict[int, int]:
        """Late event counts keyed by the window start they belonged to."""
        return dict(self._late_by_window)

    def add(self, event: LogEvent) -> LateEvent | None:
        """Fold an event into its window, or report it as late.

        Returns None when the event was accepted, and a LateEvent describing it
        when its window had already been emitted.
        """
        start = window_start(event.epoch, self.window_seconds)

        if self._max_event_time is None or event.epoch > self._max_event_time:
            self._max_event_time = event.epoch

        if start < self._closed_through:
            self._late_total += 1
            self._late_by_window[start] += 1
            watermark = self.watermark
            assert watermark is not None
            return LateEvent(epoch=event.epoch, window_start=start, watermark=watermark)

        state = self._open.get(start)
        if state is None:
            state = WindowState(start=start)
            self._open[start] = state
        state.add(event)
        return None

    def ready(self) -> list[ClosedWindow]:
        """Close and return every window the watermark has passed.

        Emitted in window order, so a consumer writing them downstream writes
        them in the order event time produced them.
        """
        watermark = self.watermark
        if watermark is None:
            return []

        due = sorted(start for start in self._open if start + self.window_seconds <= watermark)
        if not due:
            return []

        # Everything strictly below the end of the highest window just closed
        # is now off limits, including windows that never opened because no
        # event arrived for them.
        self._closed_through = max(self._closed_through, due[-1] + self.window_seconds)
        return [self._pop(start) for start in due]

    def drain(self) -> list[ClosedWindow]:
        """Close every remaining open window, watermark or not.

        Only correct at the end of a bounded replay, where no further event can
        arrive. A long running consumer never calls this: it would emit windows
        that are still filling.
        """
        due = sorted(self._open)
        if due:
            self._closed_through = max(self._closed_through, due[-1] + self.window_seconds)
        return [self._pop(start) for start in due]

    def _pop(self, start: int) -> ClosedWindow:
        state = self._open.pop(start)
        return state.close(self.window_seconds, self._late_by_window.get(start, 0))
