"""Window assignment, watermark advance and the late-event policy.

These are the parts of streaming mode with no service behind them, so they are
tested on their own. One test does reach for Postgres: the claim that the
Python window assignment agrees with the SQL the batch pipeline uses is only
worth anything if it is checked against that SQL.
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg
import pytest

from netlog_anomaly.parser import LogEvent, parse_line
from netlog_anomaly.streaming.windows import (
    WindowAggregator,
    window_id,
    window_start,
)

# The parser rejects epochs outside a plausible range, so the offsets these
# tests reason about are added to a real BGL instant that falls on a window
# boundary: 2005-06-03T22:40:00Z, divisible by 60.
BASE = 1117838400


def bgl_line(
    epoch: int,
    node: str = "R02-M1-N0-C:J12-U11",
    severity: str = "INFO",
    label: str = "-",
    message: str = "generic kernel message",
) -> str:
    """A syntactically valid BGL line at a given event time.

    Built so the tests go through the real parser rather than constructing
    LogEvent by hand; a window test that bypassed parsing could pass on fields
    the parser would never produce.
    """
    moment = datetime.fromtimestamp(epoch, tz=UTC)
    date = moment.strftime("%Y.%m.%d")
    stamp = moment.strftime("%Y-%m-%d-%H.%M.%S.%f")
    return f"{label} {epoch} {date} {node} {stamp} {node} RAS KERNEL {severity} {message}"


def event(epoch: int, line_no: int = 1, **kwargs: str) -> LogEvent:
    return parse_line(bgl_line(epoch, **kwargs), line_no)


class TestWindowStart:
    def test_floors_onto_the_grid(self) -> None:
        assert window_start(1117838570, 60) == 1117838520
        assert window_start(1117838520, 60) == 1117838520

    def test_last_second_of_a_window_stays_in_it(self) -> None:
        assert window_start(1117838579, 60) == 1117838520
        assert window_start(1117838580, 60) == 1117838580

    def test_window_size_changes_the_grid(self) -> None:
        assert window_start(1117838570, 300) == 1117838400
        assert window_start(1117838570, 1) == 1117838570

    def test_rejects_a_non_positive_window(self) -> None:
        with pytest.raises(ValueError, match="must be positive"):
            window_start(1117838570, 0)

    def test_matches_the_batch_sql(self, conn: psycopg.Connection) -> None:
        """The same floor as aggregate_windows.sql, checked against Postgres.

        If these ever disagreed, the parity test would fail with every window
        off by an offset and no indication of why, so the arithmetic is pinned
        here directly.
        """
        epochs = [
            1117838570,
            1117838520,
            1117838579,
            1117838580,
            1136376000,
            946684800,
            2524607999,
        ]
        for window_seconds in (60, 300):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT e, div(e, %(w)s) * %(w)s FROM unnest(%(epochs)s::bigint[]) AS e",
                    {"w": window_seconds, "epochs": epochs},
                )
                rows = cur.fetchall()
            assert len(rows) == len(epochs)
            for epoch, sql_start in rows:
                assert window_start(int(epoch), window_seconds) == int(sql_start)


class TestWindowId:
    def test_is_the_start_instant_in_utc(self) -> None:
        assert window_id(1117838520) == "2005-06-03T22:42:00+00:00"

    def test_is_stable_for_the_same_window(self) -> None:
        assert window_id(window_start(1117838570)) == window_id(window_start(1117838579))


class TestWatermark:
    def test_is_undefined_before_the_first_event(self) -> None:
        aggregator = WindowAggregator()
        assert aggregator.watermark is None
        assert aggregator.ready() == []

    def test_tracks_the_highest_event_time(self) -> None:
        aggregator = WindowAggregator(allowed_lateness=0)
        aggregator.add(event(BASE + 1000))
        assert aggregator.watermark == BASE + 1000
        aggregator.add(event(BASE + 1200))
        assert aggregator.watermark == BASE + 1200

    def test_does_not_go_backwards_on_an_out_of_order_event(self) -> None:
        aggregator = WindowAggregator(allowed_lateness=0)
        aggregator.add(event(BASE + 1200))
        aggregator.add(event(BASE + 1000))
        assert aggregator.watermark == BASE + 1200

    def test_allowed_lateness_holds_it_back(self) -> None:
        aggregator = WindowAggregator(allowed_lateness=120)
        aggregator.add(event(BASE + 1000))
        assert aggregator.watermark == BASE + 880

    def test_rejects_negative_lateness(self) -> None:
        with pytest.raises(ValueError, match="must not be negative"):
            WindowAggregator(allowed_lateness=-1)


class TestClosing:
    def test_a_window_stays_open_until_the_watermark_passes_its_end(self) -> None:
        aggregator = WindowAggregator(window_seconds=60, allowed_lateness=0)
        aggregator.add(event(BASE + 600))
        assert aggregator.ready() == []

        # Still inside the same window: nothing can be closed.
        aggregator.add(event(BASE + 659))
        assert aggregator.ready() == []

        # The first event of the next window takes the watermark to the end of
        # [BASE+600, BASE+660).
        aggregator.add(event(BASE + 660))
        closed = aggregator.ready()
        assert [window.start for window in closed] == [BASE + 600]
        assert closed[0].event_count == 2

    def test_closed_windows_come_out_in_window_order(self) -> None:
        aggregator = WindowAggregator(window_seconds=60, allowed_lateness=0)
        for epoch in (BASE + 720, BASE + 600, BASE + 660):
            aggregator.add(event(epoch))
        aggregator.add(event(BASE + 900))
        starts = [window.start for window in aggregator.ready()]
        assert starts == [BASE + 600, BASE + 660, BASE + 720]

    def test_a_closed_window_is_not_emitted_twice(self) -> None:
        aggregator = WindowAggregator(window_seconds=60, allowed_lateness=0)
        aggregator.add(event(BASE + 600))
        aggregator.add(event(BASE + 660))
        assert [window.start for window in aggregator.ready()] == [BASE + 600]
        assert aggregator.ready() == []

    def test_allowed_lateness_delays_the_close(self) -> None:
        aggregator = WindowAggregator(window_seconds=60, allowed_lateness=30)
        aggregator.add(event(BASE + 600))
        aggregator.add(event(BASE + 660))
        # The watermark is 30s short of the end the window needs.
        assert aggregator.ready() == []
        aggregator.add(event(BASE + 690))
        assert [window.start for window in aggregator.ready()] == [BASE + 600]

    def test_drain_closes_everything_still_open(self) -> None:
        aggregator = WindowAggregator(window_seconds=60, allowed_lateness=0)
        aggregator.add(event(BASE + 600))
        aggregator.add(event(BASE + 700))
        assert aggregator.open_windows == 2
        assert [window.start for window in aggregator.drain()] == [BASE + 600, BASE + 660]
        assert aggregator.open_windows == 0
        assert aggregator.drain() == []


class TestLateEvents:
    def test_an_out_of_order_event_inside_an_open_window_is_not_late(self) -> None:
        aggregator = WindowAggregator(window_seconds=60, allowed_lateness=0)
        aggregator.add(event(BASE + 640))
        assert aggregator.add(event(BASE + 610)) is None
        assert aggregator.late_events == 0
        aggregator.add(event(BASE + 660))
        assert aggregator.ready()[0].event_count == 2

    def test_an_event_for_an_emitted_window_is_reported_late(self) -> None:
        aggregator = WindowAggregator(window_seconds=60, allowed_lateness=0)
        aggregator.add(event(BASE + 600))
        aggregator.add(event(BASE + 660))
        aggregator.ready()

        late = aggregator.add(event(BASE + 610))
        assert late is not None
        assert late.epoch == BASE + 610
        assert late.window_start == BASE + 600
        assert late.watermark == BASE + 660

    def test_a_late_event_is_counted_and_not_aggregated(self) -> None:
        aggregator = WindowAggregator(window_seconds=60, allowed_lateness=0)
        aggregator.add(event(BASE + 600))
        aggregator.add(event(BASE + 660))
        first = aggregator.ready()[0]
        assert first.event_count == 1

        aggregator.add(event(BASE + 610))
        aggregator.add(event(BASE + 620))
        assert aggregator.late_events == 2
        assert aggregator.late_events_by_window == {BASE + 600: 2}

        # The late events reopen nothing: the first window does not come back.
        aggregator.add(event(BASE + 720))
        assert [window.start for window in aggregator.ready()] == [BASE + 660]

    def test_allowed_lateness_rescues_an_event_that_would_be_late(self) -> None:
        aggregator = WindowAggregator(window_seconds=60, allowed_lateness=60)
        aggregator.add(event(BASE + 600))
        aggregator.add(event(BASE + 660))
        assert aggregator.ready() == []

        assert aggregator.add(event(BASE + 610)) is None
        assert aggregator.late_events == 0

        aggregator.add(event(BASE + 720))
        closed = aggregator.ready()
        assert [window.start for window in closed] == [BASE + 600]
        assert closed[0].event_count == 2

    def test_the_late_count_travels_with_the_window(self) -> None:
        aggregator = WindowAggregator(window_seconds=60, allowed_lateness=0)
        aggregator.add(event(BASE + 600))
        aggregator.add(event(BASE + 720))
        aggregator.ready()
        aggregator.add(event(BASE + 605))

        # The windows that open after the late event carry no late count of
        # their own; the count belongs to the window the event was meant for.
        aggregator.add(event(BASE + 700))
        aggregator.add(event(BASE + 800))
        closed = aggregator.ready()
        assert [window.start for window in closed] == [BASE + 660, BASE + 720]
        assert [window.late_events for window in closed] == [0, 0]
        assert aggregator.late_events_by_window == {BASE + 600: 1}


class TestAggregatedColumns:
    def test_counts_match_the_batch_definitions(self) -> None:
        aggregator = WindowAggregator(window_seconds=60, allowed_lateness=0)
        severities = ["INFO", "INFO", "WARNING", "ERROR", "SEVERE", "FATAL", "FAILURE"]
        for index, severity in enumerate(severities):
            aggregator.add(
                event(
                    BASE + 600 + index,
                    line_no=index + 1,
                    severity=severity,
                    node=f"node-{index % 3}",
                )
            )
        aggregator.add(event(BASE + 660, line_no=99))
        window = aggregator.ready()[0]

        assert window.event_count == 7
        assert window.info_count == 2
        assert window.warning_count == 1
        assert window.error_count == 1
        assert window.severe_count == 1
        assert window.fatal_count == 1
        assert window.failure_count == 1
        assert window.distinct_nodes == 3
        # error_rate counts ERROR, SEVERE, FATAL and FAILURE but not WARNING.
        assert window.error_rate == pytest.approx(4 / 7)

    def test_distinct_templates_counts_masked_templates(self) -> None:
        aggregator = WindowAggregator(window_seconds=60, allowed_lateness=0)
        # The parser masks numbers, so these two differ only in a masked part
        # and collapse to one template.
        aggregator.add(event(BASE + 600, line_no=1, message="core 1 parity error"))
        aggregator.add(event(BASE + 601, line_no=2, message="core 7 parity error"))
        aggregator.add(event(BASE + 602, line_no=3, message="torus receiver link failure"))
        aggregator.add(event(BASE + 660, line_no=4))
        window = aggregator.ready()[0]
        assert window.distinct_templates == 2

    def test_is_anomalous_comes_from_the_alert_label(self) -> None:
        aggregator = WindowAggregator(window_seconds=60, allowed_lateness=0)
        aggregator.add(event(BASE + 600, line_no=1, severity="FATAL"))
        aggregator.add(event(BASE + 660, line_no=2))
        clean = aggregator.ready()[0]
        # FATAL severity on its own is not an alert label.
        assert clean.is_anomalous is False
        assert clean.alert_count == 0

        aggregator.add(event(BASE + 720, line_no=3, label="KERNDTLB", severity="FATAL"))
        aggregator.add(event(BASE + 780, line_no=4))
        flagged = aggregator.ready()[1]
        assert flagged.is_anomalous is True
        assert flagged.alert_count == 1

    def test_the_window_id_is_derived_from_the_start(self) -> None:
        aggregator = WindowAggregator(window_seconds=60, allowed_lateness=0)
        aggregator.add(event(1117838570))
        aggregator.add(event(1117838580))
        window = aggregator.ready()[0]
        assert window.start == 1117838520
        assert window.window_id == "2005-06-03T22:42:00+00:00"
        assert window.start_time == datetime(2005, 6, 3, 22, 42, tzinfo=UTC)
