"""Window aggregation SQL, checked against hand-computed expectations.

The fixture has four non-empty 60 second windows separated by two empty ones,
and includes a FATAL line whose label is "-". That line pins down that
is_anomalous comes from the dataset's alert label and not from severity.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import psycopg

from netlog_anomaly.etl import load_file, refresh_windows

W1 = datetime(2005, 6, 3, 22, 42, 0, tzinfo=UTC)  # epoch 1117838520
W2 = datetime(2005, 6, 3, 22, 43, 0, tzinfo=UTC)  # epoch 1117838580
W3 = datetime(2005, 6, 3, 22, 44, 0, tzinfo=UTC)  # epoch 1117838640
W4 = datetime(2005, 6, 3, 22, 47, 0, tzinfo=UTC)  # epoch 1117838820


def fetch_windows(conn: psycopg.Connection, window_seconds: int = 60) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT window_start, event_count, distinct_nodes, distinct_templates,
                   info_count, warning_count, error_count, severe_count, fatal_count,
                   failure_count, error_rate, alert_count, is_anomalous
              FROM window_features
             WHERE window_seconds = %s
             ORDER BY window_start
            """,
            (window_seconds,),
        )
        return {row[0]: row[1:] for row in cur.fetchall()}


def test_only_non_empty_windows_are_materialised(
    conn: psycopg.Connection, windows_log: Path
) -> None:
    load_file(conn, windows_log)
    windows, _ = refresh_windows(conn, 60)
    assert windows == 4
    assert sorted(fetch_windows(conn)) == [W1, W2, W3, W4]


def test_normal_window_counts(conn: psycopg.Connection, windows_log: Path) -> None:
    load_file(conn, windows_log)
    refresh_windows(conn, 60)
    row = fetch_windows(conn)[W1]
    #   count nodes templates info warn err sev fatal fail  rate  alerts anomalous
    assert row == (4, 2, 3, 2, 1, 1, 0, 0, 0, 0.25, 0, False)


def test_anomalous_window_counts(conn: psycopg.Connection, windows_log: Path) -> None:
    load_file(conn, windows_log)
    refresh_windows(conn, 60)
    row = fetch_windows(conn)[W2]
    assert row == (5, 2, 4, 1, 0, 0, 1, 3, 0, 0.8, 2, True)


def test_fatal_without_an_alert_label_is_not_anomalous(
    conn: psycopg.Connection, windows_log: Path
) -> None:
    load_file(conn, windows_log)
    refresh_windows(conn, 60)
    # W3 holds one FAILURE line labelled "-": severity is high, the label is not
    # an alert, so the window must not be marked anomalous.
    count, _nodes, _templates, _i, _w, _e, _s, fatal, failure, rate, alerts, anomalous = (
        fetch_windows(conn)[W3]
    )
    assert (count, fatal, failure, rate, alerts, anomalous) == (1, 0, 1, 1.0, 0, False)


def test_error_rate_excludes_warnings(conn: psycopg.Connection, windows_log: Path) -> None:
    load_file(conn, windows_log)
    refresh_windows(conn, 60)
    row = fetch_windows(conn)[W1]
    warning_count, error_rate = row[4], row[9]
    # One WARNING and one ERROR out of four events: the rate counts only the ERROR.
    assert warning_count == 1
    assert error_rate == 0.25


def test_repeated_template_collapses_in_distinct_count(
    conn: psycopg.Connection, windows_log: Path
) -> None:
    load_file(conn, windows_log)
    refresh_windows(conn, 60)
    # Two lines differing only in a number share one template.
    assert fetch_windows(conn)[W4] == (2, 1, 1, 2, 0, 0, 0, 0, 0, 0.0, 0, False)


def test_event_counts_sum_to_the_rows_loaded(conn: psycopg.Connection, windows_log: Path) -> None:
    stats = load_file(conn, windows_log)
    refresh_windows(conn, 60)
    with conn.cursor() as cur:
        cur.execute("SELECT sum(event_count) FROM window_features WHERE window_seconds = 60")
        row = cur.fetchone()
    assert row is not None
    assert row[0] == stats.rows_inserted == 12


def test_window_size_changes_the_grouping(conn: psycopg.Connection, windows_log: Path) -> None:
    load_file(conn, windows_log)
    refresh_windows(conn, 300)
    five_minute = fetch_windows(conn, 300)
    assert [(start, row[0]) for start, row in sorted(five_minute.items())] == [
        (datetime(2005, 6, 3, 22, 40, tzinfo=UTC), 10),
        (datetime(2005, 6, 3, 22, 45, tzinfo=UTC), 2),
    ]


def test_window_sizes_coexist(conn: psycopg.Connection, windows_log: Path) -> None:
    load_file(conn, windows_log)
    refresh_windows(conn, 60)
    refresh_windows(conn, 300)
    assert len(fetch_windows(conn, 60)) == 4
    assert len(fetch_windows(conn, 300)) == 2


def test_template_counts_are_per_window(conn: psycopg.Connection, windows_log: Path) -> None:
    load_file(conn, windows_log)
    refresh_windows(conn, 60)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.template, c.event_count
              FROM window_template_counts c
              JOIN event_templates t USING (template_id)
             WHERE c.window_seconds = 60 AND c.window_start = %s
             ORDER BY c.event_count DESC, t.template
            """,
            (W1,),
        )
        rows = cur.fetchall()
    assert rows == [
        ("instruction cache parity error corrected", 2),
        ("CE sym <NUM>, at <HEX>, mask <HEX>", 1),
        ("ddr: suppressing further CE interrupts", 1),
    ]


def test_template_counts_sum_to_event_counts(conn: psycopg.Connection, windows_log: Path) -> None:
    load_file(conn, windows_log)
    refresh_windows(conn, 60)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.window_start
              FROM window_features f
              JOIN (
                    SELECT window_start, sum(event_count) AS total
                      FROM window_template_counts WHERE window_seconds = 60
                     GROUP BY 1
                   ) c ON c.window_start = f.window_start
             WHERE f.window_seconds = 60 AND c.total <> f.event_count
            """
        )
        assert cur.fetchall() == []


def test_refresh_replaces_rather_than_appends(conn: psycopg.Connection, windows_log: Path) -> None:
    load_file(conn, windows_log)
    refresh_windows(conn, 60)
    before = fetch_windows(conn)
    for _ in range(3):
        refresh_windows(conn, 60)
    assert fetch_windows(conn) == before
