"""ETL behaviour against a real Postgres instance."""

from __future__ import annotations

from pathlib import Path

import psycopg

from netlog_anomaly.etl import load_file, refresh_windows


def scalar(conn: psycopg.Connection, sql: str, params: tuple = ()) -> object:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        assert row is not None
        return row[0]


def test_load_inserts_every_valid_line(conn: psycopg.Connection, sample_log: Path) -> None:
    stats = load_file(conn, sample_log)
    assert stats.lines_read == 7
    assert stats.lines_parsed == 7
    assert stats.lines_quarantined == 0
    assert stats.rows_inserted == 7
    assert stats.rows_duplicate == 0
    assert scalar(conn, "SELECT count(*) FROM log_events") == 7


def test_loaded_fields_round_trip(conn: psycopg.Connection, sample_log: Path) -> None:
    load_file(conn, sample_log)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT source_file, line_no, epoch, node, severity, label, is_alert, message
              FROM log_events WHERE line_no = 4
            """
        )
        row = cur.fetchone()
    assert row == (
        "sample_bgl.log",
        4,
        1117838573,
        "R02-M1-N0-C:J12-U11",
        "FATAL",
        "KERNDTLB",
        True,
        "data TLB error interrupt",
    )


def test_event_time_matches_epoch(conn: psycopg.Connection, sample_log: Path) -> None:
    load_file(conn, sample_log)
    mismatches = scalar(
        conn,
        "SELECT count(*) FROM log_events WHERE event_time <> to_timestamp(epoch)",
    )
    assert mismatches == 0


def test_templates_are_deduplicated(conn: psycopg.Connection, sample_log: Path) -> None:
    load_file(conn, sample_log)
    # Lines 1 and 2 are byte identical in their message, so they share a template.
    assert scalar(conn, "SELECT count(*) FROM event_templates") == 6
    shared = scalar(
        conn,
        "SELECT count(DISTINCT template_id) FROM log_events WHERE line_no IN (1, 2)",
    )
    assert shared == 1


def test_reloading_the_same_file_does_not_duplicate(
    conn: psycopg.Connection, sample_log: Path
) -> None:
    first = load_file(conn, sample_log)
    second = load_file(conn, sample_log)

    assert first.rows_inserted == 7
    assert second.rows_inserted == 0
    assert second.rows_duplicate == 7
    assert second.lines_read == 7
    assert scalar(conn, "SELECT count(*) FROM log_events") == 7
    assert scalar(conn, "SELECT count(*) FROM event_templates") == 6


def test_three_loads_still_hold_seven_rows(conn: psycopg.Connection, sample_log: Path) -> None:
    for _ in range(3):
        load_file(conn, sample_log)
    assert scalar(conn, "SELECT count(*) FROM log_events") == 7


def test_same_content_under_a_different_source_name_is_a_separate_load(
    conn: psycopg.Connection, sample_log: Path
) -> None:
    # The idempotency key is (source_file, line_no), so an explicitly different
    # source name is a distinct input rather than a duplicate.
    load_file(conn, sample_log)
    other = load_file(conn, sample_log, source_file="second_copy.log")
    assert other.rows_inserted == 7
    assert scalar(conn, "SELECT count(*) FROM log_events") == 14


def test_limit_loads_a_prefix_only(conn: psycopg.Connection, sample_log: Path) -> None:
    stats = load_file(conn, sample_log, limit=3)
    assert stats.lines_read == 3
    assert scalar(conn, "SELECT max(line_no) FROM log_events") == 3


def test_malformed_lines_are_quarantined_with_a_reason(
    conn: psycopg.Connection, malformed_log: Path
) -> None:
    stats = load_file(conn, malformed_log)
    assert stats.lines_read == 7
    assert stats.lines_parsed == 1
    assert stats.lines_quarantined == 6
    assert stats.rows_inserted == 1

    with conn.cursor() as cur:
        cur.execute("SELECT reason, count(*) FROM quarantined_lines GROUP BY 1 ORDER BY 1")
        reasons = dict(cur.fetchall())
    assert reasons == {
        "bad_date": 1,
        "bad_datetime": 1,
        "bad_epoch": 2,
        "empty_line": 1,
        "too_few_fields": 1,
    }


def test_quarantine_keeps_the_raw_line(conn: psycopg.Connection, malformed_log: Path) -> None:
    load_file(conn, malformed_log)
    raw = scalar(conn, "SELECT raw_line FROM quarantined_lines WHERE reason = 'bad_date'")
    assert isinstance(raw, str)
    assert "06/03/2005" in raw


def test_quarantine_is_idempotent_too(conn: psycopg.Connection, malformed_log: Path) -> None:
    load_file(conn, malformed_log)
    load_file(conn, malformed_log)
    assert scalar(conn, "SELECT count(*) FROM quarantined_lines") == 6


def test_ingest_run_records_the_counts(conn: psycopg.Connection, malformed_log: Path) -> None:
    stats = load_file(conn, malformed_log)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT source_file, lines_read, rows_inserted, rows_duplicate, lines_quarantined,
                   finished_at IS NOT NULL
              FROM ingest_runs WHERE run_id = %s
            """,
            (stats.run_id,),
        )
        row = cur.fetchone()
    assert row == ("malformed_bgl.log", 7, 1, 0, 6, True)


def test_a_failed_load_leaves_no_partial_rows(conn: psycopg.Connection, tmp_path: Path) -> None:
    # A load runs in one transaction, so an error part way through rolls the
    # whole thing back rather than leaving half a file behind.
    missing = tmp_path / "does_not_exist.log"
    try:
        load_file(conn, missing)
    except FileNotFoundError:
        pass
    else:  # pragma: no cover
        raise AssertionError("expected FileNotFoundError")
    conn.rollback()
    assert scalar(conn, "SELECT count(*) FROM log_events") == 0
    assert scalar(conn, "SELECT count(*) FROM ingest_runs") == 0


def test_refresh_windows_is_idempotent(conn: psycopg.Connection, sample_log: Path) -> None:
    load_file(conn, sample_log)
    first = refresh_windows(conn, window_seconds=60)
    second = refresh_windows(conn, window_seconds=60)
    assert first == second
    assert scalar(conn, "SELECT count(*) FROM window_features") == first[0]
