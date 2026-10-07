"""Load raw log files into Postgres, and aggregate them into time windows.

Loading is idempotent. Lines are staged with COPY and then moved across with
INSERT ... ON CONFLICT DO NOTHING keyed on (source_file, line_no), because COPY
on its own cannot skip conflicting rows. Loading the same file twice therefore
inserts nothing the second time and reports the duplicates it skipped.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import psycopg

from netlog_anomaly.db import read_sql
from netlog_anomaly.parser import (
    LogEvent,
    ParseError,
    RejectedLine,
    escape_for_storage,
    parse_line,
)

STAGE_EVENTS_DDL = """
CREATE TEMPORARY TABLE stg_log_events (
    source_file TEXT, line_no BIGINT, epoch BIGINT, node TEXT, event_type TEXT,
    component TEXT, severity TEXT, label TEXT, is_alert BOOLEAN,
    template_id TEXT, line_sha256 CHAR(64), message TEXT
) ON COMMIT DROP
"""

STAGE_REJECTS_DDL = """
CREATE TEMPORARY TABLE stg_quarantined_lines (
    source_file TEXT, line_no BIGINT, reason TEXT, raw_line TEXT
) ON COMMIT DROP
"""


@dataclass(frozen=True, slots=True)
class LoadStats:
    """What one load of one file did."""

    run_id: int
    source_file: str
    lines_read: int
    lines_parsed: int
    lines_quarantined: int
    rows_inserted: int
    rows_duplicate: int
    templates_seen: int


def iter_lines(path: Path, limit: int | None = None) -> Iterator[tuple[int, str]]:
    """Yield (line_no, decoded_line) pairs.

    The log contains a few byte sequences that are not valid UTF-8. Those are
    decoded with replacement characters, and the parser then quarantines the
    affected lines rather than letting mojibake into the database.
    """
    with path.open("rb") as handle:
        for line_no, raw in enumerate(handle, start=1):
            if limit is not None and line_no > limit:
                return
            yield line_no, raw.decode("utf-8", errors="replace")


def parse_lines(
    lines: Iterator[tuple[int, str]],
) -> Iterator[LogEvent | RejectedLine]:
    """Parse each line into an event or a rejection."""
    for line_no, line in lines:
        try:
            yield parse_line(line, line_no)
        except ParseError as exc:
            yield RejectedLine(
                line_no=line_no,
                reason=exc.reason,
                raw_line=escape_for_storage(line.rstrip("\n")),
            )


def load_file(
    conn: psycopg.Connection,
    path: Path,
    source_file: str | None = None,
    limit: int | None = None,
) -> LoadStats:
    """Parse and load one log file inside a single transaction.

    source_file defaults to the file's base name, so the idempotency key does
    not change when the dataset lives at a different absolute path.
    """
    name = source_file or path.name
    templates: dict[str, str] = {}
    lines_read = 0
    lines_parsed = 0
    lines_quarantined = 0

    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingest_runs (source_file) VALUES (%s) RETURNING run_id",
            (name,),
        )
        row = cur.fetchone()
        assert row is not None
        run_id = int(row[0])

        cur.execute(STAGE_EVENTS_DDL)
        cur.execute(STAGE_REJECTS_DDL)

        events_copy = (
            "COPY stg_log_events (source_file, line_no, epoch, node, event_type, component, "
            "severity, label, is_alert, template_id, line_sha256, message) FROM STDIN"
        )
        rejects_copy = (
            "COPY stg_quarantined_lines (source_file, line_no, reason, raw_line) FROM STDIN"
        )

        # Only one COPY can be in flight on a connection at a time, so rejects
        # are buffered while events stream and copied once that stream closes.
        # Rejects are a small minority by nature: a file where most lines fail
        # validation is a bad input, not a workload to optimise for.
        rejected: list[RejectedLine] = []

        with cur.copy(events_copy) as events:
            for parsed in parse_lines(iter_lines(path, limit=limit)):
                lines_read += 1
                if isinstance(parsed, RejectedLine):
                    lines_quarantined += 1
                    rejected.append(parsed)
                    continue
                lines_parsed += 1
                templates.setdefault(parsed.template_id, parsed.template)
                digest = hashlib.sha256(parsed.message.encode("utf-8")).hexdigest()
                events.write_row(
                    (
                        name,
                        parsed.line_no,
                        parsed.epoch,
                        parsed.node,
                        parsed.event_type,
                        parsed.component,
                        parsed.severity,
                        parsed.label,
                        parsed.is_alert,
                        parsed.template_id,
                        digest,
                        parsed.message,
                    )
                )

        with cur.copy(rejects_copy) as rejects:
            for reject in rejected:
                rejects.write_row((name, reject.line_no, reject.reason, reject.raw_line))

        # Templates first: log_events carries a foreign key to them.
        if templates:
            cur.executemany(
                "INSERT INTO event_templates (template_id, template) VALUES (%s, %s) "
                "ON CONFLICT (template_id) DO NOTHING",
                list(templates.items()),
            )

        cur.execute(
            """
            INSERT INTO log_events (
                source_file, line_no, event_time, epoch, node, event_type, component,
                severity, label, is_alert, template_id, line_sha256, message
            )
            SELECT source_file, line_no, to_timestamp(epoch), epoch, node, event_type,
                   component, severity, label, is_alert, template_id, line_sha256, message
            FROM stg_log_events
            ON CONFLICT (source_file, line_no) DO NOTHING
            """
        )
        rows_inserted = cur.rowcount

        cur.execute(
            """
            INSERT INTO quarantined_lines (source_file, line_no, reason, raw_line)
            SELECT source_file, line_no, reason, raw_line FROM stg_quarantined_lines
            ON CONFLICT (source_file, line_no) DO NOTHING
            """
        )

        cur.execute(
            """
            UPDATE ingest_runs
               SET finished_at = clock_timestamp(), lines_read = %s, rows_inserted = %s,
                   rows_duplicate = %s, lines_quarantined = %s
             WHERE run_id = %s
            """,
            (lines_read, rows_inserted, lines_parsed - rows_inserted, lines_quarantined, run_id),
        )

    return LoadStats(
        run_id=run_id,
        source_file=name,
        lines_read=lines_read,
        lines_parsed=lines_parsed,
        lines_quarantined=lines_quarantined,
        rows_inserted=rows_inserted,
        rows_duplicate=lines_parsed - rows_inserted,
        templates_seen=len(templates),
    )


def refresh_windows(conn: psycopg.Connection, window_seconds: int) -> tuple[int, int]:
    """Rebuild the window tables for one window size.

    Existing rows for that size are deleted first, so a re-run after loading
    more data produces the same result as a single run over everything.
    """
    with conn.transaction(), conn.cursor() as cur:
        params = {"window_seconds": window_seconds}
        cur.execute("DELETE FROM window_features WHERE window_seconds = %s", (window_seconds,))
        cur.execute(
            "DELETE FROM window_template_counts WHERE window_seconds = %s", (window_seconds,)
        )
        cur.execute(read_sql("aggregate_windows.sql"), params)
        windows = cur.rowcount
        cur.execute(read_sql("aggregate_template_counts.sql"), params)
        template_rows = cur.rowcount
    return windows, template_rows
