"""Read-only HTTP API over the results tables.

Every route is a GET and every statement is a SELECT. The API never writes,
so the dashboard cannot alter what the pipeline measured.

Run it with:

    uvicorn netlog_anomaly.api:app --reload
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Annotated, Any

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from psycopg.rows import dict_row

from netlog_anomaly import db

DEFAULT_WINDOW_SECONDS = 60
MAX_LIMIT = 5000

app = FastAPI(
    title="netlog-anomaly",
    description="Read-only view of the window features and detector results.",
    version="0.1.0",
)

# The Next.js dev server runs on a different port, so the browser needs these
# origins allowed. Only GET is permitted, matching the API itself.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@contextmanager
def _connection() -> Iterator[psycopg.Connection]:
    conn = db.connect()
    try:
        conn.read_only = True
        yield conn
    finally:
        conn.close()


def get_connection() -> Iterator[psycopg.Connection]:
    """Request-scoped read-only connection."""
    with _connection() as conn:
        yield conn


Connection = Annotated[psycopg.Connection, Depends(get_connection)]


def _rows(conn: psycopg.Connection, sql: str, params: Any = None) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


@app.get("/api/health")
def health(conn: Connection) -> dict[str, Any]:
    """Liveness plus whether there is anything to look at."""
    counts = _rows(
        conn,
        """
        SELECT (SELECT count(*) FROM log_events)      AS events,
               (SELECT count(*) FROM window_features) AS windows,
               (SELECT count(*) FROM detector_scores) AS detector_scores
        """,
    )[0]
    return {"status": "ok", **counts}


@app.get("/api/detectors")
def detectors(
    conn: Connection,
    window_seconds: int = DEFAULT_WINDOW_SECONDS,
) -> list[dict[str, Any]]:
    """The detector comparison table, best F1 first."""
    return _rows(
        conn,
        """
        SELECT detector, variant, note, precision, recall, f1,
               true_positives, false_positives, false_negatives, true_negatives
          FROM detector_scores
         WHERE window_seconds = %s
         ORDER BY f1 DESC, detector
        """,
        (window_seconds,),
    )


@app.get("/api/summary")
def summary(
    conn: Connection,
    window_seconds: int = DEFAULT_WINDOW_SECONDS,
) -> dict[str, Any]:
    """Window counts and the span covered, for the page header."""
    rows = _rows(
        conn,
        """
        SELECT count(*)                            AS windows,
               count(*) FILTER (WHERE is_anomalous) AS anomalous_windows,
               min(window_start)                   AS first_window,
               max(window_start)                   AS last_window,
               sum(event_count)                    AS events
          FROM window_features
         WHERE window_seconds = %s
        """,
        (window_seconds,),
    )
    if not rows or rows[0]["windows"] == 0:
        raise HTTPException(status_code=404, detail=f"no windows for {window_seconds}s")

    scored = _rows(
        conn,
        """
        SELECT min(window_start) AS first_scored, max(window_start) AS last_scored,
               count(DISTINCT window_start) AS scored_windows
          FROM window_predictions
         WHERE window_seconds = %s
        """,
        (window_seconds,),
    )[0]
    return {"window_seconds": window_seconds, **rows[0], **scored}


@app.get("/api/windows")
def windows(
    conn: Connection,
    window_seconds: int = DEFAULT_WINDOW_SECONDS,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = 500,
    offset: Annotated[int, Query(ge=0)] = 0,
    scored_only: bool = False,
) -> list[dict[str, Any]]:
    """Windows in time order, with the label and each detector's flag.

    Detector flags only exist for the scored (test) period; elsewhere the map
    is empty rather than falsely all-negative.
    """
    return _rows(
        conn,
        """
        SELECT f.window_start,
               f.event_count,
               f.distinct_nodes,
               f.distinct_templates,
               f.error_rate,
               f.alert_count,
               f.is_anomalous,
               COALESCE(p.flags, '{}'::jsonb) AS detector_flags
          FROM window_features f
          LEFT JOIN (
                SELECT window_start,
                       jsonb_object_agg(
                           detector || CASE WHEN variant = '-' THEN ''
                                           ELSE ' (' || variant || ')' END,
                           predicted
                       ) AS flags
                  FROM window_predictions
                 WHERE window_seconds = %(window_seconds)s
                 GROUP BY window_start
               ) p ON p.window_start = f.window_start
         WHERE f.window_seconds = %(window_seconds)s
           AND (NOT %(scored_only)s OR p.flags IS NOT NULL)
         ORDER BY f.window_start
         LIMIT %(limit)s OFFSET %(offset)s
        """,
        {
            "window_seconds": window_seconds,
            "limit": limit,
            "offset": offset,
            "scored_only": scored_only,
        },
    )


@app.get("/api/windows/{window_start}/templates")
def window_templates(
    window_start: datetime,
    conn: Connection,
    window_seconds: int = DEFAULT_WINDOW_SECONDS,
    limit: Annotated[int, Query(ge=1, le=200)] = 20,
) -> dict[str, Any]:
    """Drill-down: one window's top event templates by count."""
    window = _rows(
        conn,
        """
        SELECT window_start, event_count, distinct_nodes, distinct_templates,
               error_rate, alert_count, is_anomalous
          FROM window_features
         WHERE window_seconds = %s AND window_start = %s
        """,
        (window_seconds, window_start),
    )
    if not window:
        raise HTTPException(status_code=404, detail=f"no window at {window_start.isoformat()}")

    templates = _rows(
        conn,
        """
        SELECT c.template_id, t.template, c.event_count
          FROM window_template_counts c
          JOIN event_templates t USING (template_id)
         WHERE c.window_seconds = %s AND c.window_start = %s
         ORDER BY c.event_count DESC, t.template
         LIMIT %s
        """,
        (window_seconds, window_start, limit),
    )
    return {"window": window[0], "templates": templates}
