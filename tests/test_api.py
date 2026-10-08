"""The read-only API, against a real Postgres instance.

The API opens its own connection from NETLOG_DATABASE_URL, so these tests
point that variable at the test schema for the duration of the test rather
than reusing the fixture's connection.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import quote

import psycopg
import pytest
from fastapi.testclient import TestClient

from netlog_anomaly import db
from netlog_anomaly.api import app
from netlog_anomaly.etl import load_file, refresh_windows
from netlog_anomaly.evaluate import evaluate, store_evaluation
from tests.conftest import TEST_SCHEMA
from tests.test_evaluate import mixed_lines
from tests.test_features import write_log


@pytest.fixture
def client(conn: psycopg.Connection, database_url: str, tmp_path: Path) -> Iterator[TestClient]:
    # 200 windows: 140 train and 60 test at a 0.7 split, with an alert burst
    # every fifth window.
    load_file(conn, write_log(tmp_path / "api.log", mixed_lines(200)))
    refresh_windows(conn, 60)
    evaluation = evaluate(conn, window_seconds=60, train_fraction=0.7, top_templates=5)
    store_evaluation(conn, evaluation)
    conn.commit()

    separator = "&" if "?" in database_url else "?"
    scoped = f"{database_url}{separator}options={quote(f'-c search_path={TEST_SCHEMA}')}"
    previous = os.environ.get(db.ENV_VAR)
    os.environ[db.ENV_VAR] = scoped
    try:
        with TestClient(app) as test_client:
            yield test_client
    finally:
        if previous is None:
            os.environ.pop(db.ENV_VAR, None)
        else:
            os.environ[db.ENV_VAR] = previous


def test_health_reports_counts(client: TestClient) -> None:
    body = client.get("/api/health").json()
    assert body["status"] == "ok"
    assert body["events"] > 0
    assert body["windows"] == 200
    assert body["detector_scores"] == 7


def test_summary_describes_the_window_set(client: TestClient) -> None:
    body = client.get("/api/summary").json()
    assert body["window_seconds"] == 60
    assert body["windows"] == 200
    assert body["anomalous_windows"] == 40
    assert body["scored_windows"] == 60
    assert body["first_window"] < body["last_window"]
    # The scored span is the test period, so it starts after the first window.
    assert body["first_scored"] > body["first_window"]


def test_summary_404s_for_an_unaggregated_window_size(client: TestClient) -> None:
    response = client.get("/api/summary", params={"window_seconds": 300})
    assert response.status_code == 404
    assert "300s" in response.json()["detail"]


def test_detectors_returns_every_row_sorted_by_f1(client: TestClient) -> None:
    rows = client.get("/api/detectors").json()
    assert len(rows) == 7
    f1s = [row["f1"] for row in rows]
    assert f1s == sorted(f1s, reverse=True)
    for row in rows:
        assert 0.0 <= row["precision"] <= 1.0
        assert 0.0 <= row["recall"] <= 1.0
        assert row["note"]


def test_detectors_includes_both_variants_and_the_supervised_models(
    client: TestClient,
) -> None:
    rows = client.get("/api/detectors").json()
    labels = {(row["detector"], row["variant"]) for row in rows}
    assert ("severity rule", "-") in labels
    assert ("logistic regression", "severity-free") in labels
    assert ("gradient boosting", "severity-free") in labels
    assert ("isolation forest", "all features") in labels
    assert ("isolation forest", "severity-free") in labels


def test_detectors_confusion_counts_cover_the_test_set(client: TestClient) -> None:
    rows = client.get("/api/detectors").json()
    totals = {
        row["true_positives"]
        + row["false_positives"]
        + row["false_negatives"]
        + row["true_negatives"]
        for row in rows
    }
    assert totals == {60}


def test_windows_are_returned_in_time_order(client: TestClient) -> None:
    rows = client.get("/api/windows", params={"limit": 50}).json()
    assert len(rows) == 50
    starts = [row["window_start"] for row in rows]
    assert starts == sorted(starts)


def test_windows_carry_labels_and_detector_flags(client: TestClient) -> None:
    rows = client.get("/api/windows", params={"scored_only": True, "limit": 5000}).json()
    assert len(rows) == 60
    for row in rows:
        assert isinstance(row["is_anomalous"], bool)
        assert len(row["detector_flags"]) == 7
        assert all(isinstance(flag, bool) for flag in row["detector_flags"].values())


def test_unscored_windows_have_an_empty_flag_map_not_false_flags(
    client: TestClient,
) -> None:
    # Training-period windows were never scored. Reporting them as all-negative
    # would read as seven detectors agreeing, which is not what happened.
    rows = client.get("/api/windows", params={"limit": 5000}).json()
    unscored = [row for row in rows if not row["detector_flags"]]
    assert len(unscored) == 140
    assert all(row["detector_flags"] == {} for row in unscored)


def test_windows_pagination_does_not_repeat_or_skip(client: TestClient) -> None:
    first = client.get("/api/windows", params={"limit": 30, "offset": 0}).json()
    second = client.get("/api/windows", params={"limit": 30, "offset": 30}).json()
    all_at_once = client.get("/api/windows", params={"limit": 60, "offset": 0}).json()
    assert [r["window_start"] for r in first + second] == [r["window_start"] for r in all_at_once]


@pytest.mark.parametrize(
    ("params", "expected"),
    [({"limit": 0}, 422), ({"limit": 99999}, 422), ({"offset": -1}, 422)],
)
def test_windows_rejects_out_of_range_paging(
    client: TestClient, params: dict, expected: int
) -> None:
    assert client.get("/api/windows", params=params).status_code == expected


def test_window_templates_drill_down(client: TestClient) -> None:
    rows = client.get("/api/windows", params={"limit": 1}).json()
    start = rows[0]["window_start"]

    body = client.get(f"/api/windows/{start}/templates").json()
    assert body["window"]["window_start"] == start
    assert body["templates"]
    counts = [t["event_count"] for t in body["templates"]]
    assert counts == sorted(counts, reverse=True)
    for template in body["templates"]:
        assert template["template"]
        assert len(template["template_id"]) == 16


def test_template_counts_sum_to_the_window_event_count(client: TestClient) -> None:
    rows = client.get("/api/windows", params={"limit": 1}).json()
    start = rows[0]["window_start"]
    body = client.get(f"/api/windows/{start}/templates", params={"limit": 200}).json()
    assert sum(t["event_count"] for t in body["templates"]) == body["window"]["event_count"]


def test_window_templates_404s_for_an_unknown_window(client: TestClient) -> None:
    response = client.get("/api/windows/2001-01-01T00:00:00Z/templates")
    assert response.status_code == 404
    assert "no window" in response.json()["detail"]


def test_window_templates_rejects_an_unparseable_timestamp(client: TestClient) -> None:
    assert client.get("/api/windows/not-a-timestamp/templates").status_code == 422


def test_api_is_read_only(client: TestClient) -> None:
    # Nothing in the app should expose a mutating verb.
    for route in app.routes:
        methods = getattr(route, "methods", set()) or set()
        assert methods <= {"GET", "HEAD"}, f"{route} exposes {methods}"


def test_a_write_through_the_api_connection_is_refused(client: TestClient) -> None:
    # The connection is marked read-only, so even a bug that tried to write
    # would be stopped by Postgres rather than changing the results.
    client.get("/api/health")
    with db.connect(os.environ[db.ENV_VAR]) as conn:
        conn.read_only = True
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction), conn.cursor() as cur:
            cur.execute("DELETE FROM detector_scores")


def test_summary_reports_the_first_anomalous_scored_window(client: TestClient) -> None:
    body = client.get("/api/summary").json()
    index = body["first_anomalous_index"]
    assert isinstance(index, int)
    assert 0 <= index < body["scored_windows"]


def test_first_anomalous_index_matches_the_scored_window_list(
    client: TestClient,
) -> None:
    # The index is a position within the scored windows in time order, which is
    # exactly what /api/windows?scored_only=true returns.
    body = client.get("/api/summary").json()
    rows = client.get("/api/windows", params={"scored_only": True, "limit": 5000}).json()

    index = body["first_anomalous_index"]
    assert rows[index]["is_anomalous"] is True
    assert all(not row["is_anomalous"] for row in rows[:index])


def test_first_anomalous_index_counts_scored_windows_only(client: TestClient) -> None:
    # Training windows precede the scored ones and include anomalous windows,
    # so an index counted over every window would point at the wrong row.
    body = client.get("/api/summary").json()
    all_rows = client.get("/api/windows", params={"limit": 5000}).json()
    scored = client.get("/api/windows", params={"scored_only": True, "limit": 5000}).json()

    assert len(all_rows) > len(scored)
    assert any(row["is_anomalous"] for row in all_rows[: len(all_rows) - len(scored)])
    assert body["first_anomalous_index"] < len(scored)


def test_first_anomalous_index_is_null_without_predictions(
    conn: psycopg.Connection, client: TestClient
) -> None:
    # With nothing scored there is no scored list to index into, so the field
    # must be null rather than zero, which would point at a real window.
    with conn.cursor() as cur:
        cur.execute("DELETE FROM window_predictions")
    conn.commit()

    body = client.get("/api/summary").json()
    assert body["scored_windows"] == 0
    assert body["first_anomalous_index"] is None
