"""End to end evaluation and the command line entry point."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

import psycopg
import pytest

from netlog_anomaly.detect import severity_rule
from netlog_anomaly.etl import load_file, refresh_windows
from netlog_anomaly.evaluate import evaluate, format_report
from netlog_anomaly.features import SEVERITY_FEATURES
from netlog_anomaly.pipeline import main
from tests.conftest import TEST_SCHEMA
from tests.test_features import BASE_EPOCH, write_log

DETECTOR_LABELS = (
    "severity rule",
    "z-score baseline (all features)",
    "isolation forest (all features)",
    "z-score baseline (severity-free)",
    "isolation forest (severity-free)",
)


def mixed_lines(n_windows: int = 40) -> list[tuple[int, str, str, str]]:
    """Windows that are mostly quiet, with a burst of alerts every fifth one."""
    lines: list[tuple[int, str, str, str]] = []
    for w in range(n_windows):
        start = BASE_EPOCH + w * 60
        for i in range(4):
            lines.append((start + i, "-", "INFO", f"routine chatter kind {i % 2}"))
        if w % 5 == 0:
            for i in range(20):
                lines.append((start + 10 + (i % 40), "KERNDTLB", "FATAL", "data TLB error"))
    return lines


@pytest.fixture
def populated(conn: psycopg.Connection, tmp_path: Path) -> psycopg.Connection:
    load_file(conn, write_log(tmp_path / "mixed.log", mixed_lines()))
    refresh_windows(conn, 60)
    conn.commit()
    return conn


def test_every_detector_and_variant_is_scored(populated: psycopg.Connection) -> None:
    evaluation = evaluate(populated, window_seconds=60, train_fraction=0.7, top_templates=5)
    assert tuple(result.label for result in evaluation.results) == DETECTOR_LABELS


def test_learned_detectors_are_scored_on_both_variants(
    populated: psycopg.Connection,
) -> None:
    evaluation = evaluate(populated, window_seconds=60, train_fraction=0.7, top_templates=5)
    for name in ("z-score baseline", "isolation forest"):
        variants = {r.variant for r in evaluation.results if r.name == name}
        assert variants == {"all features", "severity-free"}


def test_the_severity_free_variant_drops_the_severity_columns(
    populated: psycopg.Connection,
) -> None:
    evaluation = evaluate(populated, window_seconds=60, train_fraction=0.7, top_templates=5)
    assert set(evaluation.reduced.feature_names).isdisjoint(SEVERITY_FEATURES)
    assert len(evaluation.reduced.feature_names) == len(evaluation.split.feature_names) - len(
        SEVERITY_FEATURES
    )


def test_both_variants_are_scored_on_the_same_windows(
    populated: psycopg.Connection,
) -> None:
    evaluation = evaluate(populated, window_seconds=60, train_fraction=0.7, top_templates=5)
    supports = {r.scores.support for r in evaluation.results}
    totals = {r.scores.total for r in evaluation.results}
    assert len(supports) == 1
    assert len(totals) == 1


def test_severity_rule_cannot_run_on_a_severity_free_split(
    populated: psycopg.Connection,
) -> None:
    # The rule reads fatal_count and failure_count directly, so it has to be
    # scored on the full split. Asking for it without those columns must fail
    # loudly rather than quietly scoring something else.
    evaluation = evaluate(populated, window_seconds=60, train_fraction=0.7, top_templates=5)
    reduced = evaluation.reduced
    with pytest.raises(ValueError):
        severity_rule(reduced, reduced.x_test)


def test_metrics_are_in_range_and_counts_cover_the_test_set(
    populated: psycopg.Connection,
) -> None:
    evaluation = evaluate(populated, window_seconds=60, train_fraction=0.7, top_templates=5)
    for result in evaluation.results:
        s = result.scores
        assert 0.0 <= s.precision <= 1.0
        assert 0.0 <= s.recall <= 1.0
        assert 0.0 <= s.f1 <= 1.0
        assert s.total == evaluation.split.n_test
        assert s.support == int(evaluation.split.y_test.sum())


def test_severity_rule_is_exact_on_a_fixture_where_fatal_means_alert(
    populated: psycopg.Connection,
) -> None:
    # In this fixture every FATAL line is labelled an alert and nothing else is,
    # so the severity rule has to score exactly 1.0. That is the point of
    # measuring it: on the real dataset it does not, and the gap is the signal.
    evaluation = evaluate(populated, window_seconds=60, train_fraction=0.7, top_templates=5)
    rule = next(r for r in evaluation.results if r.name == "severity rule")
    assert (rule.scores.precision, rule.scores.recall, rule.scores.f1) == (1.0, 1.0, 1.0)


def test_best_is_the_highest_f1(populated: psycopg.Connection) -> None:
    evaluation = evaluate(populated, window_seconds=60, train_fraction=0.7, top_templates=5)
    assert evaluation.best.scores.f1 == max(r.scores.f1 for r in evaluation.results)


def test_report_names_the_split_and_every_detector(populated: psycopg.Connection) -> None:
    report = format_report(evaluate(populated, window_seconds=60, train_fraction=0.7))
    for label in DETECTOR_LABELS:
        assert label in report
    assert "chronological" in report
    assert "Dropped for the severity-free variant" in report
    assert "Window size      : 60s" in report


def test_evaluation_is_reproducible(populated: psycopg.Connection) -> None:
    first = evaluate(populated, window_seconds=60, train_fraction=0.7, top_templates=5)
    second = evaluate(populated, window_seconds=60, train_fraction=0.7, top_templates=5)
    assert [r.scores for r in first.results] == [r.scores for r in second.results]


def schema_scoped_url(database_url: str) -> str:
    """Point the CLI at the test schema rather than the default one."""
    separator = "&" if "?" in database_url else "?"
    return f"{database_url}{separator}options={quote(f'-c search_path={TEST_SCHEMA}')}"


def test_cli_run_executes_the_whole_pipeline(
    conn: psycopg.Connection,
    database_url: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    conn.commit()
    log = write_log(tmp_path / "cli.log", mixed_lines())
    exit_code = main(
        [
            "--database-url",
            schema_scoped_url(database_url),
            "run",
            str(log),
            "--window-seconds",
            "60",
            "--train-fraction",
            "0.7",
            "--top-templates",
            "5",
        ]
    )
    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Schema applied." in output
    assert "rows inserted    : " in output
    assert "windows of 60s" in output
    for label in DETECTOR_LABELS:
        assert label in output


def test_cli_load_twice_reports_duplicates(
    conn: psycopg.Connection,
    database_url: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    conn.commit()
    log = write_log(tmp_path / "cli.log", mixed_lines(5))
    url = schema_scoped_url(database_url)
    main(["--database-url", url, "init-db"])
    main(["--database-url", url, "load", str(log)])
    capsys.readouterr()
    main(["--database-url", url, "load", str(log)])
    output = capsys.readouterr().out
    assert "rows inserted    : 0" in output
    assert "duplicates skipped: 40" in output
