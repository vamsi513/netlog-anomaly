"""Feature building and the chronological split, including leakage checks."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import psycopg
import pytest

from netlog_anomaly.etl import load_file, refresh_windows
from netlog_anomaly.features import (
    BASE_FEATURES,
    SEVERITY_FEATURES,
    build_split,
    drop_features,
)

BASE_EPOCH = 1117838520  # 2005-06-03 22:42:00 UTC, a 60 second boundary


def write_log(path: Path, lines: list[tuple[int, str, str, str]]) -> Path:
    """Write a BGL format log from (epoch, label, severity, message) tuples."""
    rendered = []
    for epoch, label, severity, message in lines:
        moment = datetime.fromtimestamp(epoch, tz=UTC)
        date = moment.strftime("%Y.%m.%d")
        stamp = moment.strftime("%Y-%m-%d-%H.%M.%S.%f")
        node = "R02-M1-N0-C:J12-U11"
        rendered.append(
            f"{label} {epoch} {date} {node} {stamp} {node} RAS KERNEL {severity} {message}"
        )
    path.write_text("\n".join(rendered) + "\n", encoding="utf-8")
    return path


def make_windows(
    conn: psycopg.Connection, tmp_path: Path, lines: list[tuple[int, str, str, str]]
) -> None:
    load_file(conn, write_log(tmp_path / "generated.log", lines))
    refresh_windows(conn, 60)


def simple_lines(n_windows: int) -> list[tuple[int, str, str, str]]:
    """One normal and one alert line per window, so every window is anomalous."""
    lines = []
    for w in range(n_windows):
        start = BASE_EPOCH + w * 60
        lines.append((start, "-", "INFO", "routine chatter"))
        lines.append((start + 1, "KERNDTLB", "FATAL", "data TLB error interrupt"))
    return lines


def test_split_is_chronological(conn: psycopg.Connection, tmp_path: Path) -> None:
    make_windows(conn, tmp_path, simple_lines(10))
    split = build_split(conn, window_seconds=60, train_fraction=0.7)

    assert split.n_train == 7
    assert split.n_test == 3
    assert max(split.train_starts) < split.cutoff
    assert min(split.test_starts) == split.cutoff
    assert list(split.train_starts) == sorted(split.train_starts)
    assert list(split.test_starts) == sorted(split.test_starts)


def test_no_window_appears_in_both_sides(conn: psycopg.Connection, tmp_path: Path) -> None:
    make_windows(conn, tmp_path, simple_lines(10))
    split = build_split(conn, window_seconds=60, train_fraction=0.5)
    assert set(split.train_starts).isdisjoint(split.test_starts)
    assert split.n_train + split.n_test == 10


def test_template_features_are_chosen_from_the_training_period_only(
    conn: psycopg.Connection, tmp_path: Path
) -> None:
    # A template that floods the test period but never appears in training must
    # not become a feature column: selecting columns over the whole dataset
    # would let the test period shape the model's inputs.
    lines: list[tuple[int, str, str, str]] = []
    for w in range(6):  # training period
        start = BASE_EPOCH + w * 60
        for i in range(5):
            lines.append((start + i, "-", "INFO", "seen during training"))
    for w in range(6, 10):  # test period
        start = BASE_EPOCH + w * 60
        for i in range(50):
            lines.append((start + i % 60, "-", "INFO", "only ever seen during test"))
    make_windows(conn, tmp_path, lines)

    split = build_split(conn, window_seconds=60, train_fraction=0.6, top_templates=1)

    assert len(split.template_features) == 1
    chosen = split.template_features[0]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT template FROM event_templates WHERE template_id = %s",
            (chosen.removeprefix("tpl_"),),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "seen during training"


def test_template_column_still_carries_test_period_values(
    conn: psycopg.Connection, tmp_path: Path
) -> None:
    # The chosen columns are populated for every window, train and test alike;
    # only the choice of columns is restricted to the training period.
    make_windows(conn, tmp_path, simple_lines(10))
    split = build_split(conn, window_seconds=60, train_fraction=0.7, top_templates=2)
    template_columns = [split.column(name) for name in split.template_features]
    assert split.x_test[:, template_columns].sum() > 0


def test_feature_names_match_matrix_width(conn: psycopg.Connection, tmp_path: Path) -> None:
    make_windows(conn, tmp_path, simple_lines(10))
    split = build_split(conn, window_seconds=60, train_fraction=0.7, top_templates=2)
    assert len(split.feature_names) == split.x_train.shape[1] == split.x_test.shape[1]
    assert split.feature_names[: len(BASE_FEATURES)] == BASE_FEATURES


def test_labels_line_up_with_the_window_rows(conn: psycopg.Connection, tmp_path: Path) -> None:
    lines: list[tuple[int, str, str, str]] = []
    for w in range(10):
        start = BASE_EPOCH + w * 60
        lines.append((start, "-", "INFO", "routine chatter"))
        if w % 2 == 0:
            lines.append((start + 1, "KERNDTLB", "FATAL", "data TLB error interrupt"))
    make_windows(conn, tmp_path, lines)

    split = build_split(conn, window_seconds=60, train_fraction=0.7)
    labels = np.concatenate([split.y_train, split.y_test])
    assert labels.tolist() == [True, False] * 5


def test_zero_template_features_is_allowed(conn: psycopg.Connection, tmp_path: Path) -> None:
    make_windows(conn, tmp_path, simple_lines(10))
    split = build_split(conn, window_seconds=60, train_fraction=0.7, top_templates=0)
    assert split.template_features == ()
    assert split.x_train.shape[1] == len(BASE_FEATURES)


@pytest.mark.parametrize("fraction", [0.0, 1.0, -0.5, 1.5])
def test_invalid_train_fraction_is_rejected(
    conn: psycopg.Connection, tmp_path: Path, fraction: float
) -> None:
    make_windows(conn, tmp_path, simple_lines(10))
    with pytest.raises(ValueError, match="train_fraction"):
        build_split(conn, window_seconds=60, train_fraction=fraction)


def test_a_fraction_that_empties_one_side_is_rejected(
    conn: psycopg.Connection, tmp_path: Path
) -> None:
    make_windows(conn, tmp_path, simple_lines(2))
    with pytest.raises(ValueError, match="empty"):
        build_split(conn, window_seconds=60, train_fraction=0.1)


def test_missing_windows_raise_a_clear_error(conn: psycopg.Connection) -> None:
    with pytest.raises(ValueError, match="No windows found"):
        build_split(conn, window_seconds=60)


def test_requesting_an_unaggregated_window_size_raises(
    conn: psycopg.Connection, tmp_path: Path
) -> None:
    make_windows(conn, tmp_path, simple_lines(10))
    with pytest.raises(ValueError, match="window_seconds=300"):
        build_split(conn, window_seconds=300)


def test_cutoff_is_an_actual_window_boundary(conn: psycopg.Connection, tmp_path: Path) -> None:
    make_windows(conn, tmp_path, simple_lines(10))
    split = build_split(conn, window_seconds=60, train_fraction=0.7)
    assert split.cutoff == datetime.fromtimestamp(BASE_EPOCH + 7 * 60, tz=UTC)


def test_drop_features_removes_exactly_the_named_columns(
    conn: psycopg.Connection, tmp_path: Path
) -> None:
    make_windows(conn, tmp_path, simple_lines(10))
    split = build_split(conn, window_seconds=60, train_fraction=0.7, top_templates=2)
    reduced = drop_features(split, ["event_count", "error_rate"])

    assert "event_count" not in reduced.feature_names
    assert "error_rate" not in reduced.feature_names
    assert len(reduced.feature_names) == len(split.feature_names) - 2
    assert reduced.x_train.shape == (split.n_train, len(reduced.feature_names))
    assert reduced.x_test.shape == (split.n_test, len(reduced.feature_names))


def test_drop_features_keeps_rows_labels_and_split_boundary(
    conn: psycopg.Connection, tmp_path: Path
) -> None:
    make_windows(conn, tmp_path, simple_lines(10))
    split = build_split(conn, window_seconds=60, train_fraction=0.7, top_templates=2)
    reduced = drop_features(split, SEVERITY_FEATURES)

    assert reduced.y_train.tolist() == split.y_train.tolist()
    assert reduced.y_test.tolist() == split.y_test.tolist()
    assert reduced.train_starts == split.train_starts
    assert reduced.test_starts == split.test_starts
    assert reduced.cutoff == split.cutoff


def test_drop_features_preserves_the_surviving_column_values(
    conn: psycopg.Connection, tmp_path: Path
) -> None:
    make_windows(conn, tmp_path, simple_lines(10))
    split = build_split(conn, window_seconds=60, train_fraction=0.7, top_templates=2)
    reduced = drop_features(split, SEVERITY_FEATURES)

    for name in reduced.feature_names:
        assert reduced.x_test[:, reduced.column(name)].tolist() == (
            split.x_test[:, split.column(name)].tolist()
        )


def test_severity_free_variant_has_no_severity_column(
    conn: psycopg.Connection, tmp_path: Path
) -> None:
    make_windows(conn, tmp_path, simple_lines(10))
    split = build_split(conn, window_seconds=60, train_fraction=0.7, top_templates=2)
    reduced = drop_features(split, SEVERITY_FEATURES)

    assert set(reduced.feature_names).isdisjoint(SEVERITY_FEATURES)
    assert reduced.feature_names[:3] == ("event_count", "distinct_nodes", "distinct_templates")
    # The template columns survive: they are derived from the message, not the
    # severity column.
    assert reduced.template_features == split.template_features


def test_every_severity_feature_is_a_real_base_feature() -> None:
    assert set(SEVERITY_FEATURES) < set(BASE_FEATURES)


def test_dropping_an_unknown_feature_is_rejected(conn: psycopg.Connection, tmp_path: Path) -> None:
    make_windows(conn, tmp_path, simple_lines(10))
    split = build_split(conn, window_seconds=60, train_fraction=0.7)
    with pytest.raises(ValueError, match="not features of this split: nonsense"):
        drop_features(split, ["nonsense"])


def test_dropping_nothing_leaves_the_split_unchanged(
    conn: psycopg.Connection, tmp_path: Path
) -> None:
    make_windows(conn, tmp_path, simple_lines(10))
    split = build_split(conn, window_seconds=60, train_fraction=0.7, top_templates=2)
    reduced = drop_features(split, [])
    assert reduced.feature_names == split.feature_names
    assert reduced.x_train.tolist() == split.x_train.tolist()
