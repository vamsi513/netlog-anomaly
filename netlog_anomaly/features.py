"""Build a feature matrix from the aggregated windows, split without leakage.

The split is chronological: the training set is the earlier part of the log and
the test set the later part. Nothing about the test period may influence the
training inputs, so the set of template features is chosen from the training
windows alone rather than from the whole dataset.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import datetime

import numpy as np
import psycopg

BASE_FEATURES: tuple[str, ...] = (
    "event_count",
    "distinct_nodes",
    "distinct_templates",
    "info_count",
    "warning_count",
    "error_count",
    "severe_count",
    "fatal_count",
    "failure_count",
    "error_rate",
)

# Every base feature computed from the severity column. error_rate belongs here
# too: it is a ratio of severity counts, so keeping it would leave the severity
# signal in a variant that claims not to use it.
SEVERITY_FEATURES: tuple[str, ...] = (
    "info_count",
    "warning_count",
    "error_count",
    "severe_count",
    "fatal_count",
    "failure_count",
    "error_rate",
)


@dataclass(frozen=True, slots=True)
class Split:
    """A chronological train/test split of the window dataset."""

    feature_names: tuple[str, ...]
    x_train: np.ndarray
    y_train: np.ndarray
    x_test: np.ndarray
    y_test: np.ndarray
    train_starts: tuple[datetime, ...]
    test_starts: tuple[datetime, ...]
    cutoff: datetime
    window_seconds: int
    template_features: tuple[str, ...]

    @property
    def n_train(self) -> int:
        return int(self.x_train.shape[0])

    @property
    def n_test(self) -> int:
        return int(self.x_test.shape[0])

    def column(self, name: str) -> int:
        return self.feature_names.index(name)


@dataclass(frozen=True, slots=True)
class FitValidation:
    """A second chronological split, inside the training period only.

    Supervised detectors need somewhere to fix a decision threshold. Doing that
    on the test period would be leakage and doing it on the same rows the model
    fitted on would overstate it, so the training period is split again in time
    order: the model fits on the earlier part and the threshold is chosen on the
    later part. The test period is not touched by either step.
    """

    x_fit: np.ndarray
    y_fit: np.ndarray
    x_validation: np.ndarray
    y_validation: np.ndarray
    fit_starts: tuple[datetime, ...]
    validation_starts: tuple[datetime, ...]
    cutoff: datetime

    @property
    def n_fit(self) -> int:
        return int(self.x_fit.shape[0])

    @property
    def n_validation(self) -> int:
        return int(self.x_validation.shape[0])


def fit_validation_split(split: Split, fit_fraction: float = 0.8) -> FitValidation:
    """Divide the training windows into a fit slice and a validation slice."""
    if not 0.0 < fit_fraction < 1.0:
        raise ValueError(f"fit_fraction must be between 0 and 1, got {fit_fraction}")

    n_fit = int(split.n_train * fit_fraction)
    if n_fit == 0 or n_fit == split.n_train:
        raise ValueError(
            f"fit_fraction={fit_fraction} leaves one side of the validation split empty "
            f"for {split.n_train} training windows"
        )

    return FitValidation(
        x_fit=split.x_train[:n_fit],
        y_fit=split.y_train[:n_fit],
        x_validation=split.x_train[n_fit:],
        y_validation=split.y_train[n_fit:],
        fit_starts=split.train_starts[:n_fit],
        validation_starts=split.train_starts[n_fit:],
        cutoff=split.train_starts[n_fit],
    )


def drop_features(split: Split, names: Iterable[str]) -> Split:
    """The same split with the named feature columns removed.

    Rows, labels, timestamps and the split boundary are untouched, so a variant
    built this way is comparable with the full one on exactly the same windows.
    """
    unwanted = set(names)
    unknown = sorted(unwanted - set(split.feature_names))
    if unknown:
        raise ValueError(f"not features of this split: {', '.join(unknown)}")

    keep = [i for i, name in enumerate(split.feature_names) if name not in unwanted]
    kept_names = tuple(split.feature_names[i] for i in keep)
    return replace(
        split,
        feature_names=kept_names,
        x_train=split.x_train[:, keep],
        x_test=split.x_test[:, keep],
        template_features=tuple(
            name for name in split.template_features if name in set(kept_names)
        ),
    )


def _fetch_base(
    conn: psycopg.Connection, window_seconds: int
) -> tuple[list[datetime], np.ndarray, np.ndarray]:
    columns = ", ".join(BASE_FEATURES)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT window_start, {columns}, is_anomalous
              FROM window_features
             WHERE window_seconds = %s
             ORDER BY window_start
            """,
            (window_seconds,),
        )
        rows = cur.fetchall()
    if not rows:
        raise ValueError(
            f"No windows found for window_seconds={window_seconds}. "
            "Load a log file and refresh the windows first."
        )
    starts = [row[0] for row in rows]
    base = np.asarray([row[1:-1] for row in rows], dtype=np.float64)
    labels = np.asarray([bool(row[-1]) for row in rows], dtype=bool)
    return starts, base, labels


def _train_top_templates(
    conn: psycopg.Connection, window_seconds: int, cutoff: datetime, limit: int
) -> list[str]:
    """Most frequent templates in the training period only.

    Selecting these over the whole dataset would let the test period decide
    which columns the model sees, which is leakage even though no label is read.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT template_id
              FROM window_template_counts
             WHERE window_seconds = %s AND window_start < %s
             GROUP BY template_id
             ORDER BY sum(event_count) DESC, template_id
             LIMIT %s
            """,
            (window_seconds, cutoff, limit),
        )
        return [row[0] for row in cur.fetchall()]


def _template_matrix(
    conn: psycopg.Connection,
    window_seconds: int,
    starts: list[datetime],
    template_ids: list[str],
) -> np.ndarray:
    matrix = np.zeros((len(starts), len(template_ids)), dtype=np.float64)
    if not template_ids:
        return matrix
    row_of = {start: i for i, start in enumerate(starts)}
    col_of = {template_id: j for j, template_id in enumerate(template_ids)}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT window_start, template_id, event_count
              FROM window_template_counts
             WHERE window_seconds = %s AND template_id = ANY(%s)
            """,
            (window_seconds, template_ids),
        )
        for start, template_id, count in cur.fetchall():
            matrix[row_of[start], col_of[template_id]] = count
    return matrix


def build_split(
    conn: psycopg.Connection,
    window_seconds: int = 60,
    train_fraction: float = 0.7,
    top_templates: int = 20,
) -> Split:
    """Assemble the feature matrix and split it chronologically.

    train_fraction is applied to the ordered list of windows, so the cutoff
    falls on a window boundary and no window is split across the two sets.
    """
    if not 0.0 < train_fraction < 1.0:
        raise ValueError(f"train_fraction must be between 0 and 1, got {train_fraction}")

    starts, base, labels = _fetch_base(conn, window_seconds)
    n_train = int(len(starts) * train_fraction)
    if n_train == 0 or n_train == len(starts):
        raise ValueError(
            f"train_fraction={train_fraction} leaves one side of the split empty "
            f"for {len(starts)} windows"
        )
    cutoff = starts[n_train]

    template_ids = _train_top_templates(conn, window_seconds, cutoff, top_templates)
    templates = _template_matrix(conn, window_seconds, starts, template_ids)

    matrix = np.hstack([base, templates])
    feature_names = BASE_FEATURES + tuple(f"tpl_{tid}" for tid in template_ids)

    return Split(
        feature_names=feature_names,
        x_train=matrix[:n_train],
        y_train=labels[:n_train],
        x_test=matrix[n_train:],
        y_test=labels[n_train:],
        train_starts=tuple(starts[:n_train]),
        test_starts=tuple(starts[n_train:]),
        cutoff=cutoff,
        window_seconds=window_seconds,
        template_features=tuple(f"tpl_{tid}" for tid in template_ids),
    )
