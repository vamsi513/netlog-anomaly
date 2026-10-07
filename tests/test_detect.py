"""Detector behaviour, on feature matrices built directly rather than via SQL."""

from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pytest

from netlog_anomaly.detect import (
    IsolationForestDetector,
    ZScoreBaseline,
    _best_f1_threshold,
    severity_rule,
)
from netlog_anomaly.features import BASE_FEATURES, Split
from netlog_anomaly.metrics import score


def make_split(x_train: np.ndarray, y_train: np.ndarray, x_test: np.ndarray, y_test: np.ndarray):
    start = datetime(2005, 6, 3, 22, 42, tzinfo=UTC)
    return Split(
        feature_names=BASE_FEATURES,
        x_train=x_train,
        y_train=y_train,
        x_test=x_test,
        y_test=y_test,
        train_starts=tuple(start for _ in range(len(x_train))),
        test_starts=tuple(start for _ in range(len(x_test))),
        cutoff=start,
        window_seconds=60,
        template_features=(),
    )


def rows(*values: tuple[float, ...]) -> np.ndarray:
    return np.asarray(values, dtype=np.float64)


def quiet_row(fatal: float = 0.0, failure: float = 0.0, event_count: float = 10.0):
    """A base feature row with everything at a nominal value except severity."""
    row = {name: 0.0 for name in BASE_FEATURES}
    row["event_count"] = event_count
    row["distinct_nodes"] = 1.0
    row["distinct_templates"] = 1.0
    row["info_count"] = event_count - fatal - failure
    row["fatal_count"] = fatal
    row["failure_count"] = failure
    row["error_rate"] = (fatal + failure) / event_count if event_count else 0.0
    return tuple(row[name] for name in BASE_FEATURES)


def test_severity_rule_flags_fatal_and_failure() -> None:
    x = rows(quiet_row(), quiet_row(fatal=3), quiet_row(failure=1), quiet_row())
    split = make_split(x, np.zeros(4, dtype=bool), x, np.zeros(4, dtype=bool))
    assert severity_rule(split, x).tolist() == [False, True, True, False]


def test_severity_rule_ignores_other_severities() -> None:
    row = {name: 0.0 for name in BASE_FEATURES}
    row["event_count"] = 10.0
    row["error_count"] = 5.0
    row["severe_count"] = 5.0
    row["error_rate"] = 1.0
    x = rows(tuple(row[name] for name in BASE_FEATURES))
    split = make_split(x, np.zeros(1, dtype=bool), x, np.zeros(1, dtype=bool))
    assert severity_rule(split, x).tolist() == [False]


def test_zscore_flags_the_outlier() -> None:
    rng = np.random.default_rng(0)
    normal = rng.normal(loc=10.0, scale=1.0, size=(200, 3))
    train = np.vstack([normal, [[100.0, 100.0, 100.0]]])
    y_train = np.array([False] * 200 + [True])

    baseline = ZScoreBaseline().fit(train, y_train)
    predictions = baseline.predict(np.array([[10.0, 10.0, 10.0], [100.0, 100.0, 100.0]]))
    assert predictions.tolist() == [False, True]


def test_zscore_threshold_comes_from_training_only() -> None:
    rng = np.random.default_rng(1)
    train = rng.normal(size=(100, 2))
    y_train = np.zeros(100, dtype=bool)
    y_train[:5] = True
    baseline = ZScoreBaseline().fit(train, y_train)
    before = baseline.threshold

    # Scoring a different matrix must not move the fitted threshold.
    baseline.predict(rng.normal(loc=50.0, size=(100, 2)))
    assert baseline.threshold == before


def test_zscore_survives_a_constant_feature() -> None:
    train = np.hstack([np.arange(50, dtype=np.float64).reshape(-1, 1), np.ones((50, 1))])
    y_train = np.zeros(50, dtype=bool)
    y_train[-3:] = True
    baseline = ZScoreBaseline().fit(train, y_train)
    scores = baseline.decision_scores(train)
    assert np.isfinite(scores).all()


def test_zscore_requires_fit_before_scoring() -> None:
    with pytest.raises(RuntimeError, match="fit must be called"):
        ZScoreBaseline().predict(np.zeros((2, 2)))


def test_best_threshold_separates_a_clean_split() -> None:
    scores = np.array([0.1, 0.2, 0.3, 5.0, 6.0])
    labels = np.array([False, False, False, True, True])
    threshold = _best_f1_threshold(scores, labels)
    assert (scores >= threshold).tolist() == labels.tolist()


def test_best_threshold_with_no_positives_flags_nothing() -> None:
    scores = np.array([0.1, 0.2, 0.3])
    threshold = _best_f1_threshold(scores, np.zeros(3, dtype=bool))
    assert np.isinf(threshold)
    assert not (scores >= threshold).any()


def test_best_threshold_maximises_f1_against_a_brute_force_sweep() -> None:
    rng = np.random.default_rng(7)
    scores = rng.normal(size=120)
    labels = scores > 1.0
    labels[rng.integers(0, 120, 8)] = True  # add noise so the split is imperfect

    chosen = _best_f1_threshold(scores, labels)
    chosen_f1 = score(labels.tolist(), (scores >= chosen).tolist()).f1
    best_seen = max(
        score(labels.tolist(), (scores >= candidate).tolist()).f1 for candidate in scores
    )
    assert chosen_f1 == pytest.approx(best_seen)


def test_isolation_forest_is_deterministic_for_a_seed() -> None:
    rng = np.random.default_rng(3)
    train = rng.normal(size=(300, 4))
    y_train = np.zeros(300, dtype=bool)
    y_train[:15] = True

    first = IsolationForestDetector().fit(train, y_train).predict(train)
    second = IsolationForestDetector().fit(train, y_train).predict(train)
    assert first.tolist() == second.tolist()


def test_isolation_forest_contamination_comes_from_the_training_rate() -> None:
    train = np.random.default_rng(4).normal(size=(200, 3))
    y_train = np.zeros(200, dtype=bool)
    y_train[:20] = True
    forest = IsolationForestDetector().fit(train, y_train)
    assert forest.contamination == pytest.approx(0.1)


def test_isolation_forest_contamination_stays_in_range_with_no_positives() -> None:
    train = np.random.default_rng(5).normal(size=(50, 2))
    forest = IsolationForestDetector().fit(train, np.zeros(50, dtype=bool))
    assert 0.0 < forest.contamination <= 0.5


def test_isolation_forest_requires_fit_before_predicting() -> None:
    with pytest.raises(RuntimeError, match="fit must be called"):
        IsolationForestDetector().predict(np.zeros((2, 2)))
