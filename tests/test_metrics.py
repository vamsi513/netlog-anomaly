"""Metric computation."""

from __future__ import annotations

import pytest

from netlog_anomaly.metrics import score


def test_counts_each_cell_of_the_confusion_matrix() -> None:
    result = score([1, 1, 0, 0, 1, 0], [1, 0, 1, 0, 1, 0])
    assert (result.true_positives, result.false_negatives) == (2, 1)
    assert (result.false_positives, result.true_negatives) == (1, 2)
    assert result.total == 6
    assert result.support == 3
    assert result.predicted_positives == 3


def test_perfect_prediction() -> None:
    result = score([1, 0, 1, 0], [1, 0, 1, 0])
    assert (result.precision, result.recall, result.f1) == (1.0, 1.0, 1.0)


def test_known_values() -> None:
    # 3 true positives, 1 false positive, 2 false negatives.
    result = score([1, 1, 1, 1, 1, 0, 0], [1, 1, 1, 0, 0, 1, 0])
    assert result.precision == pytest.approx(3 / 4)
    assert result.recall == pytest.approx(3 / 5)
    assert result.f1 == pytest.approx(2 * (3 / 4) * (3 / 5) / ((3 / 4) + (3 / 5)))
    assert result.f1 == pytest.approx(0.6666666666666666)


def test_predicting_nothing_gives_zero_precision_not_one() -> None:
    result = score([1, 0, 1, 0], [0, 0, 0, 0])
    assert result.predicted_positives == 0
    assert result.precision == 0.0
    assert result.recall == 0.0
    assert result.f1 == 0.0


def test_predicting_everything_gives_full_recall() -> None:
    result = score([1, 0, 0, 0], [1, 1, 1, 1])
    assert result.recall == 1.0
    assert result.precision == pytest.approx(0.25)


def test_no_actual_positives_gives_zero_recall() -> None:
    result = score([0, 0, 0], [1, 0, 0])
    assert result.support == 0
    assert result.recall == 0.0
    assert result.precision == 0.0
    assert result.f1 == 0.0


def test_booleans_and_integers_behave_the_same() -> None:
    as_int = score([1, 0, 1], [1, 1, 0])
    as_bool = score([True, False, True], [True, True, False])
    assert as_int == as_bool


def test_length_mismatch_is_rejected() -> None:
    with pytest.raises(ValueError, match="length mismatch"):
        score([1, 0, 1], [1, 0])


def test_empty_input() -> None:
    result = score([], [])
    assert result.total == 0
    assert (result.precision, result.recall, result.f1) == (0.0, 0.0, 0.0)
