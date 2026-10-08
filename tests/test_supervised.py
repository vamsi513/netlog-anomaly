"""Supervised detectors and the nested fit/validation split."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import numpy as np
import pytest

from netlog_anomaly.features import (
    BASE_FEATURES,
    SEVERITY_FEATURES,
    FitValidation,
    Split,
    fit_validation_split,
)
from netlog_anomaly.metrics import score
from netlog_anomaly.supervised import (
    _best_f1_threshold,
    gradient_boosting,
    logistic_regression,
)

START = datetime(2005, 6, 3, 22, 42, tzinfo=UTC)


def starts(n: int, offset: int = 0) -> tuple[datetime, ...]:
    return tuple(START + timedelta(minutes=offset + i) for i in range(n))


def make_split(n_train: int = 400, n_test: int = 200, n_features: int = 5) -> Split:
    """A separable synthetic split: positives sit at a feature offset."""
    rng = np.random.default_rng(0)

    def block(n: int) -> tuple[np.ndarray, np.ndarray]:
        y = np.zeros(n, dtype=bool)
        y[::5] = True
        x = rng.normal(size=(n, n_features))
        x[y] += 2.5
        return x, y

    x_train, y_train = block(n_train)
    x_test, y_test = block(n_test)
    return Split(
        feature_names=tuple(f"f{i}" for i in range(n_features)),
        x_train=x_train,
        y_train=y_train,
        x_test=x_test,
        y_test=y_test,
        train_starts=starts(n_train),
        test_starts=starts(n_test, offset=n_train),
        cutoff=START + timedelta(minutes=n_train),
        window_seconds=60,
        template_features=(),
    )


def test_fit_validation_slices_are_disjoint_and_ordered() -> None:
    split = make_split()
    slices = fit_validation_split(split, fit_fraction=0.8)

    assert slices.n_fit == 320
    assert slices.n_validation == 80
    assert slices.n_fit + slices.n_validation == split.n_train
    assert set(slices.fit_starts).isdisjoint(slices.validation_starts)
    assert max(slices.fit_starts) < slices.cutoff
    assert min(slices.validation_starts) == slices.cutoff


def test_validation_slice_is_the_later_part_of_training() -> None:
    split = make_split()
    slices = fit_validation_split(split, fit_fraction=0.8)
    assert max(slices.fit_starts) < min(slices.validation_starts)
    # And the whole validation slice still precedes the test period.
    assert max(slices.validation_starts) < min(split.test_starts)


def test_no_test_window_reaches_either_slice() -> None:
    split = make_split()
    slices = fit_validation_split(split, fit_fraction=0.8)
    test = set(split.test_starts)
    assert test.isdisjoint(slices.fit_starts)
    assert test.isdisjoint(slices.validation_starts)


def test_fit_validation_rows_match_the_training_rows() -> None:
    split = make_split()
    slices = fit_validation_split(split, fit_fraction=0.8)
    rebuilt = np.vstack([slices.x_fit, slices.x_validation])
    assert rebuilt.tolist() == split.x_train.tolist()
    assert np.concatenate([slices.y_fit, slices.y_validation]).tolist() == split.y_train.tolist()


@pytest.mark.parametrize("fraction", [0.0, 1.0, -0.2, 1.4])
def test_invalid_fit_fraction_is_rejected(fraction: float) -> None:
    with pytest.raises(ValueError, match="fit_fraction"):
        fit_validation_split(make_split(), fit_fraction=fraction)


def test_a_fit_fraction_that_empties_a_slice_is_rejected() -> None:
    with pytest.raises(ValueError, match="empty"):
        fit_validation_split(make_split(n_train=4), fit_fraction=0.01)


@pytest.mark.parametrize("build", [logistic_regression, gradient_boosting])
def test_detector_learns_a_separable_problem(build) -> None:
    split = make_split()
    detector = build().fit(fit_validation_split(split, fit_fraction=0.8))
    result = score(split.y_test.tolist(), detector.predict(split.x_test).tolist())
    assert result.f1 > 0.8


@pytest.mark.parametrize("build", [logistic_regression, gradient_boosting])
def test_threshold_is_recorded_and_used(build) -> None:
    split = make_split()
    slices = fit_validation_split(split, fit_fraction=0.8)
    detector = build().fit(slices)

    probabilities = detector.probabilities(split.x_test)
    assert detector.predict(split.x_test).tolist() == (probabilities >= detector.threshold).tolist()
    assert 0.0 <= detector.threshold <= 1.0


@pytest.mark.parametrize("build", [logistic_regression, gradient_boosting])
def test_threshold_depends_only_on_the_validation_slice(build) -> None:
    # Replacing the test rows must not move a fitted threshold. If it does,
    # the test period is feeding back into tuning.
    split = make_split()
    slices = fit_validation_split(split, fit_fraction=0.8)

    first = build().fit(slices).threshold
    shifted = make_split()
    shifted = Split(
        feature_names=shifted.feature_names,
        x_train=shifted.x_train,
        y_train=shifted.y_train,
        x_test=shifted.x_test * 100.0,
        y_test=shifted.y_test,
        train_starts=shifted.train_starts,
        test_starts=shifted.test_starts,
        cutoff=shifted.cutoff,
        window_seconds=shifted.window_seconds,
        template_features=shifted.template_features,
    )
    second = build().fit(fit_validation_split(shifted, fit_fraction=0.8)).threshold
    assert first == second


@pytest.mark.parametrize("build", [logistic_regression, gradient_boosting])
def test_detector_is_deterministic_for_a_seed(build) -> None:
    split = make_split()
    slices = fit_validation_split(split, fit_fraction=0.8)
    first = build().fit(slices).predict(split.x_test)
    second = build().fit(slices).predict(split.x_test)
    assert first.tolist() == second.tolist()


@pytest.mark.parametrize("build", [logistic_regression, gradient_boosting])
def test_scoring_before_fit_is_an_error(build) -> None:
    with pytest.raises(RuntimeError, match="fit must be called"):
        build().probabilities(np.zeros((2, 5)))


def test_logistic_regression_scales_inside_the_pipeline() -> None:
    # Scaling has to be fitted on the fit slice, so it belongs in the pipeline
    # rather than being applied to the matrix beforehand.
    detector = logistic_regression()
    assert [name for name, _ in detector.estimator.steps] == ["scale", "model"]


def test_best_threshold_matches_a_brute_force_sweep() -> None:
    rng = np.random.default_rng(3)
    probabilities = rng.uniform(size=200)
    labels = probabilities > 0.7
    labels[rng.integers(0, 200, 10)] = True

    chosen = _best_f1_threshold(probabilities, labels)
    chosen_f1 = score(labels.tolist(), (probabilities >= chosen).tolist()).f1
    best = max(score(labels.tolist(), (probabilities >= c).tolist()).f1 for c in probabilities)
    assert chosen_f1 == pytest.approx(best)


def test_best_threshold_with_no_positives_flags_nothing() -> None:
    probabilities = np.array([0.1, 0.4, 0.9])
    threshold = _best_f1_threshold(probabilities, np.zeros(3, dtype=bool))
    assert np.isinf(threshold)
    assert not (probabilities >= threshold).any()


def test_severity_features_are_not_in_the_supervised_feature_names() -> None:
    # Guards the constant itself: every severity column must be a base feature,
    # so dropping them cannot silently miss one.
    assert set(SEVERITY_FEATURES) < set(BASE_FEATURES)
    survivors = tuple(n for n in BASE_FEATURES if n not in SEVERITY_FEATURES)
    assert survivors == ("event_count", "distinct_nodes", "distinct_templates")


def test_fit_validation_is_frozen() -> None:
    slices = fit_validation_split(make_split(), fit_fraction=0.8)
    assert isinstance(slices, FitValidation)
    with pytest.raises(AttributeError):
        slices.cutoff = START  # type: ignore[misc]
