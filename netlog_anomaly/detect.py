"""Anomaly detectors: a severity rule, a z-score baseline and Isolation Forest.

All three are fitted on the training split only and evaluated on the test
split. What each one is allowed to look at during fitting is stated on the
class, because that is what makes the comparison meaningful.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from sklearn.ensemble import IsolationForest

from netlog_anomaly.features import Split
from netlog_anomaly.metrics import score

RANDOM_SEED = 42


def severity_rule(split: Split, x: np.ndarray) -> np.ndarray:
    """Flag any window containing a FATAL or FAILURE line.

    This is here as a floor, not as a contribution. Every alert-labelled line
    in BGL carries FATAL or FAILURE severity, so this rule has near perfect
    recall by construction and the only question is its precision. A learned
    detector that cannot beat it is not earning its complexity.
    """
    fatal = x[:, split.column("fatal_count")]
    failure = x[:, split.column("failure_count")]
    return (fatal + failure) > 0


@dataclass
class ZScoreBaseline:
    """Threshold on the largest absolute z-score across all features.

    Means and standard deviations come from the training windows. The threshold
    is the one that maximises F1 on the training split, so this baseline does
    use training labels; it is a tuned rule, not an unsupervised one.
    """

    threshold: float = field(default=0.0, init=False)
    train_f1: float = field(default=0.0, init=False)
    _mean: np.ndarray | None = field(default=None, init=False, repr=False)
    _std: np.ndarray | None = field(default=None, init=False, repr=False)

    def fit(self, x_train: np.ndarray, y_train: np.ndarray) -> ZScoreBaseline:
        self._mean = x_train.mean(axis=0)
        std = x_train.std(axis=0)
        # A constant feature carries no information; dividing by one leaves its
        # z-score at zero instead of producing infinities.
        self._std = np.where(std == 0.0, 1.0, std)

        scores = self.decision_scores(x_train)
        self.threshold = _best_f1_threshold(scores, y_train)
        self.train_f1 = score(y_train.tolist(), self.predict(x_train).tolist()).f1
        return self

    def decision_scores(self, x: np.ndarray) -> np.ndarray:
        if self._mean is None or self._std is None:
            raise RuntimeError("fit must be called before scoring")
        return np.abs((x - self._mean) / self._std).max(axis=1)

    def predict(self, x: np.ndarray) -> np.ndarray:
        return self.decision_scores(x) >= self.threshold


def _best_f1_threshold(scores: np.ndarray, y: np.ndarray) -> float:
    """Threshold maximising F1, found by sweeping every candidate cut point.

    Thresholds are evaluated in one pass over the descending score order rather
    than by a nested sweep, which matters at tens of thousands of windows.
    """
    positives = int(y.sum())
    if positives == 0 or scores.size == 0:
        return float("inf")

    order = np.argsort(-scores, kind="stable")
    ordered_labels = y[order].astype(np.int64)
    true_positives = np.cumsum(ordered_labels)
    predicted = np.arange(1, scores.size + 1, dtype=np.int64)

    precision = true_positives / predicted
    recall = true_positives / positives
    denominator = precision + recall
    f1 = np.where(
        denominator > 0, 2 * precision * recall / np.where(denominator > 0, denominator, 1), 0.0
    )

    return float(scores[order][int(np.argmax(f1))])


@dataclass
class IsolationForestDetector:
    """Isolation Forest over the window features.

    Fitted on every training window without labels. The one label-derived
    input is the contamination rate, taken from the training split's own
    anomaly rate; a deployment would have to guess or tune that value.
    """

    n_estimators: int = 200
    random_state: int = RANDOM_SEED
    contamination: float = field(default=0.0, init=False)
    _model: IsolationForest | None = field(default=None, init=False, repr=False)

    def fit(self, x_train: np.ndarray, y_train: np.ndarray) -> IsolationForestDetector:
        rate = float(y_train.mean())
        # Keep within the range scikit-learn accepts for an explicit rate.
        self.contamination = min(max(rate, 1e-6), 0.5)
        self._model = IsolationForest(
            n_estimators=self.n_estimators,
            contamination=self.contamination,
            random_state=self.random_state,
            n_jobs=1,
        ).fit(x_train)
        return self

    def predict(self, x: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("fit must be called before predicting")
        return self._model.predict(x) == -1
