"""Supervised detectors, trained on the training period only.

Both models see the severity-free feature set: window volume, node and template
diversity, and the per-template counts. No severity column reaches them.

Each one fits on the earlier part of the training period and has its decision
threshold chosen on the later part. The threshold is a free parameter and
tuning it on the test period would be leakage; tuning it on the same rows the
model fitted on would flatter it. The model reported is the one fitted on the
fit slice, not a refit on all of training: refitting shifts the score
distribution and the tuned threshold stops meaning what it meant.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from netlog_anomaly.features import FitValidation
from netlog_anomaly.metrics import score

RANDOM_SEED = 42


def _best_f1_threshold(probabilities: np.ndarray, y: np.ndarray) -> float:
    """Probability cut point maximising F1, swept in one pass.

    Candidates are the observed probabilities in descending order, which is
    every threshold that can change a prediction.
    """
    positives = int(y.sum())
    if positives == 0 or probabilities.size == 0:
        return float("inf")

    order = np.argsort(-probabilities, kind="stable")
    true_positives = np.cumsum(y[order].astype(np.int64))
    predicted = np.arange(1, probabilities.size + 1, dtype=np.int64)

    precision = true_positives / predicted
    recall = true_positives / positives
    denominator = precision + recall
    f1 = np.where(
        denominator > 0, 2 * precision * recall / np.where(denominator > 0, denominator, 1), 0.0
    )
    return float(probabilities[order][int(np.argmax(f1))])


@dataclass
class SupervisedDetector:
    """A probabilistic classifier plus a threshold tuned on validation."""

    name: str
    estimator: Pipeline | HistGradientBoostingClassifier
    threshold: float = field(default=0.5, init=False)
    validation_f1: float = field(default=0.0, init=False)
    n_fit: int = field(default=0, init=False)
    n_validation: int = field(default=0, init=False)
    _fitted: bool = field(default=False, init=False, repr=False)

    def fit(self, slices: FitValidation) -> SupervisedDetector:
        self.estimator.fit(slices.x_fit, slices.y_fit)
        self._fitted = True
        self.n_fit = slices.n_fit
        self.n_validation = slices.n_validation

        validation_probabilities = self.probabilities(slices.x_validation)
        self.threshold = _best_f1_threshold(validation_probabilities, slices.y_validation)
        self.validation_f1 = score(
            slices.y_validation.tolist(),
            (validation_probabilities >= self.threshold).tolist(),
        ).f1
        return self

    def probabilities(self, x: np.ndarray) -> np.ndarray:
        if not self._fitted:
            raise RuntimeError("fit must be called before scoring")
        return self.estimator.predict_proba(x)[:, 1]

    def predict(self, x: np.ndarray) -> np.ndarray:
        return self.probabilities(x) >= self.threshold


def logistic_regression() -> SupervisedDetector:
    """Logistic regression on standardised features.

    Scaling is part of the pipeline so it is fitted on the fit slice alone;
    standardising before the split would carry validation and test statistics
    into training.
    """
    return SupervisedDetector(
        name="logistic regression",
        estimator=Pipeline(
            [
                ("scale", StandardScaler()),
                (
                    "model",
                    LogisticRegression(max_iter=2000, random_state=RANDOM_SEED),
                ),
            ]
        ),
    )


def gradient_boosting() -> SupervisedDetector:
    """Histogram-based gradient boosting. Tree splits need no scaling."""
    return SupervisedDetector(
        name="gradient boosting",
        estimator=HistGradientBoostingClassifier(random_state=RANDOM_SEED),
    )
