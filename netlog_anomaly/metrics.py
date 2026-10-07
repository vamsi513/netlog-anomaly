"""Binary classification metrics, computed directly from the confusion counts."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Scores:
    """Confusion counts and the metrics derived from them."""

    true_positives: int
    false_positives: int
    true_negatives: int
    false_negatives: int

    @property
    def support(self) -> int:
        """Number of actual positives."""
        return self.true_positives + self.false_negatives

    @property
    def predicted_positives(self) -> int:
        return self.true_positives + self.false_positives

    @property
    def total(self) -> int:
        return (
            self.true_positives + self.false_positives + self.true_negatives + self.false_negatives
        )

    @property
    def precision(self) -> float:
        """Zero when nothing was predicted positive.

        There is no agreed value for precision with an empty prediction set.
        Reporting zero keeps a detector that flags nothing from looking perfect.
        """
        if self.predicted_positives == 0:
            return 0.0
        return self.true_positives / self.predicted_positives

    @property
    def recall(self) -> float:
        if self.support == 0:
            return 0.0
        return self.true_positives / self.support

    @property
    def f1(self) -> float:
        precision, recall = self.precision, self.recall
        if precision + recall == 0:
            return 0.0
        return 2 * precision * recall / (precision + recall)


def score(y_true: Sequence[bool | int], y_pred: Sequence[bool | int]) -> Scores:
    """Compare labels with predictions. Both sequences must be the same length."""
    if len(y_true) != len(y_pred):
        raise ValueError(f"length mismatch: {len(y_true)} labels, {len(y_pred)} predictions")

    tp = fp = tn = fn = 0
    for actual, predicted in zip(y_true, y_pred, strict=True):
        if predicted:
            if actual:
                tp += 1
            else:
                fp += 1
        elif actual:
            fn += 1
        else:
            tn += 1
    return Scores(true_positives=tp, false_positives=fp, true_negatives=tn, false_negatives=fn)
