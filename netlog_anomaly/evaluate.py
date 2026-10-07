"""Run every detector over one split and collect the measured scores.

Each learned detector is run twice: once on all features, and once on a
severity-free variant with every severity-derived column removed. The second
variant answers a narrower question than the first, and the README says which.
"""

from __future__ import annotations

from dataclasses import dataclass

import psycopg

from netlog_anomaly.detect import IsolationForestDetector, ZScoreBaseline, severity_rule
from netlog_anomaly.features import SEVERITY_FEATURES, Split, build_split, drop_features
from netlog_anomaly.metrics import Scores, score

ALL_FEATURES = "all features"
SEVERITY_FREE = "severity-free"
NOT_APPLICABLE = "-"


@dataclass(frozen=True, slots=True)
class DetectorResult:
    name: str
    variant: str
    note: str
    scores: Scores

    @property
    def label(self) -> str:
        if self.variant == NOT_APPLICABLE:
            return self.name
        return f"{self.name} ({self.variant})"


@dataclass(frozen=True, slots=True)
class Evaluation:
    split: Split
    reduced: Split
    results: tuple[DetectorResult, ...]

    @property
    def best(self) -> DetectorResult:
        return max(self.results, key=lambda result: result.scores.f1)


def _learned_results(split: Split, variant: str) -> tuple[DetectorResult, ...]:
    """Fit and score the two learned detectors on one feature set."""
    y_test = split.y_test.tolist()

    baseline = ZScoreBaseline().fit(split.x_train, split.y_train)
    forest = IsolationForestDetector().fit(split.x_train, split.y_train)

    return (
        DetectorResult(
            name="z-score baseline",
            variant=variant,
            note=(
                f"max |z| >= {baseline.threshold:.4f} over {len(split.feature_names)} features, "
                "threshold tuned on train"
            ),
            scores=score(y_test, baseline.predict(split.x_test).tolist()),
        ),
        DetectorResult(
            name="isolation forest",
            variant=variant,
            note=(
                f"{forest.n_estimators} trees over {len(split.feature_names)} features, "
                f"contamination {forest.contamination:.4f}"
            ),
            scores=score(y_test, forest.predict(split.x_test).tolist()),
        ),
    )


def evaluate_split(split: Split) -> Evaluation:
    """Fit every detector on the training windows and score it on the test windows.

    The severity rule needs the severity columns, so it is always scored on the
    full split; only the learned detectors get a reduced variant.
    """
    reduced = drop_features(split, SEVERITY_FEATURES)

    rule = DetectorResult(
        name="severity rule",
        variant=NOT_APPLICABLE,
        note="window contains a FATAL or FAILURE line; no fitting",
        scores=score(split.y_test.tolist(), severity_rule(split, split.x_test).tolist()),
    )

    results = (
        (rule,) + _learned_results(split, ALL_FEATURES) + _learned_results(reduced, SEVERITY_FREE)
    )
    return Evaluation(split=split, reduced=reduced, results=results)


def evaluate(
    conn: psycopg.Connection,
    window_seconds: int = 60,
    train_fraction: float = 0.7,
    top_templates: int = 20,
) -> Evaluation:
    split = build_split(
        conn,
        window_seconds=window_seconds,
        train_fraction=train_fraction,
        top_templates=top_templates,
    )
    return evaluate_split(split)


def format_report(evaluation: Evaluation) -> str:
    """Render the evaluation as plain text, including the split description."""
    split = evaluation.split
    train_rate = float(split.y_train.mean())
    test_rate = float(split.y_test.mean())

    lines = [
        f"Window size      : {split.window_seconds}s",
        f"Windows          : {split.n_train + split.n_test} "
        f"({split.n_train} train, {split.n_test} test)",
        f"Split cutoff     : {split.cutoff.isoformat()} (chronological)",
        f"Train period     : {split.train_starts[0].isoformat()} .. "
        f"{split.train_starts[-1].isoformat()}",
        f"Test period      : {split.test_starts[0].isoformat()} .. "
        f"{split.test_starts[-1].isoformat()}",
        f"Anomalous windows: {int(split.y_train.sum())} train ({train_rate:.2%}), "
        f"{int(split.y_test.sum())} test ({test_rate:.2%})",
        f"Features         : {len(split.feature_names)} all, "
        f"{len(evaluation.reduced.feature_names)} severity-free "
        f"({len(split.template_features)} template columns chosen from train)",
        "",
        f"{'detector':<38} {'precision':>10} {'recall':>10} {'f1':>10} "
        f"{'TP':>7} {'FP':>7} {'FN':>7} {'TN':>7}",
        "-" * 110,
    ]
    for result in evaluation.results:
        s = result.scores
        lines.append(
            f"{result.label:<38} {s.precision:>10.4f} {s.recall:>10.4f} {s.f1:>10.4f} "
            f"{s.true_positives:>7} {s.false_positives:>7} "
            f"{s.false_negatives:>7} {s.true_negatives:>7}"
        )
    lines.append("")
    for result in evaluation.results:
        lines.append(f"  {result.label}: {result.note}")
    lines.append("")
    lines.append(f"Dropped for the severity-free variant: {', '.join(SEVERITY_FEATURES)}")
    lines.append("")
    lines.append(f"Best F1: {evaluation.best.label} ({evaluation.best.scores.f1:.4f})")
    return "\n".join(lines)
