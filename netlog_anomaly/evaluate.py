"""Run every detector over one split and collect the measured scores."""

from __future__ import annotations

from dataclasses import dataclass

import psycopg

from netlog_anomaly.detect import IsolationForestDetector, ZScoreBaseline, severity_rule
from netlog_anomaly.features import Split, build_split
from netlog_anomaly.metrics import Scores, score


@dataclass(frozen=True, slots=True)
class DetectorResult:
    name: str
    note: str
    scores: Scores


@dataclass(frozen=True, slots=True)
class Evaluation:
    split: Split
    results: tuple[DetectorResult, ...]
    zscore_threshold: float
    contamination: float

    @property
    def best(self) -> DetectorResult:
        return max(self.results, key=lambda result: result.scores.f1)


def evaluate_split(split: Split) -> Evaluation:
    """Fit each detector on the training windows and score it on the test windows."""
    y_test = split.y_test.tolist()

    baseline = ZScoreBaseline().fit(split.x_train, split.y_train)
    forest = IsolationForestDetector().fit(split.x_train, split.y_train)

    results = (
        DetectorResult(
            name="severity rule",
            note="window contains a FATAL or FAILURE line",
            scores=score(y_test, severity_rule(split, split.x_test).tolist()),
        ),
        DetectorResult(
            name="z-score baseline",
            note=f"max |z| >= {baseline.threshold:.4f}, threshold tuned on train",
            scores=score(y_test, baseline.predict(split.x_test).tolist()),
        ),
        DetectorResult(
            name="isolation forest",
            note=f"{forest.n_estimators} trees, contamination {forest.contamination:.4f}",
            scores=score(y_test, forest.predict(split.x_test).tolist()),
        ),
    )
    return Evaluation(
        split=split,
        results=results,
        zscore_threshold=baseline.threshold,
        contamination=forest.contamination,
    )


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
        f"Features         : {len(split.feature_names)} "
        f"({len(split.template_features)} template columns chosen from train)",
        "",
        f"{'detector':<20} {'precision':>10} {'recall':>10} {'f1':>10} "
        f"{'TP':>7} {'FP':>7} {'FN':>7} {'TN':>7}",
        "-" * 92,
    ]
    for result in evaluation.results:
        s = result.scores
        lines.append(
            f"{result.name:<20} {s.precision:>10.4f} {s.recall:>10.4f} {s.f1:>10.4f} "
            f"{s.true_positives:>7} {s.false_positives:>7} "
            f"{s.false_negatives:>7} {s.true_negatives:>7}"
        )
    lines.append("")
    for result in evaluation.results:
        lines.append(f"  {result.name}: {result.note}")
    lines.append("")
    lines.append(f"Best F1: {evaluation.best.name} ({evaluation.best.scores.f1:.4f})")
    return "\n".join(lines)
