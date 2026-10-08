"""Run every detector over one split and collect the measured scores.

Each learned detector is run twice: once on all features, and once on a
severity-free variant with every severity-derived column removed. The second
variant answers a narrower question than the first, and the README says which.
"""

from __future__ import annotations

from dataclasses import dataclass

import psycopg

from netlog_anomaly.detect import IsolationForestDetector, ZScoreBaseline, severity_rule
from netlog_anomaly.features import (
    SEVERITY_FEATURES,
    Split,
    build_split,
    drop_features,
    fit_validation_split,
)
from netlog_anomaly.metrics import Scores, score
from netlog_anomaly.supervised import gradient_boosting, logistic_regression

ALL_FEATURES = "all features"
SEVERITY_FREE = "severity-free"
NOT_APPLICABLE = "-"


@dataclass(frozen=True, slots=True)
class DetectorResult:
    name: str
    variant: str
    note: str
    scores: Scores
    predictions: tuple[bool, ...]

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
    """Fit and score the two unsupervised detectors on one feature set."""
    y_test = split.y_test.tolist()

    baseline = ZScoreBaseline().fit(split.x_train, split.y_train)
    forest = IsolationForestDetector().fit(split.x_train, split.y_train)

    z_predictions = baseline.predict(split.x_test)
    forest_predictions = forest.predict(split.x_test)

    return (
        DetectorResult(
            name="z-score baseline",
            variant=variant,
            note=(
                f"max |z| >= {baseline.threshold:.4f} over {len(split.feature_names)} features, "
                "threshold tuned on train"
            ),
            scores=score(y_test, z_predictions.tolist()),
            predictions=tuple(bool(v) for v in z_predictions),
        ),
        DetectorResult(
            name="isolation forest",
            variant=variant,
            note=(
                f"{forest.n_estimators} trees over {len(split.feature_names)} features, "
                f"contamination {forest.contamination:.4f}"
            ),
            scores=score(y_test, forest_predictions.tolist()),
            predictions=tuple(bool(v) for v in forest_predictions),
        ),
    )


def _supervised_results(
    split: Split, variant: str, fit_fraction: float
) -> tuple[DetectorResult, ...]:
    """Fit both supervised detectors and score them on the test period.

    The split passed in is already severity-free. Thresholds come from the
    validation slice of the training period, never from the test period.
    """
    slices = fit_validation_split(split, fit_fraction=fit_fraction)
    y_test = split.y_test.tolist()

    results = []
    for detector in (logistic_regression(), gradient_boosting()):
        detector.fit(slices)
        predictions = detector.predict(split.x_test)
        results.append(
            DetectorResult(
                name=detector.name,
                variant=variant,
                note=(
                    f"threshold {detector.threshold:.4f} tuned on a validation slice of "
                    f"{detector.n_validation} train windows; fitted on {detector.n_fit}; "
                    f"validation F1 {detector.validation_f1:.4f}"
                ),
                scores=score(y_test, predictions.tolist()),
                predictions=tuple(bool(v) for v in predictions),
            )
        )
    return tuple(results)


def evaluate_split(split: Split, fit_fraction: float = 0.8) -> Evaluation:
    """Fit every detector on the training windows and score it on the test windows.

    The severity rule needs the severity columns, so it is always scored on the
    full split. The supervised detectors only ever see the reduced one.
    """
    reduced = drop_features(split, SEVERITY_FEATURES)
    rule_predictions = severity_rule(split, split.x_test)

    rule = DetectorResult(
        name="severity rule",
        variant=NOT_APPLICABLE,
        note="window contains a FATAL or FAILURE line; no fitting",
        scores=score(split.y_test.tolist(), rule_predictions.tolist()),
        predictions=tuple(bool(v) for v in rule_predictions),
    )

    results = (
        (rule,)
        + _learned_results(split, ALL_FEATURES)
        + _learned_results(reduced, SEVERITY_FREE)
        + _supervised_results(reduced, SEVERITY_FREE, fit_fraction)
    )
    return Evaluation(split=split, reduced=reduced, results=results)


def evaluate(
    conn: psycopg.Connection,
    window_seconds: int = 60,
    train_fraction: float = 0.7,
    top_templates: int = 20,
    fit_fraction: float = 0.8,
) -> Evaluation:
    split = build_split(
        conn,
        window_seconds=window_seconds,
        train_fraction=train_fraction,
        top_templates=top_templates,
    )
    return evaluate_split(split, fit_fraction=fit_fraction)


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


def store_evaluation(conn: psycopg.Connection, evaluation: Evaluation) -> tuple[int, int]:
    """Write the scores and per-window predictions for this window size.

    Existing rows for the window size are replaced, so re-evaluating leaves one
    set of results rather than accumulating them.
    """
    split = evaluation.split
    window_seconds = split.window_seconds

    with conn.transaction(), conn.cursor() as cur:
        cur.execute("DELETE FROM detector_scores WHERE window_seconds = %s", (window_seconds,))
        cur.execute("DELETE FROM window_predictions WHERE window_seconds = %s", (window_seconds,))

        cur.executemany(
            """
            INSERT INTO detector_scores (
                window_seconds, detector, variant, note, precision, recall, f1,
                true_positives, false_positives, false_negatives, true_negatives
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    window_seconds,
                    result.name,
                    result.variant,
                    result.note,
                    result.scores.precision,
                    result.scores.recall,
                    result.scores.f1,
                    result.scores.true_positives,
                    result.scores.false_positives,
                    result.scores.false_negatives,
                    result.scores.true_negatives,
                )
                for result in evaluation.results
            ],
        )
        scores_written = len(evaluation.results)

        rows = [
            (window_seconds, start, result.name, result.variant, predicted)
            for result in evaluation.results
            for start, predicted in zip(split.test_starts, result.predictions, strict=True)
        ]
        with cur.copy(
            "COPY window_predictions (window_seconds, window_start, detector, variant, "
            "predicted) FROM STDIN"
        ) as copy:
            for row in rows:
                copy.write_row(row)

    return scores_written, len(rows)
