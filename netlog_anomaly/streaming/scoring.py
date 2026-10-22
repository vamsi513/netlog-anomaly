"""Score one closed window at a time, with no training set in reach.

What is reused from the batch package, and what is not:

* netlog_anomaly.features.BASE_FEATURES gives the column order, so a streamed
  row is the same ten columns in the same positions as a batch row.
* netlog_anomaly.detect.severity_rule is called directly. It only ever asks its
  split for `column(name)`, so a layout object answering that one question is
  enough; the rule itself is not restated here.
* netlog_anomaly.detect.ZScoreBaseline does the z-score arithmetic, rebuilt
  from stored statistics through from_statistics.

No trained model is reused, because the batch pipeline never persists one.
evaluate.py fits Isolation Forest, logistic regression and gradient boosting in
memory and stores only their metrics and per-window predictions; there is no
serialised estimator in the repository and no code that writes one. So
streaming mode scores with the severity rule and the z-score over statistics
exported from the batch training period, and docs/STREAMING.md says so.

The z-score covers the ten base features only, not the per-template counts the
batch z-score also sees. A streamed window could carry template counts, but the
column set would then have to be frozen into the statistics file and matched
exactly at scoring time; the file records which features it covers so the two
cannot silently drift apart.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from netlog_anomaly.detect import ZScoreBaseline, ZScoreStatistics, severity_rule
from netlog_anomaly.features import BASE_FEATURES
from netlog_anomaly.streaming.windows import ClosedWindow

STATISTICS_VERSION = 1


@dataclass(frozen=True, slots=True)
class FeatureLayout:
    """The column positions severity_rule asks for.

    severity_rule is typed against Split, but reads nothing from it except
    column(name). Passing this instead lets the batch rule run unchanged over a
    single streamed row rather than being reimplemented alongside it.
    """

    feature_names: tuple[str, ...]

    def column(self, name: str) -> int:
        return self.feature_names.index(name)


@dataclass(frozen=True, slots=True)
class TrainingStatistics:
    """Everything the stream needs to score, as exported from the batch run."""

    feature_names: tuple[str, ...]
    zscore: ZScoreStatistics
    window_seconds: int
    train_fraction: float
    n_train_windows: int
    train_cutoff: str
    train_f1: float
    source: str

    @classmethod
    def load(cls, path: Path) -> TrainingStatistics:
        payload = json.loads(path.read_text(encoding="utf-8"))
        version = payload.get("version")
        if version != STATISTICS_VERSION:
            raise ValueError(
                f"{path} has statistics version {version!r}, expected {STATISTICS_VERSION}"
            )
        feature_names = tuple(payload["feature_names"])
        if feature_names != BASE_FEATURES:
            raise ValueError(
                f"{path} was exported for features {feature_names}, "
                f"but this build scores {BASE_FEATURES}"
            )
        return cls(
            feature_names=feature_names,
            zscore=ZScoreStatistics(
                mean=tuple(float(value) for value in payload["mean"]),
                std=tuple(float(value) for value in payload["std"]),
                threshold=float(payload["threshold"]),
            ),
            window_seconds=int(payload["window_seconds"]),
            train_fraction=float(payload["train_fraction"]),
            n_train_windows=int(payload["n_train_windows"]),
            train_cutoff=str(payload["train_cutoff"]),
            train_f1=float(payload["train_f1"]),
            source=str(payload["source"]),
        )

    def dump(self, path: Path) -> None:
        payload = {
            "version": STATISTICS_VERSION,
            "feature_names": list(self.feature_names),
            "mean": list(self.zscore.mean),
            "std": list(self.zscore.std),
            "threshold": self.zscore.threshold,
            "window_seconds": self.window_seconds,
            "train_fraction": self.train_fraction,
            "n_train_windows": self.n_train_windows,
            "train_cutoff": self.train_cutoff,
            "train_f1": self.train_f1,
            "source": self.source,
        }
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


@dataclass(frozen=True, slots=True)
class ScoredWindow:
    """A closed window with both detector outputs attached."""

    window: ClosedWindow
    severity_flag: bool
    zscore: float
    zscore_flag: bool

    @property
    def flagged(self) -> bool:
        """Flagged by either detector.

        Either rather than both: the two answer different questions, and a
        window only one of them objects to is still worth a reader's attention.
        """
        return self.severity_flag or self.zscore_flag


def feature_row(window: ClosedWindow) -> np.ndarray:
    """One window as a row in BASE_FEATURES order.

    Built by name from BASE_FEATURES rather than written out as a literal
    tuple, so adding a base feature to the batch pipeline breaks this loudly
    instead of silently shifting every column by one.
    """
    values = {
        "event_count": float(window.event_count),
        "distinct_nodes": float(window.distinct_nodes),
        "distinct_templates": float(window.distinct_templates),
        "info_count": float(window.info_count),
        "warning_count": float(window.warning_count),
        "error_count": float(window.error_count),
        "severe_count": float(window.severe_count),
        "fatal_count": float(window.fatal_count),
        "failure_count": float(window.failure_count),
        "error_rate": float(window.error_rate),
    }
    missing = [name for name in BASE_FEATURES if name not in values]
    if missing:
        raise ValueError(f"streamed window cannot supply base features: {', '.join(missing)}")
    return np.asarray([values[name] for name in BASE_FEATURES], dtype=np.float64)


class WindowScorer:
    """Applies the severity rule and the stored z-score to closed windows."""

    def __init__(self, statistics: TrainingStatistics) -> None:
        self.statistics = statistics
        self.layout = FeatureLayout(statistics.feature_names)
        self.baseline = ZScoreBaseline.from_statistics(statistics.zscore)

    @classmethod
    def from_file(cls, path: Path) -> WindowScorer:
        return cls(TrainingStatistics.load(path))

    def score(self, window: ClosedWindow) -> ScoredWindow:
        row = feature_row(window).reshape(1, -1)
        severity_flag = bool(severity_rule(self.layout, row)[0])  # type: ignore[arg-type]
        zscore = float(self.baseline.decision_scores(row)[0])
        return ScoredWindow(
            window=window,
            severity_flag=severity_flag,
            zscore=zscore,
            zscore_flag=zscore >= self.statistics.zscore.threshold,
        )
