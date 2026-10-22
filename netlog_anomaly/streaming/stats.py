"""Export the batch training statistics the stream scores with.

The split is built by netlog_anomaly.features.build_split with the same
arguments the batch evaluation uses, then reduced to the base features alone,
and ZScoreBaseline is fitted on its training half. Only the training windows
are touched: the statistics must not carry anything from the period the batch
evaluation reports its numbers on.
"""

from __future__ import annotations

from pathlib import Path

import psycopg

from netlog_anomaly.detect import ZScoreBaseline
from netlog_anomaly.features import BASE_FEATURES, build_split
from netlog_anomaly.streaming.scoring import TrainingStatistics


def export_statistics(
    conn: psycopg.Connection,
    window_seconds: int = 60,
    train_fraction: float = 0.7,
    source: str = "BGL.log",
) -> TrainingStatistics:
    """Fit the z-score over the base features of the training windows."""
    # top_templates=0 asks build_split for the base columns only, which is the
    # column set a streamed window can supply.
    split = build_split(
        conn,
        window_seconds=window_seconds,
        train_fraction=train_fraction,
        top_templates=0,
    )
    if split.feature_names != BASE_FEATURES:
        raise ValueError(f"expected the base features, got {split.feature_names}")
    baseline = ZScoreBaseline().fit(split.x_train, split.y_train)

    return TrainingStatistics(
        feature_names=split.feature_names,
        zscore=baseline.statistics(),
        window_seconds=window_seconds,
        train_fraction=train_fraction,
        n_train_windows=split.n_train,
        train_cutoff=split.cutoff.isoformat(),
        train_f1=baseline.train_f1,
        source=source,
    )


def write_statistics(
    conn: psycopg.Connection,
    path: Path,
    window_seconds: int = 60,
    train_fraction: float = 0.7,
    source: str = "BGL.log",
) -> TrainingStatistics:
    statistics = export_statistics(
        conn,
        window_seconds=window_seconds,
        train_fraction=train_fraction,
        source=source,
    )
    statistics.dump(path)
    return statistics
