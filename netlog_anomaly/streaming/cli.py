"""Command line entry point for streaming mode.

python -m netlog_anomaly.streaming create-topic
python -m netlog_anomaly.streaming produce data/BGL.log --limit 200000
python -m netlog_anomaly.streaming export-stats
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from netlog_anomaly import db
from netlog_anomaly.streaming.config import Settings
from netlog_anomaly.streaming.producer import ensure_topic, replay
from netlog_anomaly.streaming.stats import write_statistics


def _create_topic(settings: Settings, args: argparse.Namespace) -> None:
    ensure_topic(
        settings,
        topic=args.topic,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
    )
    print(f"Topic {args.topic or settings.topic!r} is available.")


def _produce(settings: Settings, args: argparse.Namespace) -> None:
    def progress(published: int) -> None:
        print(f"  published {published} events", flush=True)

    ensure_topic(settings, topic=args.topic)
    stats = replay(
        settings,
        args.path,
        topic=args.topic,
        limit=args.limit,
        speed=args.speed,
        on_progress=progress,
    )
    print(f"Published to topic {stats.topic!r} in {stats.elapsed_seconds:.2f}s")
    print(f"  lines read : {stats.lines_read}")
    print(f"  published  : {stats.published}")
    print(f"  rejected   : {stats.rejected}")
    print(f"  events/s   : {stats.events_per_second:.0f}")


def _export_stats(settings: Settings, args: argparse.Namespace) -> None:
    path = args.out or settings.stats_path
    with db.connect(args.database_url) as conn:
        statistics = write_statistics(
            conn,
            path,
            window_seconds=args.window_seconds,
            train_fraction=args.train_fraction,
            source=args.source,
        )
    print(f"Wrote {path}")
    print(f"  features      : {', '.join(statistics.feature_names)}")
    print(f"  train windows : {statistics.n_train_windows}")
    print(f"  train cutoff  : {statistics.train_cutoff}")
    print(f"  max|z| cut    : {statistics.zscore.threshold:.4f}")
    print(f"  train F1      : {statistics.train_f1:.4f}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="netlog-anomaly-stream", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create-topic", help="Create the raw topic if it is missing.")
    create.add_argument("--topic", default=None)
    create.add_argument("--partitions", type=int, default=6)
    create.add_argument("--replication-factor", type=int, default=3)

    produce = subparsers.add_parser("produce", help="Replay a log file into the raw topic.")
    produce.add_argument("path", type=Path)
    produce.add_argument("--topic", default=None)
    produce.add_argument("--limit", type=int, default=None, help="Publish only the first N lines.")
    produce.add_argument(
        "--speed",
        type=float,
        default=0.0,
        help="Multiple of real time; 0 publishes as fast as the cluster accepts.",
    )

    stats = subparsers.add_parser(
        "export-stats", help="Fit the z-score on the batch training windows and store it."
    )
    stats.add_argument("--out", type=Path, default=None)
    stats.add_argument("--database-url", default=None)
    stats.add_argument("--window-seconds", type=int, default=60)
    stats.add_argument("--train-fraction", type=float, default=0.7)
    stats.add_argument("--source", default="BGL.log")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()

    if args.command == "create-topic":
        _create_topic(settings, args)
    elif args.command == "produce":
        _produce(settings, args)
    elif args.command == "export-stats":
        _export_stats(settings, args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
