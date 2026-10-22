"""Command line entry point for streaming mode.

python -m netlog_anomaly.streaming create-topic
python -m netlog_anomaly.streaming produce data/BGL.log --limit 200000
python -m netlog_anomaly.streaming export-stats
python -m netlog_anomaly.streaming consume --member c1
python -m netlog_anomaly.streaming measure --mode throughput --limit 1000000
"""

from __future__ import annotations

import argparse
import statistics
import sys
import tempfile
from pathlib import Path

from netlog_anomaly import db
from netlog_anomaly.streaming.config import Settings
from netlog_anomaly.streaming.consumer import WindowConsumer
from netlog_anomaly.streaming.measure import RunResult, delay_run, throughput_run
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


def _consume(settings: Settings, args: argparse.Namespace) -> None:
    consumer = WindowConsumer(
        settings,
        member=args.member,
        group_id=args.group,
        topic=args.topic,
        index=args.index,
        refresh=args.refresh,
        verbose=not args.quiet,
    )
    stats = consumer.run(idle_timeout=args.idle_timeout, max_runtime=args.max_runtime)
    # One machine readable line, so the failure and throughput runs can read
    # back what each member actually did instead of being told.
    print(f"SUMMARY {stats.as_json()}", flush=True)


def _measure(settings: Settings, args: argparse.Namespace) -> None:
    print(
        f"Measuring {args.mode} on this machine over the first {args.limit} lines of "
        f"{args.path}, {args.runs} runs per configuration.\n"
        "Brokers, OpenSearch, the producer and every consumer share this one machine."
    )
    results: dict[int, list[RunResult]] = {}
    with tempfile.TemporaryDirectory(prefix="netlog-measure-") as log_dir:
        for members in args.members:
            results[members] = []
            for run in range(1, args.runs + 1):
                print(f"\n{args.mode}, {members} consumer(s), run {run} of {args.runs}")
                if args.mode == "throughput":
                    result = throughput_run(settings, args.path, args.limit, members, Path(log_dir))
                else:
                    result = delay_run(
                        settings,
                        args.path,
                        args.limit,
                        members,
                        Path(log_dir),
                        target_seconds=args.target_seconds,
                    )
                results[members].append(result)
                print(
                    f"  records {result.records}, windows {result.windows}, "
                    f"span {result.span_seconds:.2f}s, "
                    f"{result.events_per_second:.0f} events/s, "
                    f"delay median {result.delay_median_ms:.0f} ms, "
                    f"p95 {result.delay_p95_ms:.0f} ms, "
                    f"late {result.late_events}, commit conflicts {result.commit_conflicts}"
                )

    print(f"\nMedian of {args.runs} runs, one machine:")
    header = (
        f"{'consumers':>9}  {'events/s':>10}  {'span s':>8}  "
        f"{'delay med ms':>12}  {'delay p95 ms':>12}  {'windows':>8}"
    )
    print(header)
    print("-" * len(header))
    for members in args.members:
        runs = results[members]
        print(
            f"{members:>9}  "
            f"{statistics.median(r.events_per_second for r in runs):>10.0f}  "
            f"{statistics.median(r.span_seconds for r in runs):>8.2f}  "
            f"{statistics.median(r.delay_median_ms for r in runs):>12.0f}  "
            f"{statistics.median(r.delay_p95_ms for r in runs):>12.0f}  "
            f"{statistics.median(r.windows for r in runs):>8.0f}"
        )


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

    consume = subparsers.add_parser(
        "consume", help="Join the consumer group, window the stream and index it."
    )
    consume.add_argument("--member", default="consumer", help="Label used in this member's log.")
    consume.add_argument("--group", default=None)
    consume.add_argument("--topic", default=None)
    consume.add_argument("--index", default=None)
    consume.add_argument(
        "--idle-timeout",
        type=float,
        default=10.0,
        help="Close the remaining windows after this many seconds with no records.",
    )
    consume.add_argument("--max-runtime", type=float, default=1800.0)
    consume.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh the index after every bulk write, so documents are searchable at once.",
    )
    consume.add_argument("--quiet", action="store_true")

    measure = subparsers.add_parser(
        "measure", help="Time the pipeline on this machine with 1, 2 and 3 consumers."
    )
    measure.add_argument("path", type=Path, nargs="?", default=Path("data/BGL.log"))
    measure.add_argument("--mode", choices=("throughput", "delay"), default="throughput")
    measure.add_argument("--limit", type=int, default=1_000_000)
    measure.add_argument("--runs", type=int, default=3)
    measure.add_argument("--members", type=int, nargs="+", default=[1, 2, 3])
    measure.add_argument(
        "--target-seconds",
        type=float,
        default=20.0,
        help="Delay mode only: how long the paced replay should take.",
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
    elif args.command == "consume":
        _consume(settings, args)
    elif args.command == "measure":
        _measure(settings, args)
    elif args.command == "export-stats":
        _export_stats(settings, args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
