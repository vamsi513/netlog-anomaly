"""Command line entry point for the pipeline.

python -m netlog_anomaly.pipeline init-db
python -m netlog_anomaly.pipeline load data/BGL.log
python -m netlog_anomaly.pipeline aggregate --window-seconds 60
python -m netlog_anomaly.pipeline evaluate --window-seconds 60
python -m netlog_anomaly.pipeline run data/BGL.log
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import psycopg

from netlog_anomaly import db
from netlog_anomaly.etl import load_file, refresh_windows
from netlog_anomaly.evaluate import evaluate, format_report, store_evaluation


def _init_db(conn: psycopg.Connection, reset: bool) -> None:
    if reset:
        db.reset_schema(conn)
        print("Schema dropped and recreated.")
    else:
        db.apply_schema(conn)
        print("Schema applied.")
    conn.commit()


def _load(conn: psycopg.Connection, path: Path, limit: int | None) -> None:
    started = time.monotonic()
    stats = load_file(conn, path, limit=limit)
    elapsed = time.monotonic() - started
    print(f"Loaded {path} as source_file={stats.source_file!r} in {elapsed:.1f}s")
    print(f"  lines read       : {stats.lines_read}")
    print(f"  rows inserted    : {stats.rows_inserted}")
    print(f"  duplicates skipped: {stats.rows_duplicate}")
    print(f"  quarantined      : {stats.lines_quarantined}")
    print(f"  templates seen   : {stats.templates_seen}")


def _aggregate(conn: psycopg.Connection, window_seconds: int) -> None:
    started = time.monotonic()
    windows, template_rows = refresh_windows(conn, window_seconds)
    elapsed = time.monotonic() - started
    print(f"Built {windows} windows of {window_seconds}s in {elapsed:.1f}s")
    print(f"  template count rows: {template_rows}")


def _evaluate(
    conn: psycopg.Connection, window_seconds: int, train_fraction: float, top_templates: int
) -> None:
    evaluation = evaluate(
        conn,
        window_seconds=window_seconds,
        train_fraction=train_fraction,
        top_templates=top_templates,
    )
    print(format_report(evaluation))
    scores, predictions = store_evaluation(conn, evaluation)
    print()
    print(f"Stored {scores} detector scores and {predictions} window predictions.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="netlog-anomaly", description=__doc__)
    parser.add_argument(
        "--database-url",
        default=None,
        help=f"Overrides the {db.ENV_VAR} environment variable.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init-db", help="Create the tables.")
    init.add_argument("--reset", action="store_true", help="Drop existing tables first.")

    load = subparsers.add_parser("load", help="Parse and load a log file. Safe to re-run.")
    load.add_argument("path", type=Path)
    load.add_argument("--limit", type=int, default=None, help="Load only the first N lines.")

    aggregate = subparsers.add_parser("aggregate", help="Rebuild the window tables.")
    aggregate.add_argument("--window-seconds", type=int, default=60)

    evaluate_cmd = subparsers.add_parser("evaluate", help="Score every detector.")
    evaluate_cmd.add_argument("--window-seconds", type=int, default=60)
    evaluate_cmd.add_argument("--train-fraction", type=float, default=0.7)
    evaluate_cmd.add_argument("--top-templates", type=int, default=20)

    run = subparsers.add_parser("run", help="Load, aggregate and evaluate in one go.")
    run.add_argument("path", type=Path)
    run.add_argument("--limit", type=int, default=None)
    run.add_argument("--window-seconds", type=int, default=60)
    run.add_argument("--train-fraction", type=float, default=0.7)
    run.add_argument("--top-templates", type=int, default=20)
    run.add_argument("--reset", action="store_true", help="Drop existing tables first.")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    with db.connect(args.database_url) as conn:
        if args.command == "init-db":
            _init_db(conn, reset=args.reset)
        elif args.command == "load":
            _load(conn, args.path, args.limit)
        elif args.command == "aggregate":
            _aggregate(conn, args.window_seconds)
        elif args.command == "evaluate":
            _evaluate(conn, args.window_seconds, args.train_fraction, args.top_templates)
        elif args.command == "run":
            _init_db(conn, reset=args.reset)
            print()
            _load(conn, args.path, args.limit)
            print()
            _aggregate(conn, args.window_seconds)
            print()
            _evaluate(conn, args.window_seconds, args.train_fraction, args.top_templates)
    return 0


if __name__ == "__main__":
    sys.exit(main())
