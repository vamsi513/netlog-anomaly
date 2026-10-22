"""Measure streaming mode on this machine: throughput, and end to end delay.

Both numbers are measured on one laptop, with the brokers, OpenSearch, the
producer and every consumer sharing ten cores and one disk. They describe this
stack, not a cluster, and the README says so wherever they appear.

Two modes, because one number cannot answer both questions:

* throughput publishes the whole slice first and then starts the consumers, so
  the group reads a backlog at full speed. That is the rate the pipeline can
  drain a queue at.
* delay starts the consumers first and then publishes at a pace they keep up
  with, so a window's delay is the time the pipeline took rather than the time
  the record spent queued behind a million others. Measuring latency against a
  saturated backlog would measure the backlog.

Each run gets a fresh topic, index and consumer group, so nothing a previous
run committed or retained can change the next one.
"""

from __future__ import annotations

import json
import os
import statistics
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

from confluent_kafka.admin import AdminClient

from netlog_anomaly.streaming.config import Settings
from netlog_anomaly.streaming.producer import ensure_topic, replay
from netlog_anomaly.streaming.sink import client

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ASSIGN_DEADLINE = 60.0
FINISH_DEADLINE = 900.0


@dataclass
class MemberProcess:
    """One consumer running as a subprocess, with its log on disk.

    A subprocess rather than a thread, for two reasons: the consumer group has
    to see independent members, and a test that kills one needs something a
    signal can reach.
    """

    name: str
    process: subprocess.Popen[bytes]
    log_path: Path

    def text(self) -> str:
        try:
            return self.log_path.read_text(encoding="utf-8", errors="replace")
        except FileNotFoundError:
            return ""

    def assigned(self) -> bool:
        return "assigned partitions" in self.text()

    def summary(self) -> dict[str, object] | None:
        """The member's own account of its run, or None if it has not finished."""
        for line in reversed(self.text().splitlines()):
            if line.startswith("SUMMARY "):
                return json.loads(line.removeprefix("SUMMARY "))
        return None

    def wait(self, timeout: float) -> int:
        return self.process.wait(timeout=timeout)

    def kill(self) -> None:
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=30)


def start_member(
    name: str,
    group: str,
    topic: str,
    index: str,
    log_dir: Path,
    idle_timeout: float = 15.0,
) -> MemberProcess:
    """Launch one consumer through the module's own command line."""
    log_path = log_dir / f"{name}.log"
    handle = log_path.open("wb")
    environment = dict(os.environ)
    environment["PYTHONUNBUFFERED"] = "1"
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "netlog_anomaly.streaming",
            "consume",
            "--member",
            name,
            "--group",
            group,
            "--topic",
            topic,
            "--index",
            index,
            "--idle-timeout",
            str(idle_timeout),
        ],
        stdout=handle,
        stderr=subprocess.STDOUT,
        env=environment,
        cwd=PROJECT_ROOT,
    )
    return MemberProcess(name=name, process=process, log_path=log_path)


def wait_for_assignment(members: list[MemberProcess], deadline: float = ASSIGN_DEADLINE) -> None:
    limit = monotonic() + deadline
    while monotonic() < limit:
        if all(member.assigned() for member in members):
            return
        for member in members:
            if member.process.poll() is not None:
                raise RuntimeError(f"{member.name} exited before being assigned:\n{member.text()}")
        sleep(0.2)
    raise TimeoutError("not every member was assigned partitions in time")


@dataclass(frozen=True, slots=True)
class RunResult:
    """One measured run of one configuration."""

    mode: str
    members: int
    published: int
    records: int
    windows: int
    span_seconds: float
    late_events: int
    commit_conflicts: int
    delay_ms: tuple[int, ...]

    @property
    def events_per_second(self) -> float:
        """Records consumed over the span the group was working.

        The span runs from the earliest first-record time any member reported
        to the latest last-work time any member reported, on wall clock so the
        separate processes land on one timeline. It excludes the rebalance
        before the first record and the idle wait that ends the replay.
        """
        if self.span_seconds <= 0:
            return 0.0
        return self.records / self.span_seconds

    @property
    def delay_median_ms(self) -> float:
        return statistics.median(self.delay_ms) if self.delay_ms else 0.0

    @property
    def delay_p95_ms(self) -> float:
        if not self.delay_ms:
            return 0.0
        ordered = sorted(self.delay_ms)
        return float(ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))])


def _fresh_names() -> tuple[str, str, str]:
    token = uuid4().hex[:10]
    return f"measure-raw-{token}", f"measure-windows-{token}", f"measure-group-{token}"


def _drop(settings: Settings, topic: str, index: str) -> None:
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    for future in admin.delete_topics([topic]).values():
        try:
            future.result(timeout=60)
        except Exception as exc:  # noqa: BLE001 - cleanup must not mask a result
            print(f"  could not delete topic {topic}: {exc}")
    os_client = client(settings)
    try:
        if os_client.indices.exists(index=index):
            os_client.indices.delete(index=index)
    except Exception as exc:  # noqa: BLE001
        print(f"  could not delete index {index}: {exc}")


def _collect(
    settings: Settings, index: str, members: list[MemberProcess]
) -> tuple[int, int, int, int, float, tuple[int, ...]]:
    summaries = []
    for member in members:
        summary = member.summary()
        if summary is None:
            raise RuntimeError(f"{member.name} printed no summary:\n{member.text()}")
        summaries.append(summary)

    records = sum(int(summary["records"]) for summary in summaries)
    windows = sum(int(summary["windows_written"]) for summary in summaries)
    late = sum(int(summary["late_events"]) for summary in summaries)
    conflicts = sum(int(summary["commit_conflicts"]) for summary in summaries)

    firsts = [s["first_record_at"] for s in summaries if s["first_record_at"] is not None]
    lasts = [s["last_work_at"] for s in summaries if s["last_work_at"] is not None]
    span = max(float(max(lasts)) - float(min(firsts)), 0.0) if firsts and lasts else 0.0

    return records, windows, late, conflicts, span, _delays(settings, index)


def _delays(settings: Settings, index: str) -> tuple[int, ...]:
    """The ingest_lag_ms of every indexed window.

    Each window carries the gap between the moment its newest event was
    published and the moment its document was written, both read from the same
    machine's clock.
    """
    os_client = client(settings)
    if not os_client.indices.exists(index=index):
        return ()
    os_client.indices.refresh(index=index)
    delays: list[int] = []
    response = os_client.search(
        index=index,
        body={"query": {"exists": {"field": "ingest_lag_ms"}}, "sort": ["_doc"]},
        size=1000,
        scroll="2m",
        _source=["ingest_lag_ms"],
    )
    scroll_id = response.get("_scroll_id")
    try:
        while True:
            hits = response["hits"]["hits"]
            if not hits:
                break
            delays.extend(int(hit["_source"]["ingest_lag_ms"]) for hit in hits)
            response = os_client.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = response.get("_scroll_id")
    finally:
        if scroll_id:
            os_client.clear_scroll(scroll_id=scroll_id)
    return tuple(delays)


def throughput_run(
    settings: Settings, path: Path, limit: int, members: int, log_dir: Path
) -> RunResult:
    """Publish the whole slice, then let the group drain it."""
    topic, index, group = _fresh_names()
    ensure_topic(settings, topic=topic, partitions=6, replication_factor=3)
    try:
        produced = replay(settings, path, topic=topic, limit=limit)
        processes = [
            start_member(f"m{n}", group, topic, index, log_dir) for n in range(1, members + 1)
        ]
        try:
            wait_for_assignment(processes)
            for member in processes:
                member.wait(timeout=FINISH_DEADLINE)
        finally:
            for member in processes:
                member.kill()

        records, windows, late, conflicts, span, delays = _collect(settings, index, processes)
        return RunResult(
            mode="throughput",
            members=members,
            published=produced.published,
            records=records,
            windows=windows,
            span_seconds=span,
            late_events=late,
            commit_conflicts=conflicts,
            delay_ms=delays,
        )
    finally:
        _drop(settings, topic, index)


def event_time_span(path: Path, limit: int) -> int:
    """Seconds of event time the first `limit` lines of a log cover."""
    from netlog_anomaly.etl import iter_lines
    from netlog_anomaly.parser import ParseError, parse_line

    lo: int | None = None
    hi: int | None = None
    for line_no, line in iter_lines(path, limit=limit):
        try:
            event = parse_line(line, line_no)
        except ParseError:
            continue
        lo = event.epoch if lo is None else min(lo, event.epoch)
        hi = event.epoch if hi is None else max(hi, event.epoch)
    if lo is None or hi is None:
        raise ValueError(f"no parsable lines in the first {limit} lines of {path}")
    return max(hi - lo, 1)


def delay_run(
    settings: Settings,
    path: Path,
    limit: int,
    members: int,
    log_dir: Path,
    target_seconds: float = 20.0,
) -> RunResult:
    """Start the group first, then publish at a pace it keeps up with."""
    topic, index, group = _fresh_names()
    ensure_topic(settings, topic=topic, partitions=6, replication_factor=3)
    try:
        processes = [
            start_member(f"m{n}", group, topic, index, log_dir) for n in range(1, members + 1)
        ]
        try:
            wait_for_assignment(processes)
            speed = event_time_span(path, limit) / target_seconds
            produced = replay(settings, path, topic=topic, limit=limit, speed=speed)
            for member in processes:
                member.wait(timeout=FINISH_DEADLINE)
        finally:
            for member in processes:
                member.kill()

        records, windows, late, conflicts, span, delays = _collect(settings, index, processes)
        return RunResult(
            mode="delay",
            members=members,
            published=produced.published,
            records=records,
            windows=windows,
            span_seconds=span,
            late_events=late,
            commit_conflicts=conflicts,
            delay_ms=delays,
        )
    finally:
        _drop(settings, topic, index)
