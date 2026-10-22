"""Kill a consumer mid run and check the group loses nothing.

Three members take the six partitions between them. One is killed with SIGKILL
while it is working, so it gets no chance to commit, flush or leave the group
cleanly: the broker only learns it is gone when its session times out. The two
survivors then take its partitions and finish.

What must hold afterwards:

* every window the slice completes is in the index, including the windows the
  killed member was holding in memory and had not written;
* no window start appears in two documents, asked of OpenSearch as an
  aggregation rather than inferred from the ids being unique;
* every window's event count and anomaly flag still match the batch tables, so
  a window rebuilt after the rebalance is a whole window and not a fragment.

The members run as subprocesses rather than threads. SIGKILL on a thread does
not exist, and a cooperative shutdown would not test anything: the point is a
member that stops without warning.
"""

from __future__ import annotations

import signal
import time
from pathlib import Path

import psycopg
import pytest
from streaming_helpers import (
    SOURCE_FILE,
    compare,
    complete_windows,
    duplicate_window_starts,
    fetch_streamed,
)

from netlog_anomaly.streaming.config import Settings
from netlog_anomaly.streaming.measure import start_member
from netlog_anomaly.streaming.producer import replay
from netlog_anomaly.streaming.sink import client

SLICE_LINES = 1_000_000
MEMBERS = 3
IDLE_TIMEOUT = 25.0
# The broker evicts a member one session timeout after it stops heartbeating;
# consumer_config sets that to 10s, so the survivors must outlast it by a
# margin before their own idle timeout ends their run.
START_DEADLINE = 60.0
FINISH_DEADLINE = 300.0


def indexed_count(settings: Settings, index: str) -> int:
    os_client = client(settings)
    if not os_client.indices.exists(index=index):
        return 0
    os_client.indices.refresh(index=index)
    return int(os_client.count(index=index)["count"])


def test_killing_a_consumer_loses_no_window(
    stream_settings: Settings,
    stream_topic: str,
    stream_index: str,
    stream_group: str,
    bgl_log: object,
    batch_conn: psycopg.Connection,
    tmp_path: Path,
) -> None:
    produced = replay(stream_settings, bgl_log, topic=stream_topic, limit=SLICE_LINES)  # type: ignore[arg-type]
    assert produced.published > 0

    members = [
        start_member(
            f"m{index}",
            stream_group,
            stream_topic,
            stream_index,
            tmp_path,
            idle_timeout=IDLE_TIMEOUT,
        )
        for index in range(1, MEMBERS + 1)
    ]

    try:
        # Wait until all three are in the group and documents are landing, so
        # the kill lands on a member that is actually working.
        deadline = time.monotonic() + START_DEADLINE
        while time.monotonic() < deadline:
            if (
                all(member.assigned() for member in members)
                and indexed_count(stream_settings, stream_index) > 0
            ):
                break
            if any(member.process.poll() is not None for member in members):
                pytest.fail(
                    "a member exited before the group was working:\n"
                    + "\n".join(f"--- {m.name} ---\n{m.text()}" for m in members)
                )
            time.sleep(0.2)
        else:
            pytest.fail(
                "the group never reached a working state:\n"
                + "\n".join(f"--- {m.name} ---\n{m.text()}" for m in members)
            )

        victim, survivors = members[1], [members[0], members[2]]
        documents_before_kill = indexed_count(stream_settings, stream_index)
        victim_summary_before = victim.summary()
        assert victim_summary_before is None, "the victim had already finished its run"

        victim.process.send_signal(signal.SIGKILL)
        victim.process.wait(timeout=30)
        assert victim.process.returncode != 0, "SIGKILL did not kill the member"

        for member in survivors:
            member.process.wait(timeout=FINISH_DEADLINE)
    finally:
        for member in members:
            member.kill()

    logs = "\n".join(f"--- {m.name} ---\n{m.text()}" for m in members)
    for member in survivors:
        assert member.process.returncode == 0, (
            f"{member.name} exited {member.process.returncode}\n{logs}"
        )

    summaries = [member.summary() for member in survivors]
    assert all(summary is not None for summary in summaries), (
        f"a survivor printed no summary\n{logs}"
    )

    survivor_partitions = {
        partition
        for summary in summaries
        if summary is not None
        for partition in summary["partitions_seen"]  # type: ignore[union-attr]
    }
    assert survivor_partitions == {0, 1, 2, 3, 4, 5}, (
        f"the survivors only ever saw partitions {sorted(survivor_partitions)}; "
        f"the rebalance did not hand over the killed member's partitions\n{logs}"
    )

    streamed = fetch_streamed(stream_settings, stream_index)
    complete, partial = complete_windows(batch_conn, streamed, SLICE_LINES)
    missing, unexpected, mismatches = compare(streamed, complete)
    duplicates = duplicate_window_starts(stream_settings, stream_index)

    survivor_records = sum(int(summary["records"]) for summary in summaries if summary is not None)
    print(
        f"\nfailure test over the first {SLICE_LINES} lines of {SOURCE_FILE}:\n"
        f"  members started          : {MEMBERS}, one killed with SIGKILL\n"
        f"  documents at kill time   : {documents_before_kill}\n"
        f"  records read by survivors: {survivor_records} of {produced.published} published\n"
        f"  windows in the index     : {len(streamed)}\n"
        f"  complete windows compared: {len(complete)}\n"
        f"  windows missing          : {len(missing)}\n"
        f"  duplicate window starts  : {len(duplicates)}\n"
        f"  field mismatches         : {len(mismatches)}\n"
        f"  partial windows excluded : {len(partial)}"
    )

    assert not missing, f"{len(missing)} windows were lost: {missing[:5]}\n{logs}"
    assert not duplicates, f"duplicate documents for window starts: {duplicates[:5]}"
    assert set(unexpected) <= set(partial), (
        f"streamed windows that are neither complete nor partial: "
        f"{sorted(set(unexpected) - set(partial))[:5]}"
    )
    assert not mismatches, "\n".join(mismatches[:10])
    assert len(complete) > 1000, f"only {len(complete)} windows compared; the slice is too small"
