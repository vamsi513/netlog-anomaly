# Streaming mode

The batch pipeline loads `BGL.log` into Postgres and aggregates it with one SQL
`GROUP BY`. Streaming mode replays the same file into Kafka, builds the same 60
second windows as events arrive, scores them and indexes them in OpenSearch.
The two paths share the parser and the window arithmetic and nothing else, so
agreeing on a fixed slice of the log is evidence rather than assumption.

Everything measured here ran on one laptop: ten cores, 16 GB of memory, with
the brokers, OpenSearch, the producer and every consumer sharing them. These
are not cluster numbers.

## 1. Producer: partition by time bucket

`netlog_anomaly/streaming/producer.py` publishes one Kafka record per parsed log
line to the topic `bgl-raw`. The record value is the raw line; the key is the
window the event belongs to:

    key = str(floor(epoch / window_seconds))

Kafka's default partitioner hashes the key, so **every event of one window
lands on one partition, and therefore on one consumer**. That is the whole
reason for this choice.

### Why not partition by node

Partitioning by node is the obvious alternative and it breaks the aggregation.
A 60 second BGL window routinely contains events from hundreds of nodes, so
keying by node would scatter one window across all six partitions and up to six
consumers. None of them could produce that window's `event_count`,
`distinct_nodes`, `distinct_templates` or anomaly flag on its own: `count(*)`
and `count(DISTINCT node)` are not computable from a fragment without a second
stage. Reassembling the window would need either a shuffle into a second topic
keyed by window, or a downstream merge of partial windows with its own state
and its own completeness problem. Keying by window buys exact single pass
aggregation and no shuffle.

### What it costs

Skew. One busy minute of the log is one hot partition, and the load across
partitions follows the log's own time distribution rather than anything
balanced. Measured over the first 200,000 lines, the six partitions received
37,499 / 28,298 / 44,809 / 28,844 / 30,588 / 29,962 events, a 1.58x spread
between the lightest and the heaviest. A window too large for one consumer to
hold also has nowhere else to go, which is a real limit, not a tuning problem.

Verified: over that slice, 587 distinct windows were published and **none of
them appeared on more than one partition**.

The producer parses each line before publishing, for two reasons: the key needs
the event time, and a line the parser rejects would otherwise reach the topic
and have to be rejected again by every consumer. The batch loader quarantines
those lines rather than counting them in a window, so keeping them out of the
topic keeps the two paths over the same input.

`--speed` paces the replay as a multiple of real time; the default, 0, publishes
as fast as the cluster accepts. A seven month log cannot be replayed at 1x, and
the throughput numbers are measured unthrottled.

## 2. Consumers: event time, a watermark, and an explicit lateness policy

`netlog_anomaly/streaming/consumer.py` is one member of a consumer group. Each
member keeps a **separate `WindowAggregator` per assigned partition**
(`netlog_anomaly/streaming/windows.py`). One aggregator across all partitions
would share a watermark, and a partition racing ahead would then close another
partition's windows before their events had arrived.

Window assignment is the same arithmetic the batch pipeline uses in
`aggregate_windows.sql`:

    to_timestamp(div(epoch, window_seconds) * window_seconds)

`test_streaming_windows.py` checks the Python floor against that SQL in
Postgres rather than taking it on trust, because if the two ever disagreed the
parity test would fail with every window offset and no indication why.

### The watermark

Events arrive in roughly, not exactly, event time order, so a window cannot be
emitted the moment an event from the next one appears. The watermark is:

    watermark = highest event time seen - allowed_lateness

and a window `[start, start + window_seconds)` closes once the watermark
reaches its end. `allowed_lateness` (`NETLOG_STREAM_ALLOWED_LATENESS`, default
0 seconds) buys tolerance for out of order arrival at the cost of delaying
every window by that much. It is a straight trade and there is no setting that
avoids it.

### Late events are counted and reported

An event whose window has already been emitted cannot be folded into a result
that has left the process. Those events are **counted, attributed to the window
they belonged to, logged as they happen, and reported in the run summary**,
never silently dropped, because a silently dropped event is a wrong answer
nobody can detect. The aggregator exposes `late_events` and
`late_events_by_window`, each closed window carries its own `late_events`, and
the figure is stored on the OpenSearch document.

Over the measured runs of the BGL replay the late count was **0**: the producer
publishes in file order and the file is close enough to time ordered that,
with a window key per partition, nothing arrived after its window closed. The
policy matters for a real feed, not for this replay, and saying the count was
zero is not the same as saying the path is unexercised:
`test_streaming_windows.py` drives it directly.

### A bounded replay has to end somewhere

`--idle-timeout` closes every remaining window once nothing has arrived for
that long. This is only correct for a replay that is genuinely finished: an idle
topic is not evidence that a window is complete. A consumer meant to run
indefinitely would never do it.

## 3. Scoring: what is reused, and what could not be

Reused from `netlog_anomaly` directly:

| Reused | How |
| --- | --- |
| `parser.parse_line`, `parser.LogEvent` | The producer and the consumer both parse with it. BGL parsing is not restated anywhere in `streaming/`. |
| `features.BASE_FEATURES` | Gives the column order, so a streamed row is the same ten columns in the same positions as a batch row. `scoring.feature_row` builds by name from it, so adding a base feature to the batch pipeline breaks streaming loudly instead of shifting every column by one. |
| `detect.severity_rule` | Called directly. It reads nothing from its split except `column(name)`, so `scoring.FeatureLayout` answers that one question and the batch rule runs unchanged over a single streamed row. |
| `detect.ZScoreBaseline` | Does the z-score arithmetic, rebuilt from stored statistics. |
| `features.build_split` | `streaming/stats.py` uses it to produce the training statistics, with the same arguments the batch evaluation uses. |
| `etl.iter_lines` | The producer reads the log file with it, including its UTF-8 replacement behaviour. |

**A trained model could not be reused cleanly.** `evaluate.py` fits Isolation
Forest, logistic regression and gradient boosting in memory and stores only
their metrics and per window predictions; there is no serialised estimator in
the repository and no code that writes one. Rather than add model persistence,
streaming mode scores with:

* the **severity rule**: flag any window containing a FATAL or FAILURE line,
  the batch function itself; and
* the **z-score**: the largest absolute z-score across the ten base features,
  against means, standard deviations and a threshold exported from the batch
  **training** period only.

Those statistics live in `netlog_anomaly/streaming/training_stats.json`, written
by `python -m netlog_anomaly.streaming export-stats`. The file shipped here was
fitted on the 19,237 training windows of the full BGL run (train fraction 0.7,
cutoff `2005-11-01T20:27:00+00:00`), giving a `max|z|` threshold of 2.5409 at a
training F1 of 0.4643. The file records which features it covers and its own
version, and the loader refuses a file whose feature set does not match the
build, so the two cannot silently drift apart.

The streaming z-score covers the ten base features only, not the per template
counts the batch z-score also sees. A streamed window could carry template
counts, but the column set would then have to be frozen into the statistics
file and matched exactly at scoring time; this is the narrower, honest version.

`is_anomalous` on a window is the dataset's own alert label aggregated over the
window, exactly as `bool_or(is_alert)` does in batch. It is a label, not a
detector output, and nothing in the scoring path reads it.

## 4. Sink: idempotent writes, and commit after write

`netlog_anomaly/streaming/sink.py` bulk indexes scored windows into OpenSearch
with **the window start as the document id**. A window delivered twice is
therefore indexed twice and counted once: the second write replaces the first
with identical content instead of adding a row. The bulk action is `index`, not
`create`, because `create` would reject a re-delivered window as a conflict and
a conflict the consumer has to ignore is harder to reason about than a write
that is simply idempotent.

Offsets are committed only after the bulk write for that batch has succeeded,
and **never past a record whose window is still open**. The safe commit point
for a partition is the lowest offset still feeding an open window, or the next
unread offset when nothing is open. Both rules are needed:

* commit after write, so a crash between the two replays the records rather
  than skipping them;
* never commit past an open window, so a crash cannot leave a window that no
  longer exists in memory and is no longer in anyone's replay range.

Nothing is committed when a partition is revoked. The new owner re-reads from
the last commit, rebuilds those windows and rewrites their documents. A commit
rejected because the group's generation moved on mid batch is not fatal for the
same reason: the offsets were not accepted, so the records stay unread for
whoever owns the partition next. Those rejections are counted and reported as
`commit_conflicts`.

**This is at-least-once delivery with idempotent writes. It is not
exactly-once.** Nothing here prevents a record from being delivered more than
once; the deterministic document id makes the duplicate harmless. Calling it
exactly-once would require the write and the offset commit to be one atomic
operation, which a Kafka consumer and an OpenSearch cluster cannot give you.

One index holds one window size. The id is the window start alone, so indexing
60 second and 300 second windows into the same index would have them overwrite
each other; the window size is stored on every document and the index name is
configurable, which is how two sizes are kept apart.

The mapping is `dynamic: strict`, so a typo in a field name fails the write
instead of quietly creating a second field nobody queries.

## 5. The local stack

`docker-compose.stream.yml` brings up:

* **three Kafka brokers in KRaft combined mode** (`apache/kafka:4.1.0`), each
  running broker and controller in one process, with a fixed cluster id;
* the topic `bgl-raw` with **6 partitions, replication factor 3 and
  `min.insync.replicas=2`**, created by a short lived init container once the
  brokers report healthy;
* `default.replication.factor=3`, `min.insync.replicas=2` and replication
  factor 3 for the internal `__consumer_offsets` and transaction state topics.
  The offsets topic matters here: the failure test kills a consumer, and the
  group can only resume from committed offsets if those offsets survived the
  loss of a broker;
* **one OpenSearch node** (`opensearchproject/opensearch:2.19.3`), single node
  discovery, 1 GB heap, one shard and no replica. A single node cannot place a
  replica, and asking for one would leave the index yellow forever.

**Three brokers did run on this machine.** No memory limit was hit, so the
fallback to one broker was not needed. Observed with `docker stats` on an idle
stack: 325.1 MiB, 335.1 MiB and 333.9 MiB for the three brokers and 1.484 GiB
for OpenSearch, against the 7.748 GiB the Docker VM has, about 2.46 GiB in
total. All six partitions reported `Isr: 3` after start up.

Three brokers rather than one so that replication factor 3 with
`min.insync.replicas=2` is a real setting and not a lie the topic config cannot
honour. With one broker, `acks=all` would mean one copy on one disk.

Every listener is `PLAINTEXT` and OpenSearch runs with its security plugin
disabled. This is a local development stack, not a deployable one.

## Tests

| Test | What it establishes |
| --- | --- |
| `tests/test_streaming_windows.py` | Window assignment, including against the batch SQL in Postgres; watermark advance and the effect of allowed lateness; late events counted, attributed and not folded into an emitted window; the aggregated columns against the batch definitions. 26 cases, no Kafka needed. |
| `tests/test_streaming_parity.py` | Streamed windows against the stored `window_features` rows on a fixed slice, on event count and anomaly flag. |
| `tests/test_streaming_failure.py` | Three members, one killed with `SIGKILL` mid run; after the rebalance, no window lost, no duplicate window start, no field mismatch. |
| `tests/test_streaming_duplicates.py` | The same consumer group rewound to offset 0 and run again: document count unchanged, every document rewritten in place. |

The integration tests use the real services; there are no mocks. They **skip**
rather than fail when Kafka or OpenSearch is not reachable or `data/BGL.log` is
missing, so a checkout without the stack running still gets a green batch
suite. They are not part of the GitHub Actions workflow, which has no Kafka or
OpenSearch; they are run locally against the compose stack.

Each test creates its own topic, index and consumer group and deletes them
afterwards, so one test's retained records and committed offsets cannot change
another's result.

A window is only compared where the slice completes it. A slice cut at a line
number ends in the middle of a window, and the batch table counts that window
over the whole file, so comparing it would be comparing different inputs. Which
windows are complete is asked of the database (no line of the file outside the
slice falls inside the window) rather than assumed from the log being in time
order.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `NETLOG_KAFKA_BOOTSTRAP` | `localhost:19092,localhost:19093,localhost:19094` | All three brokers. A client given one broker that happens to be the one that is down cannot reach the cluster at all. |
| `NETLOG_KAFKA_TOPIC` | `bgl-raw` | Raw log topic. |
| `NETLOG_KAFKA_GROUP` | `netlog-windows` | Consumer group. |
| `NETLOG_OPENSEARCH_URL` | `http://localhost:9200` | Sink. |
| `NETLOG_OPENSEARCH_INDEX` | `netlog-windows` | Index holding scored windows. |
| `NETLOG_STREAM_WINDOW_SECONDS` | `60` | Window size; matches the batch default. |
| `NETLOG_STREAM_ALLOWED_LATENESS` | `0` | Seconds the watermark is held back. |
| `NETLOG_STREAM_STATS` | the packaged `training_stats.json` | Scoring statistics file. |
