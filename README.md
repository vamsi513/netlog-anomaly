# netlog-anomaly

An ETL and anomaly detection pipeline over a public labeled system log dataset.
Raw log lines are parsed and loaded into Postgres, aggregated into fixed time
windows with SQL, and scored by three detectors whose precision, recall and F1
are measured against the dataset's own labels.

The headline result is that **the simplest possible rule wins**. A rule that
flags any window containing a `FATAL` or `FAILURE` line scores F1 0.759 on the
held-out test period, against 0.392 for a tuned z-score baseline and 0.152 for
an Isolation Forest. The numbers below are what the code produced, not what
would make the project look better.

## Dataset

| | |
|---|---|
| Name | BGL (BlueGene/L supercomputer RAS log), from LogHub |
| Source | <https://zenodo.org/records/8196385> (`BGL.zip`), DOI [10.5281/zenodo.8196385](https://doi.org/10.5281/zenodo.8196385) |
| Archive | 57,489,019 bytes; SHA-256 `d67fd82a711aea0157a9b83175892c6ee60e384a2ddf5bc51f39118453816da8` |
| Extracted | `BGL.log`, 743,185,031 bytes, 4,747,963 lines |
| Time span | 2005-06-03 to 2006-01-04 (215 days) |
| Labels | Per line. First field is `-` for a normal line, otherwise an alert category such as `KERNDTLB`. 348,460 lines (7.3%) are alerts. |
| License | CC BY 4.0, as published on the Zenodo record |

The dataset is **not committed to this repository**. `scripts/download_data.sh`
fetches it and verifies the SHA-256 above before extracting; a mismatch aborts.
Re-running the script is a no-op once the archive is present and verified.

Please cite LogHub if you use the data. The collection is published by the
LogPAI team; the Zenodo record above has the canonical citation.

## How to run

Needs Python 3.13 and a reachable Postgres instance.

```bash
git clone <this repo> && cd netlog-anomaly
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
createdb netlog
export NETLOG_DATABASE_URL=postgresql://localhost:5432/netlog

./scripts/download_data.sh                      # ~57 MB download, verified
.venv/bin/python -m netlog_anomaly.pipeline run data/BGL.log --reset
```

`run` does everything. The steps are also separate, and `load` is safe to
re-run:

```bash
python -m netlog_anomaly.pipeline init-db [--reset]
python -m netlog_anomaly.pipeline load data/BGL.log [--limit N]
python -m netlog_anomaly.pipeline aggregate --window-seconds 60
python -m netlog_anomaly.pipeline evaluate --window-seconds 60 --train-fraction 0.7
```

Tests need the same `NETLOG_DATABASE_URL`; they create and drop their own
schema inside that database and never touch the pipeline's tables.

```bash
.venv/bin/python -m pytest
.venv/bin/ruff check . && .venv/bin/ruff format --check .
```

## How it works

**Parse.** BGL lines have nine whitespace-delimited header fields and a
free-form message. The epoch, date, timestamp and severity columns are
validated; the severity check is against BGL's fixed vocabulary (`INFO`,
`WARNING`, `ERROR`, `SEVERE`, `FATAL`, `FAILURE`) rather than a loose pattern,
which is what catches lines whose columns have shifted. Lines that fail go to a
`quarantined_lines` table with a reason instead of being dropped.

The full log ships without event template identifiers, so templates are derived
here: the message is masked (hex values, IP addresses, node identifiers, paths
and integers become placeholders) and the masked string is hashed. This found
19,959 distinct templates. It is a deterministic masking rule, not a learned
log parser such as Drain, and it will split or merge templates that a learned
parser would not.

**Load.** Lines are staged with `COPY`, then moved across with
`INSERT ... ON CONFLICT DO NOTHING` keyed on `(source_file, line_no)`, because
`COPY` cannot skip conflicting rows on its own. Loading the same file twice
inserts nothing the second time and reports the duplicates it skipped. Each
load is one transaction, so a failure part way through a file leaves nothing
behind.

**Aggregate.** SQL buckets events into fixed tumbling windows: event count,
distinct node and template counts, per-severity counts, an error rate, and the
window label. Only non-empty windows are materialised — the log has long idle
stretches, and 215 days of 60-second buckets would be mostly nothing. A window
is labelled anomalous when it contains at least one alert-labelled line, which
comes from the dataset's label field and never from severity.

Per-template counts are kept in a long table rather than pivoted, because which
templates become feature columns has to be decided after the train/test split.

**Window size.** 60 seconds, chosen by measurement rather than preference.
Per-node 60-second windows give 2,930,296 windows for 4.7M events — about 1.6
events each, too sparse to build features from. Hourly windows collapse the
dataset to 3,610 windows at an 18% anomaly rate. Global 60-second windows give
27,482 windows at 6.7%, which is both a usable size and a realistic imbalance.

**Split.** Chronological: the first 70% of windows in time order are the
training set, the rest the test set. Nothing from the test period influences
the training inputs — in particular the 20 template feature columns are the
most frequent templates *in the training windows only*. Selecting them over the
whole dataset would let the test period decide what the model sees, which is
leakage even though no label is read. There is a test for exactly that, using a
template that floods the test period and must not become a feature.

## Detectors

| Detector | What it does | What it sees when fitting |
|---|---|---|
| severity rule | Flags any window with a `FATAL` or `FAILURE` line | Nothing. No fitting. |
| z-score baseline | Flags windows whose largest absolute z-score across all 30 features exceeds a threshold | Training feature means and standard deviations, plus **training labels** to pick the threshold that maximises training F1 |
| isolation forest | scikit-learn `IsolationForest`, 200 trees, seed 42 | Training features, unlabelled, plus the training anomaly rate as the contamination parameter |

The severity rule is in the table as a floor, not as a contribution. Every one
of the 348,460 alert-labelled lines in BGL carries `FATAL` or `FAILURE`
severity, while only 40.7% of `FATAL` lines are alerts. So the rule has perfect
recall by construction and the only open question is its precision. Reporting it
is the difference between knowing whether a learned detector earns its
complexity and assuming it does.

## Measured results

Measured on an **Apple M5 (10 cores, 16 GB RAM), macOS 26.6.2 (arm64), Python
3.13.15, PostgreSQL 17.11, scikit-learn 1.7.2**, over the full 4,747,963-line
log.

Dataset as loaded:

| | |
|---|---|
| Lines read | 4,747,963 |
| Rows loaded | 4,747,643 |
| Quarantined | 320 (316 `unknown_severity`, 4 `nul_byte`) |
| Distinct templates | 19,959 |
| Windows (60s) | 27,482 |
| Train / test windows | 19,237 / 8,245 |
| Split cutoff | 2005-11-01T20:27:00Z |
| Anomalous windows | 1,434 train (7.45%), 404 test (4.90%) |
| Features | 30 (10 base, 20 template columns chosen from train) |

Scores on the **test period only**:

| Detector | Precision | Recall | F1 | TP | FP | FN | TN |
|---|---|---|---|---|---|---|---|
| severity rule | 0.6112 | 1.0000 | **0.7587** | 404 | 257 | 0 | 7584 |
| z-score baseline (max \|z\| ≥ 2.5065) | 0.2736 | 0.6881 | 0.3915 | 278 | 738 | 126 | 7103 |
| isolation forest (200 trees, contamination 0.0745) | 0.1851 | 0.1287 | 0.1518 | 52 | 229 | 352 | 7612 |

Timings on the same machine: 70.3s to parse and load 4.7M lines, 9.7s to build
the windows, under a second to fit and score all three detectors. The loaded
database is about 1.3 GB.

### Reading these numbers

The **trivial rule beats both learned detectors by a wide margin**, and the
Isolation Forest is the worst of the three. That is the honest result, and it is
worth being specific about why rather than presenting it as a surprise:

- BGL's alert labels are not statistical outliers in window volume. Many
  high-traffic windows are entirely normal and many alert windows are small, so
  an unsupervised outlier detector keyed on feature magnitude is looking for the
  wrong thing. The Isolation Forest finds 52 of 404 anomalous test windows.
- The severity rule's recall of exactly 1.0000 is not a sign of a leak. It
  follows from the dataset: an alert line is always `FATAL` or `FAILURE`, so a
  window containing an alert always contains such a line. Its 257 false
  positives are the real information — `FATAL` windows that the BG/L
  administrators did not treat as alerts.
- The train and test anomaly rates differ (7.45% against 4.90%) because the
  split is chronological and the log is not stationary. A random split would
  have matched the rates and inflated every score, which is the reason not to
  use one.
- No hyperparameter search was run for the Isolation Forest, and the z-score
  threshold is the only tuned quantity. A tuned forest would likely do better
  than 0.152; it is not reported here because it was not measured.

## Optional: Airflow DAG

`airflow/dags/netlog_etl.py` sequences the same functions the command line
uses, as four idempotent tasks. It holds no pipeline logic of its own.

Airflow is deliberately kept out of `requirements.txt` — it pins a large
dependency set that the pipeline and its tests do not need — so it installs
into a separate virtual environment:

```bash
python3 -m venv .venv-airflow
.venv-airflow/bin/pip install -r requirements-airflow.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.13.txt
.venv-airflow/bin/pip install -r requirements.txt

export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH="$PWD"
export NETLOG_DATABASE_URL=postgresql://localhost:5432/netlog
export NETLOG_LOG_PATH="$PWD/data/BGL.log"

.venv-airflow/bin/airflow db migrate
.venv-airflow/bin/airflow dags test netlog_etl
```

Verified on Airflow 3.3.2 with Python 3.13.15: all four tasks succeeded, and a
second run left the event and window counts unchanged while adding a row to
`ingest_runs`. The DAG is **not** exercised in CI — only locally, by the command
above.

## CI

`.github/workflows/ci.yml` runs ruff, the full pytest suite against a real
Postgres service container, and a CLI smoke test over the committed fixtures
that loads the same file twice to prove the load is idempotent. Versions are
pinned rather than floating: Python 3.13.15, `postgres:17.11`, ruff 0.14.0,
`ubuntu-24.04`, and actions pinned to commit SHAs. CI does not download the
dataset; it checks that the download script parses and that the Zenodo URL is
reachable.

## Limitations

- **One dataset.** Everything here is measured on BGL alone. Nothing about
  these numbers transfers to HDFS, Thunderbird, or a production log stream, and
  no such claim is made.
- **Offline and batch.** The pipeline reads a complete file. There is no
  streaming ingestion, no incremental window update, and no serving path. It is
  not a real-time detector and is not structured to become one without rework.
- **The task is nearly solved by severity.** Because every BGL alert line is
  `FATAL` or `FAILURE`, this dataset is a weak test of a learned detector. A
  dataset whose labels are not recoverable from a single field would be a more
  honest benchmark.
- **Severity features are included.** `fatal_count` and `failure_count` are
  among the 30 features, so the learned detectors can read the field that
  nearly determines the label. This is not leakage — severity is part of the log
  line, available at inference time — but it does mean the learned scores are
  not independent of the severity signal. The severity rule is reported
  alongside them so the overlap is visible rather than hidden.
- **Hand-rolled templating.** Masking plus hashing is not a log parser. A
  learned parser such as Drain would produce a different, probably better,
  template set, and the template features would change with it.
- **Window labels are coarse.** One alert line makes a whole 60-second window
  anomalous. A window with a single alert and one with two hundred are the same
  label.
- **No hyperparameter search.** The Isolation Forest runs at defaults with a
  contamination rate taken from the training split. The reported 0.152 is that
  configuration's score, not the best achievable.
- **Quarantine is not exhaustive.** Validation catches 320 malformed lines.
  Lines whose columns shifted in a way that still lands a valid severity token
  in the ninth position would pass; the severity allowlist is what makes this
  rare rather than impossible.
- **Rejected lines are buffered in memory** during a load. That is fine when
  rejects are a small minority, which is the only case this is built for.
