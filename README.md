# netlog-anomaly

An ETL and anomaly detection pipeline over a public labeled system log dataset.
Raw log lines are parsed and loaded into Postgres, aggregated into fixed time
windows with SQL, and scored by three detectors whose precision, recall and F1
are measured against the dataset's own labels.

The headline result is that **the simplest possible rule wins**. A rule that
flags any window containing a `FATAL` or `FAILURE` line scores F1 0.759 on the
held-out test period. The best learned detector, gradient boosting on features
with every severity column removed, reaches 0.553. Strip the severity features
from the unsupervised detectors and the z-score collapses from 0.392 to 0.013
while the Isolation Forest does not move, which says the z-score was reading
severity and the forest never was. The numbers below are what the code
produced, not what would make the project look better.

There is also a small dashboard: a read-only FastAPI service over the results
tables and a Next.js page that charts the window timeline, shows the detector
comparison and drills into a single window's templates.

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
| z-score baseline | Flags windows whose largest absolute z-score across the feature set exceeds a threshold | Training feature means and standard deviations, plus **training labels** to pick the threshold that maximises training F1 |
| isolation forest | scikit-learn `IsolationForest`, 200 trees, seed 42 | Training features, unlabelled, plus the training anomaly rate as the contamination parameter |

Two supervised detectors are also fitted, on the severity-free set only:

| Detector | What it does | What it sees when fitting |
|---|---|---|
| logistic regression | `LogisticRegression` on standardised features, inside a pipeline so the scaler is fitted on the fit slice alone | The fit slice's features **and labels**, plus the validation slice to choose a probability threshold |
| gradient boosting | `HistGradientBoostingClassifier`, seed 42, defaults | The same |

These need a decision threshold, and where that threshold is chosen decides
whether the result means anything. The training period is therefore split a
second time, in time order: the model fits on the **earlier 80%** of training
windows and the threshold that maximises F1 is chosen on the **later 20%**. The
test period is touched by neither step. The model reported is the one fitted on
the fit slice rather than a refit on all of training, because refitting shifts
the score distribution and the tuned threshold stops meaning what it meant.

The unsupervised detectors are run on two feature sets:

- **all features** — all 30 columns, including the per-severity counts and the
  error rate.
- **severity-free** — the same split with all seven severity-derived columns
  dropped (`info_count`, `warning_count`, `error_count`, `severe_count`,
  `fatal_count`, `failure_count`, `error_rate`), leaving 23: event count,
  distinct node and template counts, and the 20 template columns. `error_rate`
  is dropped along with the raw counts because it is a ratio of them, and
  keeping it would leave the severity signal in a variant that claims not to
  use it.

Both variants are built from the same split and scored on identical windows
with identical labels, so the difference between them is the feature set and
nothing else. The severity rule is unaffected — it reads the severity columns
directly and is always scored on the full split.

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
| Features | 30 all features; 23 severity-free (20 template columns chosen from train) |

Scores on the **test period only**:

| Detector | Supervised | Features | Precision | Recall | F1 | TP | FP | FN | TN |
|---|---|---|---|---|---|---|---|---|---|
| severity rule | no | — | 0.6112 | 1.0000 | **0.7587** | 404 | 257 | 0 | 7584 |
| gradient boosting | yes | severity-free, 23 | 0.5662 | 0.5396 | 0.5526 | 218 | 167 | 186 | 7674 |
| logistic regression | yes | severity-free, 23 | 0.4424 | 0.3614 | 0.3978 | 146 | 184 | 258 | 7657 |
| z-score baseline | no | all 30 | 0.2736 | 0.6881 | 0.3915 | 278 | 738 | 126 | 7103 |
| isolation forest | no | all 30 | 0.1851 | 0.1287 | 0.1518 | 52 | 229 | 352 | 7612 |
| isolation forest | no | severity-free, 23 | 0.1889 | 0.1262 | 0.1513 | 51 | 219 | 353 | 7622 |
| z-score baseline | no | severity-free, 23 | 0.0140 | 0.0124 | 0.0131 | 5 | 353 | 399 | 7488 |

Tuned thresholds: max \|z\| ≥ 2.5065 on all features and ≥ 6.2576
severity-free; probability ≥ 0.6647 for gradient boosting and ≥ 0.0678 for
logistic regression, both chosen on a 3,848 window validation slice after
fitting on 15,389. Both forests ran at 200 trees with contamination 0.0745, the
training anomaly rate.

Timings on the same machine: 70.3s to parse and load 4.7M lines and 9.7s to
build the windows, with under a second to fit and score all three detectors.
A repeat of the whole run from a clean `git archive` export, in a fresh
virtual environment built from the pinned requirements, measured 74.5s and 9.2s
for the same two steps and reproduced the table above value for value. The loaded
database is about 1.3 GB.

### Reading these numbers

The **trivial rule still beats every learned detector**, including both
supervised ones, and it does so without being fitted to anything. That is the
honest result, and it is worth being specific about why rather than presenting
it as a surprise:

- The severity rule's recall of exactly 1.0000 is not a sign of a leak. It
  follows from the dataset: an alert line is always `FATAL` or `FAILURE`, so a
  window containing an alert always contains such a line. Its 257 false
  positives are the real information — `FATAL` windows that the BG/L
  administrators did not treat as alerts.
- **The z-score baseline was reading severity and little else.** Removing the
  severity columns takes it from F1 0.3915 to 0.0131 — from a mediocre detector
  to one no better than guessing. Its 0.3915 was not evidence that window
  statistics detect anomalies; it was a noisier route to the severity signal
  the rule reads directly.
- **The Isolation Forest was not using severity at all.** It scores 0.1518 with
  those columns and 0.1513 without, a difference of five hundredths of a
  percent. The severity features were available to it and it did not exploit
  them; its weakness is not a missing feature.
- **Supervision is worth a lot, and still not enough.** Gradient boosting on
  the severity-free features reaches 0.5526 against 0.1513 for the best
  unsupervised detector on the same 23 columns. Labels during training are what
  closes most of that gap. It is still 0.21 short of a rule that reads one
  field and is fitted to nothing.
- **Both supervised models generalise worse than their validation suggested.**
  Gradient boosting scored F1 0.7423 on the validation slice and 0.5526 on the
  test period; logistic regression scored 0.5517 and 0.3978. The validation
  slice is adjacent in time to the fit slice while the test period is months
  later, so the drop is the log changing, not a tuning mistake. Quoting the
  validation figure as the result would overstate both models by roughly 0.16.
- BGL's alert labels are not statistical outliers in window volume. Many
  high-traffic windows are entirely normal and many alert windows are small, so
  an unsupervised detector keyed on feature magnitude is looking for the wrong
  thing. That is the same conclusion from both directions: the forest finds 52
  of 404 anomalous test windows with severity and 51 without.
- The train and test anomaly rates differ (7.45% against 4.90%) because the
  split is chronological and the log is not stationary. A random split would
  have matched the rates and inflated every score, which is the reason not to
  use one.
- No hyperparameter search was run for either forest, and the z-score threshold
  is the only tuned quantity. A tuned forest would likely beat 0.152; it is not
  reported here because it was not measured.

### What the severity-free variant does and does not show

**It shows** that the unsupervised detectors do not recover the labels from
window volume, node spread and template mix alone: the z-score drops to 0.0131
and the forest stays at 0.1513. It also shows that a supervised model on those
same 23 columns does find real signal in them — gradient boosting reaches
0.5526 — so the features are not empty; the unsupervised methods were simply
asking the wrong question of them.

**It does not show** that the variant is free of every correlate of severity.
The 20 template columns survive the cut, and some of those templates *are* the
messages that carry `FATAL` severity — `data TLB error interrupt` is a template
and a fatal condition at once. The variant removes the severity **column**, not
every trace of the information in it. This matters most for the supervised
models: gradient boosting's 0.5526 is partly it learning which templates are
fatal ones, which is a different achievement from detecting anomalies without
severity information. A genuinely severity-independent test would need features
built only from volume and timing, which is not what is measured here.

**It also does not show** that severity features are illegitimate. Severity is
part of the log line and available at inference time, so using it is fair. The
variant exists to make visible how much of each learned score depends on it,
not to argue the full-feature numbers are invalid.

## Dashboard

A read-only API and a single page over the results the pipeline stored. Run the
pipeline first; the dashboard displays what is in Postgres and nothing else.

```bash
# API, from the project virtual environment
pip install -r requirements-api.txt
export NETLOG_DATABASE_URL=postgresql://localhost:5432/netlog
uvicorn netlog_anomaly.api:app --port 8000

# Page, in another shell
cd frontend
npm ci
npm run dev           # http://localhost:3000
```

If the page is served from anywhere other than port 3000, set
`NETLOG_CORS_ORIGINS` for the API to that origin, and
`NEXT_PUBLIC_API_BASE_URL` for the page to the API's.

| Route | Returns |
|---|---|
| `GET /api/health` | Liveness plus row counts |
| `GET /api/summary` | Window counts, anomaly rate and the span covered |
| `GET /api/detectors` | The comparison table, best F1 first |
| `GET /api/windows` | Windows in time order with labels and per-detector flags |
| `GET /api/windows/{window_start}/templates` | One window's stats and top templates |

Every route is a GET, every statement is a SELECT, and the connection is opened
read-only, so the dashboard cannot change what was measured. Detector flags
exist only for the scored test period; elsewhere the flag map is empty rather
than all-negative, which would read as every detector agreeing when in fact
none of them ran there.

The page shows the timeline on a **log scale**: event counts per window run
from 1 to over 16,000 and most windows hold a single event, so a linear axis
renders nearly everything as a flat line.

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

`.github/workflows/ci.yml` has two jobs. The first runs ruff, the full pytest
suite of 165 tests against a real Postgres service container, and a CLI smoke
test over the committed fixtures that loads the same file twice to prove the
load is idempotent. The second installs the frontend with `npm ci`, which
installs exactly the lockfile and fails if it disagrees with `package.json`,
then lints, typechecks and builds it.

Versions are pinned rather than floating: Python 3.13.15, `postgres:17.11`,
ruff 0.14.0, Node 24.14.1, Next 16.4.0, TypeScript 5.9.3, `ubuntu-24.04`, and
actions pinned to commit SHAs. CI does not download the dataset; it checks that
the download script parses and that the Zenodo URL is reachable.

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
- **Severity features are included in the full variant,** so the learned
  detectors can read the field that nearly determines the label. This is not
  leakage — severity is part of the log line, available at inference time — but
  it does mean those scores are not independent of the severity signal. The
  severity-free variant and the severity rule are both reported so the overlap
  is visible rather than hidden.
- **The severity-free variant is not severity-independent.** Dropping the seven
  severity columns leaves the 20 template columns, and some templates are
  themselves fatal messages. It measures the cost of removing the severity
  field, not of removing the information.
- **Five of the seven reported detector runs are poor.** The best learned F1 is
  0.5526 and the next is 0.3978; the remaining four sit between 0.15 and 0.01.
  This is a measurement of these detectors on this dataset with these features,
  not evidence that log anomaly detection does not work.
- **The supervised results are single configurations, not tuned ceilings.**
  Both models run at scikit-learn defaults with only the decision threshold
  chosen. No search over depth, regularisation, learning rate or class weights
  was run, and no cross-validation: there is one fit slice and one validation
  slice. A tuned model would likely score higher.
- **Validation and test scores differ substantially** for both supervised
  models, by roughly 0.16 F1. Only the test numbers are reported as results.
  Anyone reusing this split should expect the same gap rather than treating the
  validation figure as the achievable score.
- **Hand-rolled templating.** Masking plus hashing is not a log parser. A
  learned parser such as Drain would produce a different, probably better,
  template set, and the template features would change with it.
- **Window labels are coarse.** One alert line makes a whole 60-second window
  anomalous. A window with a single alert and one with two hundred are the same
  label.
- **No hyperparameter search.** Both forests run at defaults with a
  contamination rate taken from the training split. The reported 0.152 and
  0.151 are those configurations' scores, not the best achievable.
- **Quarantine is not exhaustive.** Validation catches 320 malformed lines.
  Lines whose columns shifted in a way that still lands a valid severity token
  in the ninth position would pass; the severity allowlist is what makes this
  rare rather than impossible.
- **Rejected lines are buffered in memory** during a load. That is fine when
  rejects are a small minority, which is the only case this is built for.
- **The dashboard shows stored results, not live scoring.** Detector flags come
  from the predictions the last `evaluate` run wrote. Nothing is scored on
  demand, and a window outside the test period has no flags at all.
- **The dashboard has no authentication.** It is read-only and intended for
  localhost. Exposing it would publish the contents of the results tables to
  anyone who can reach it.

## License

MIT. See [LICENSE](LICENSE).

The BGL dataset is **not** covered by this license. It is published on Zenodo
under CC BY 4.0 and is not redistributed here; `scripts/download_data.sh`
fetches it from the original source.
