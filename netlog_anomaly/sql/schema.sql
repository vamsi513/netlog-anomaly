-- Schema for the log ETL and window aggregation.
--
-- Idempotency: log_events and quarantined_lines are keyed on
-- (source_file, line_no), so loading the same file twice inserts nothing the
-- second time. Loads go through an unlogged staging table and then an
-- INSERT ... ON CONFLICT DO NOTHING, because COPY itself cannot skip conflicts.

CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id            BIGSERIAL PRIMARY KEY,
    source_file       TEXT        NOT NULL,
    window_seconds    INTEGER,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    lines_read        BIGINT      NOT NULL DEFAULT 0,
    rows_inserted     BIGINT      NOT NULL DEFAULT 0,
    rows_duplicate    BIGINT      NOT NULL DEFAULT 0,
    lines_quarantined BIGINT      NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS event_templates (
    template_id TEXT PRIMARY KEY,
    template    TEXT        NOT NULL,
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS log_events (
    source_file TEXT        NOT NULL,
    line_no     BIGINT      NOT NULL,
    event_time  TIMESTAMPTZ NOT NULL,
    epoch       BIGINT      NOT NULL,
    node        TEXT        NOT NULL,
    event_type  TEXT        NOT NULL,
    component   TEXT        NOT NULL,
    severity    TEXT        NOT NULL,
    label       TEXT        NOT NULL,
    is_alert    BOOLEAN     NOT NULL,
    template_id TEXT        NOT NULL REFERENCES event_templates (template_id),
    line_sha256 CHAR(64)    NOT NULL,
    message     TEXT        NOT NULL,
    PRIMARY KEY (source_file, line_no)
);

CREATE INDEX IF NOT EXISTS log_events_epoch_idx ON log_events (epoch);

-- Malformed lines are kept rather than dropped, so the reject count is
-- auditable and the lines themselves can be inspected.
CREATE TABLE IF NOT EXISTS quarantined_lines (
    source_file TEXT   NOT NULL,
    line_no     BIGINT NOT NULL,
    reason      TEXT   NOT NULL,
    raw_line    TEXT   NOT NULL,
    PRIMARY KEY (source_file, line_no)
);

-- One row per fixed time window that contains at least one event.
CREATE TABLE IF NOT EXISTS window_features (
    window_seconds     INTEGER     NOT NULL,
    window_start       TIMESTAMPTZ NOT NULL,
    event_count        INTEGER     NOT NULL,
    distinct_nodes     INTEGER     NOT NULL,
    distinct_templates INTEGER     NOT NULL,
    info_count         INTEGER     NOT NULL,
    warning_count      INTEGER     NOT NULL,
    error_count        INTEGER     NOT NULL,
    severe_count       INTEGER     NOT NULL,
    fatal_count        INTEGER     NOT NULL,
    failure_count      INTEGER     NOT NULL,
    error_rate         DOUBLE PRECISION NOT NULL,
    alert_count        INTEGER     NOT NULL,
    is_anomalous       BOOLEAN     NOT NULL,
    PRIMARY KEY (window_seconds, window_start)
);

-- Per-window counts per template, kept long rather than pivoted. Which
-- templates become model features is decided from the training split only, so
-- the pivot has to happen after the split rather than here.
CREATE TABLE IF NOT EXISTS window_template_counts (
    window_seconds INTEGER     NOT NULL,
    window_start   TIMESTAMPTZ NOT NULL,
    template_id    TEXT        NOT NULL,
    event_count    INTEGER     NOT NULL,
    PRIMARY KEY (window_seconds, window_start, template_id)
);
