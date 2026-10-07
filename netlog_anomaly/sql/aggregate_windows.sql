-- Aggregate log_events into fixed tumbling windows.
--
-- A window exists only if it contains at least one event; the log has long
-- idle stretches and materialising empty windows would swamp the dataset.
--
-- error_rate counts ERROR, SEVERE, FATAL and FAILURE lines, not WARNING.
--
-- is_anomalous comes from the dataset's own per-line alert label, never from
-- severity. The two are related in BGL but not equivalent.
INSERT INTO window_features (
    window_seconds, window_start, event_count, distinct_nodes, distinct_templates,
    info_count, warning_count, error_count, severe_count, fatal_count, failure_count,
    error_rate, alert_count, is_anomalous
)
SELECT
    %(window_seconds)s AS window_seconds,
    to_timestamp(div(epoch, %(window_seconds)s) * %(window_seconds)s) AS window_start,
    count(*)                                              AS event_count,
    count(DISTINCT node)                                  AS distinct_nodes,
    count(DISTINCT template_id)                           AS distinct_templates,
    count(*) FILTER (WHERE severity = 'INFO')             AS info_count,
    count(*) FILTER (WHERE severity = 'WARNING')          AS warning_count,
    count(*) FILTER (WHERE severity = 'ERROR')            AS error_count,
    count(*) FILTER (WHERE severity = 'SEVERE')           AS severe_count,
    count(*) FILTER (WHERE severity = 'FATAL')            AS fatal_count,
    count(*) FILTER (WHERE severity = 'FAILURE')          AS failure_count,
    count(*) FILTER (WHERE severity IN ('ERROR', 'SEVERE', 'FATAL', 'FAILURE'))::double precision
        / count(*)                                        AS error_rate,
    count(*) FILTER (WHERE is_alert)                      AS alert_count,
    bool_or(is_alert)                                     AS is_anomalous
FROM log_events
GROUP BY 1, 2;
