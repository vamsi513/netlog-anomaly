-- Per-window counts for each event template, kept long rather than pivoted.
-- Which templates become model features is chosen from the training split
-- only, so the pivot cannot happen here without leaking test information.
INSERT INTO window_template_counts (
    window_seconds, window_start, template_id, event_count
)
SELECT
    %(window_seconds)s AS window_seconds,
    to_timestamp(div(epoch, %(window_seconds)s) * %(window_seconds)s) AS window_start,
    template_id,
    count(*) AS event_count
FROM log_events
GROUP BY 1, 2, 3;
