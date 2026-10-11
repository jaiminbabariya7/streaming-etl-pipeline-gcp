-- Fails if a session ends before it starts (clock or ordering problem upstream).
SELECT event_id, session_id, session_duration_secs
FROM {{ ref('fct_events') }}
WHERE session_duration_secs < 0
