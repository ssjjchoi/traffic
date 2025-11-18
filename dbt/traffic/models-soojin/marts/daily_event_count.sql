WITH base AS (
    SELECT
        startDate AS start_ts
    FROM {{ source('TRAFFIC_RAW', 'TRAFFIC_EVENT_JIYEON') }}
    LIMIT 5000
)

SELECT
    TO_DATE(start_ts) AS event_date,
    COUNT(*) AS event_count
FROM base
GROUP BY event_date
ORDER BY event_date