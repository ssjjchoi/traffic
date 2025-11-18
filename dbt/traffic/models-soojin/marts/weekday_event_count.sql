WITH base AS (
    SELECT
        startDate AS start_ts,
        eventType,
        eventDetailType,
        roadName,
        type
    FROM {{ source('TRAFFIC_RAW', 'TRAFFIC_EVENT_JIYEON') }}
    LIMIT 5000
),

weekday_summary AS (
    SELECT
        TO_VARCHAR(start_ts, 'DY') AS weekday_short,
        DAYOFWEEK(start_ts) AS weekday_num,
        COUNT(*) AS event_count
    FROM base
    WHERE start_ts IS NOT NULL
    GROUP BY weekday_short, weekday_num
)

SELECT *
FROM weekday_summary
ORDER BY weekday_num