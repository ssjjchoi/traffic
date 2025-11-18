WITH base AS (
    SELECT
    
        TRY_TO_DATE(
            SUBSTR(REGEXP_REPLACE(startDate, '[^0-9]', ''), 1, 8),
            'YYYYMMDD'
        ) AS event_date,

        DAYOFWEEK(
            TRY_TO_DATE(
                SUBSTR(REGEXP_REPLACE(startDate, '[^0-9]', ''), 1, 8),
                'YYYYMMDD'
            )
        ) AS weekday_order,

        CASE 
            WHEN LENGTH(REGEXP_REPLACE(startDate, '[^0-9]', '')) >= 10 
            THEN TO_NUMBER(SUBSTR(REGEXP_REPLACE(startDate, '[^0-9]', ''), 9, 2))
            ELSE NULL
        END AS hour,

        1 AS cnt
    FROM {{ source('TRAFFIC_RAW', 'TRAFFIC_EVENT_JIYEON') }}
    WHERE startDate IS NOT NULL AND TRIM(startDate) <> ''
    LIMIT 5000
)

SELECT
    CASE weekday_order
        WHEN 2 THEN '월'
        WHEN 3 THEN '화'
        WHEN 4 THEN '수'
        WHEN 5 THEN '목'
        WHEN 6 THEN '금'
        WHEN 7 THEN '토'
        WHEN 1 THEN '일'
    END AS weekday,
    hour,
    COUNT(*) AS event_count,
    weekday_order
FROM base
GROUP BY 1, 2, 4
ORDER BY weekday_order, hour