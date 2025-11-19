-- 지속시간을 구간별 분류`

WITH base AS (
SELECT
*,
CASE
WHEN ENDDATE IS NULL THEN DATEDIFF('minute', STARTDATE, CURRENT_TIMESTAMP)
ELSE DATEDIFF('minute', STARTDATE, ENDDATE)
END AS duration_min
FROM TRAFFIC_EVENT_JIYEON
)
SELECT
CASE
WHEN duration_min < 30 THEN '00~30분'
WHEN duration_min < 60 THEN '30~60분'
WHEN duration_min < 120 THEN '1~2시간'
WHEN duration_min < 240 THEN '2~4시간'
WHEN duration_min < 1440 THEN '4~24시간'
ELSE '24시간 이상'
END AS duration_range,
COUNT(*) AS event_count
FROM base
GROUP BY 1
ORDER BY event_count DESC;