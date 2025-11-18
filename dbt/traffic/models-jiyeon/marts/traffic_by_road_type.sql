WITH traffic_events AS (
    SELECT * FROM {{ ref('src_traffic_events') }}
)

SELECT
    type as road_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT roadName) as total_roads,
    COUNT(DISTINCT DATE(startDate)) as active_days,
    COUNT(DISTINCT linkId) as unique_locations
FROM traffic_events
WHERE type IS NOT NULL
GROUP BY type
ORDER BY total_events DESC