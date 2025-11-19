{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT 
    eventType,
    COUNT(*) AS event_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM {{ ref('stg_event_info') }}
GROUP BY eventType
ORDER BY event_count DESC
