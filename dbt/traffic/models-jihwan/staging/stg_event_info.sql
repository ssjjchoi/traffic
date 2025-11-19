{{ config(
    materialized='view'
) }}

SELECT
    eventType,
    eventDetailType,
    startDate,
    endDate,
    coordX,
    coordY
FROM {{ source('raw_data', 'raw_event_info_table_jihwan') }}
