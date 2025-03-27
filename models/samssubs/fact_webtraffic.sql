{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
) }}

SELECT
    ts.trafficsource_key,
    wp.webpage_key,
    et.eventtype_key,
    dt.date_key,
    COUNT(*) AS interactioncount
FROM {{ source('samssubs_landing', 'web_traffic_events') }} wt
INNER JOIN {{ ref('samssubs_dim_trafficsource') }} ts ON wt.TRAFFIC_SOURCE = ts.traffic_source
INNER JOIN {{ ref('samssubs_dim_webpage') }} wp ON wt.PAGE_URL = page_url
INNER JOIN {{ ref('samssubs_dim_eventtype') }} et ON wt.EVENT_NAME = et.event_name
INNER JOIN {{ ref('samssubs_dim_date') }} dt ON CAST(wt.event_timestamp AS DATE) = dt.date
GROUP BY ts.trafficsource_key, wp.webpage_key, et.eventtype_key, dt.date_key