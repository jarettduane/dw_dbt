{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}


select DISTINCT
{{ dbt_utils.generate_surrogate_key(['traffic_source']) }} as TrafficSource_Key,
traffic_source
FROM {{ source('samssubs_landing', 'web_traffic_events') }}
WHERE Page_Url IS NOT NULL