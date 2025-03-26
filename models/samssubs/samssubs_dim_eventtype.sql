{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}


select DISTINCT
{{ dbt_utils.generate_surrogate_key(['event_name']) }} as EventType_Key,
event_name
FROM {{ source('samssubs_landing', 'web_traffic_events') }}
WHERE Page_Url IS NOT NULL