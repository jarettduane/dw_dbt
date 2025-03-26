{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}


select DISTINCT
{{ dbt_utils.generate_surrogate_key(['Page_Url']) }} as WebPage_Key,
Page_Url AS URL
FROM {{ source('samssubs_landing', 'web_traffic_events') }}
WHERE Page_Url IS NOT NULL