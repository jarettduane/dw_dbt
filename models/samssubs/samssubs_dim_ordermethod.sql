{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['OrderMethod']) }} as Method_Key,
OrderMethod
FROM {{source('samssubs_sams_subs_landing', '"ORDER"') }}