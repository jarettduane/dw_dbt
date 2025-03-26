{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['StoreID', 'Address']) }} as Store_Key,
StoreID,
Address,
City,
State,
Zip
FROM {{ source('samssubs_sams_subs_landing', 'store') }}