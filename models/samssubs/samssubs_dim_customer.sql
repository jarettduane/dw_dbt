{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['CustomerID', 'CustomerLName']) }} as Customer_Key,
CustomerID,
CustomerFName,
CustomerLName,
CustomerBDay,
CustomerPhone
FROM {{ source('samssubs_sams_subs_landing', 'customer') }}