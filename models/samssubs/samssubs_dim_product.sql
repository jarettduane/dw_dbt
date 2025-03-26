{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['p.ProductID', 'p.ProductName']) }} AS Product_Key,
p.ProductID,
p.ProductName,
p.ProductCalories,
p.ProductCost,
s.Length,
s.BreadType
FROM {{ source('samssubs_sams_subs_landing', 'product') }} AS p 
LEFT JOIN {{ source('samssubs_sams_subs_landing', 'sandwich') }} AS s 
    ON p.ProductID = s.ProductID