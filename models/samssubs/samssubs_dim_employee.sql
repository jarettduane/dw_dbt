{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['EmployeeID', 'EmployeeLName']) }} as Employee_Key,
EmployeeID,
EmployeeFName,
EmployeeLName,
EmployeeBDay,
StoreID
FROM {{ source('samssubs_sams_subs_landing', 'employee') }}