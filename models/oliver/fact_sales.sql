{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}

SELECT
    c.cust_key,
    d.date_key,
    e.employee_key,
    p.product_key,
    s.store_key
FROM {{ source('oliver_landing'), ('orderline')}} ol
INNER JOIN {{ source('insurance_landing', 'policies') }} pd ON c.PolicyID = pd.PolicyID
INNER JOIN {{ ref('dim_policy') }} p ON pd.PolicyID = p.policyid 
INNER JOIN {{ ref('dim_customer') }} cu ON pd.CustomerID = cu.customerid 
INNER JOIN {{ ref('dim_agent') }} a ON pd.AgentID = a.agentid 
INNER JOIN {{ ref('dim_date') }} d ON d.date_day = c.ClaimDate