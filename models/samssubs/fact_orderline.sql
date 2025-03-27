{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
) }}

SELECT
    c.customer_key,
    d.date_key,
    e.employee_key,
    p.product_key,
    s.store_key,
    m.method_key,
    od.orderlineqty,
    od.orderlineprice
FROM {{ source('samssubs_sams_subs_landing', 'orderdetails') }} od
INNER JOIN {{ source('samssubs_sams_subs_landing', '"ORDER"')}} o ON od.ordernumber = o.ordernumber
INNER JOIN {{ source('samssubs_sams_subs_landing', 'employee')}} es ON o.employeeid = es.employeeid
INNER JOIN {{ ref('samssubs_dim_customer') }} c ON o.customerid = c.customerid
INNER JOIN {{ ref('samssubs_dim_employee') }} e ON o.employeeid = e.employeeid
INNER JOIN {{ ref('samssubs_dim_product') }} p ON od.productid = p.productid
INNER JOIN {{ ref('samssubs_dim_store') }} s ON es.storeid = s.storeid
INNER JOIN {{ ref('samssubs_dim_ordermethod') }} m ON o.ordermethod = m.ordermethod
INNER JOIN {{ ref('samssubs_dim_date') }} d ON o.orderdate = d.date