{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

with cte_date as (
{{ dbt_date.get_date_dimension("1990-01-01", "2050-12-31") }}
)

SELECT
{{ dbt_utils.generate_surrogate_key(['date_day']) }} as Date_Key,
date_day AS Date,
day_of_month AS Day,
month_of_year AS Month,
year_number AS Year
from cte_date