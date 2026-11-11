select distinct purchase_date as date_day,year(purchase_date) year,month(purchase_date) month,date_trunc('month',purchase_date)::date month_start from {{ ref('fact_orders') }}
