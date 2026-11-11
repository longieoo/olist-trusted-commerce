select order_id from {{ ref('fact_orders') }}
where purchase_to_carrier_days<0 or carrier_to_customer_days<0 or purchase_to_delivery_days<0
or (late_eligible=0 and is_late is not null) or is_late not in (0,1)
