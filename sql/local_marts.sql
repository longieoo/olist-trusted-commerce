-- Local analytical publication. Natural keys remain stable across snapshots.
CREATE TABLE dim_order_customer AS SELECT * FROM silver_customers;
CREATE TABLE dim_seller AS SELECT * FROM silver_sellers;
CREATE TABLE dim_product AS
SELECT p.*, t.product_category_name_english
FROM silver_products p LEFT JOIN silver_category_translation t USING(product_category_name);
CREATE TABLE fact_order_items AS SELECT * FROM silver_order_items;
CREATE TABLE fact_payments AS SELECT * FROM silver_order_payments;
CREATE TABLE bridge_order_seller AS SELECT DISTINCT order_id,seller_id FROM fact_order_items;
CREATE TABLE order_review_summary AS
SELECT order_id,review_id,review_score,
       count(*) OVER(PARTITION BY order_id) AS review_history_count
FROM silver_order_reviews
QUALIFY row_number() OVER(PARTITION BY order_id
  ORDER BY review_answer_timestamp DESC NULLS LAST, review_creation_date DESC NULLS LAST,review_id DESC)=1;
CREATE TABLE fact_orders AS
WITH items AS (
 SELECT order_id,sum(price)::DECIMAL(18,2) merchandise_brl,
 sum(freight_value)::DECIMAL(18,2) freight_brl,count(*) item_count
 FROM fact_order_items GROUP BY order_id
), payments AS (
 SELECT order_id,sum(payment_value)::DECIMAL(18,2) payment_brl,count(*) payment_count
 FROM fact_payments GROUP BY order_id
), base AS (
 SELECT o.*,c.customer_unique_id,c.customer_city,c.customer_state,
 cast(o.order_purchase_timestamp AS DATE) purchase_date,
 date_diff('second',cast(o.order_purchase_timestamp AS TIMESTAMP),
   cast(o.order_delivered_customer_date AS TIMESTAMP))/86400.0 purchase_to_delivery_days,
 CASE WHEN o.order_status='delivered' AND o.order_delivered_customer_date IS NOT NULL
  AND o.order_estimated_delivery_date IS NOT NULL THEN 1 ELSE 0 END late_eligible,
 coalesce(i.merchandise_brl,0)::DECIMAL(18,2) merchandise_brl,
 coalesce(i.freight_brl,0)::DECIMAL(18,2) freight_brl,
 coalesce(p.payment_brl,0)::DECIMAL(18,2) payment_brl,
 coalesce(i.item_count,0) item_count,coalesce(p.payment_count,0) payment_count,
 r.review_score,r.review_history_count
 FROM silver_orders o LEFT JOIN dim_order_customer c USING(customer_id)
 LEFT JOIN items i USING(order_id) LEFT JOIN payments p USING(order_id)
 LEFT JOIN order_review_summary r USING(order_id)
)
SELECT *, item_count>0 AS has_item_records, payment_count>0 AS has_payment_records,
 CASE WHEN late_eligible=1 THEN
 cast(cast(order_delivered_customer_date AS DATE)>cast(order_estimated_delivery_date AS DATE) AS INTEGER)
 ELSE NULL END is_late,
 payment_brl-merchandise_brl-freight_brl AS payment_difference_brl
FROM base;
CREATE TABLE mart_delivery_daily AS
SELECT purchase_date,customer_state,count(*) orders,sum(late_eligible) eligible_delivered,
 sum(coalesce(is_late,0)) late_orders,
 sum(coalesce(is_late,0))*1.0/nullif(sum(late_eligible),0) late_delivery_rate,
 avg(purchase_to_delivery_days) avg_delivery_days,count(purchase_to_delivery_days) duration_coverage,
 sum(merchandise_brl) merchandise_brl,sum(freight_brl) freight_brl,sum(payment_brl) payment_brl
FROM fact_orders GROUP BY purchase_date,customer_state;
CREATE TABLE mart_seller_delivery AS
SELECT b.seller_id,s.seller_state,count(*) order_seller_pairs,
 sum(o.late_eligible) eligible_order_seller_pairs,
 sum(coalesce(o.is_late,0)) late_order_seller_pairs,
 sum(coalesce(o.is_late,0))*1.0/nullif(sum(o.late_eligible),0) late_delivery_rate
FROM bridge_order_seller b JOIN fact_orders o USING(order_id)
LEFT JOIN dim_seller s USING(seller_id) GROUP BY b.seller_id,s.seller_state;

-- ZIP observations are aggregated BEFORE joining orders, avoiding fan-out.
CREATE TABLE dim_zip AS
SELECT lpad(cast(geolocation_zip_code_prefix AS VARCHAR),5,'0') zip_prefix,
 median(try_cast(geolocation_lat AS DOUBLE)) latitude,
 median(try_cast(geolocation_lng AS DOUBLE)) longitude,count(*) observations
FROM silver_geolocation
WHERE try_cast(geolocation_lat AS DOUBLE) BETWEEN -90 AND 90
 AND try_cast(geolocation_lng AS DOUBLE) BETWEEN -180 AND 180
GROUP BY 1;
CREATE TABLE fact_order_geography AS
SELECT o.order_id,c.customer_state,c.customer_zip_code_prefix,z.latitude,z.longitude,
 z.observations,z.latitude IS NOT NULL AS has_coordinates
FROM fact_orders o JOIN dim_order_customer c USING(customer_id)
LEFT JOIN dim_zip z ON lpad(cast(c.customer_zip_code_prefix AS VARCHAR),5,'0')=z.zip_prefix;
CREATE TABLE mart_corridors AS
SELECT s.seller_state,o.customer_state,count(*) order_seller_pairs,
 count(DISTINCT o.order_id) distinct_orders,
 sum(o.late_eligible) eligible_pairs,sum(coalesce(o.is_late,0)) late_pairs,
 median(o.purchase_to_delivery_days) median_delivery_days,
 quantile_cont(o.purchase_to_delivery_days,0.9) p90_delivery_days,
 count(*) FILTER(WHERE z.latitude IS NOT NULL AND g.latitude IS NOT NULL) distance_coverage_pairs,
 avg(CASE WHEN z.latitude IS NOT NULL AND g.latitude IS NOT NULL THEN
  6371*2*asin(sqrt(least(1.0,greatest(0.0,
   pow(sin(radians(z.latitude-g.latitude)/2),2)+cos(radians(z.latitude))*cos(radians(g.latitude))*
   pow(sin(radians(z.longitude-g.longitude)/2),2))))) END) approximate_distance_km
FROM bridge_order_seller b JOIN fact_orders o USING(order_id)
JOIN dim_seller s USING(seller_id) JOIN fact_order_geography g USING(order_id)
LEFT JOIN dim_zip z ON lpad(cast(s.seller_zip_code_prefix AS VARCHAR),5,'0')=z.zip_prefix
GROUP BY s.seller_state,o.customer_state;
-- Cutoff is the latest observed purchase in this accepted snapshot, not wall-clock time.
CREATE TABLE customer_observation AS
WITH dates AS (
 SELECT customer_unique_id,min(purchase_date) first_observed_date,count(*) orders,
 sum(merchandise_brl) observed_merchandise_brl,
 min(purchase_date) FILTER(WHERE rn>1) second_observed_date
 FROM (SELECT *,row_number() OVER(PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp,order_id) rn
 FROM fact_orders) q GROUP BY customer_unique_id
)
SELECT *, (SELECT max(purchase_date) FROM fact_orders) observation_cutoff FROM dates;
CREATE TABLE mart_customer_cohorts AS
WITH spine AS (
 SELECT unnest(generate_series(date_trunc('month',min(first_observed_date))::DATE,
 date_trunc('month',max(first_observed_date))::DATE,INTERVAL '1 month'))::DATE cohort_month
 FROM customer_observation
), horizons AS (SELECT unnest([30,60,90]) horizon_days)
SELECT s.cohort_month,h.horizon_days,count(c.customer_unique_id) observed_customers,
 count(c.customer_unique_id) FILTER(WHERE date_diff('day',first_observed_date,observation_cutoff)>=h.horizon_days) mature_customers,
 count(c.customer_unique_id) FILTER(WHERE date_diff('day',first_observed_date,observation_cutoff)>=h.horizon_days
 AND date_diff('day',first_observed_date,second_observed_date)<=h.horizon_days) repeat_customers,
 max(observation_cutoff) observation_cutoff
FROM spine s CROSS JOIN horizons h LEFT JOIN customer_observation c
 ON date_trunc('month',c.first_observed_date)::DATE=s.cohort_month GROUP BY s.cohort_month,h.horizon_days;
CREATE TABLE mart_review_experience AS
SELECT order_status,is_late,count(*) orders,count(review_score) reviewed_orders,
 avg(review_score) average_score,
 count(*) FILTER(WHERE review_score<=2) low_score_orders
FROM fact_orders GROUP BY order_status,is_late;
CREATE TABLE mart_payment_control AS
SELECT order_status,has_item_records,has_payment_records,count(*) orders,
 count(*) FILTER(WHERE has_item_records AND has_payment_records AND abs(payment_difference_brl)>0.01) source_difference_orders,
 sum(merchandise_brl) merchandise_brl,sum(freight_brl) freight_brl,sum(payment_brl) payment_brl
FROM fact_orders GROUP BY order_status,has_item_records,has_payment_records;
