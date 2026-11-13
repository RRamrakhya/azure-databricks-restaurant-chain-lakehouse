CREATE OR REFRESH MATERIALIZED VIEW daily_sales_summary
CLUSTER BY (order_date)
COMMENT 'Chain-wide daily KPIs.'
AS SELECT order_date,
          count(DISTINCT order_id)          AS total_orders,
          sum(total_amount)                 AS total_revenue,
          avg(total_amount)                 AS avg_order_value,
          count(DISTINCT customer_id)       AS unique_customers,
          count(DISTINCT restaurant_id)     AS active_restaurants,
          count_if(order_type = 'dine-in')  AS dine_in_orders,
          count_if(order_type = 'takeaway') AS takeaway_orders,
          count_if(order_type = 'delivery') AS delivery_orders
FROM fact_orders WHERE status = 'completed' GROUP BY order_date;

CREATE OR REFRESH MATERIALIZED VIEW restaurant_daily_sales
CLUSTER BY (order_date)
COMMENT 'Per-location daily revenue (powers revenue-by-location widgets).'
AS SELECT o.order_date, o.restaurant_id, r.restaurant_name, r.city,
          count(DISTINCT o.order_id) AS total_orders, sum(o.total_amount) AS total_revenue
FROM fact_orders o LEFT JOIN dim_restaurants r USING (restaurant_id)
WHERE o.status = 'completed' GROUP BY o.order_date, o.restaurant_id, r.restaurant_name, r.city;

CREATE OR REFRESH MATERIALIZED VIEW customer_360_profile
COMMENT 'One row per customer. Loyalty tiers by lifetime spend.'
AS WITH spend AS (
  SELECT customer_id, sum(total_amount) AS lifetime_spend, count(DISTINCT order_id) AS total_orders,
         avg(total_amount) AS avg_order_value, max(order_date) AS last_order_date
  FROM fact_orders WHERE status = 'completed' GROUP BY customer_id
), fav_rest AS (
  SELECT customer_id, restaurant_id AS favourite_restaurant_id FROM fact_orders
  GROUP BY customer_id, restaurant_id QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY count(*) DESC) = 1
), fav_item AS (
  SELECT o.customer_id, i.item_name AS favourite_menu_item FROM fact_order_items i JOIN fact_orders o USING (order_id)
  GROUP BY o.customer_id, i.item_name QUALIFY row_number() OVER (PARTITION BY o.customer_id ORDER BY sum(i.quantity) DESC) = 1
), sent AS (
  SELECT customer_id, avg(CASE sentiment WHEN 'positive' THEN 1 WHEN 'neutral' THEN 0 WHEN 'negative' THEN -1 END) AS avg_sentiment_score
  FROM fact_reviews GROUP BY customer_id
)
SELECT c.customer_id, c.first_name, c.last_name, s.lifetime_spend, s.total_orders, s.avg_order_value, s.last_order_date,
       CASE WHEN s.lifetime_spend >= 50000 THEN 'Platinum' WHEN s.lifetime_spend >= 20000 THEN 'Gold'
            WHEN s.lifetime_spend >= 5000 THEN 'Silver' ELSE 'Bronze' END AS loyalty_tier,
       s.lifetime_spend >= 50000 AS is_vip, fr.favourite_restaurant_id, fi.favourite_menu_item, se.avg_sentiment_score
FROM dim_customers c LEFT JOIN spend s USING (customer_id) LEFT JOIN fav_rest fr USING (customer_id)
LEFT JOIN fav_item fi USING (customer_id) LEFT JOIN sent se USING (customer_id);

CREATE OR REFRESH MATERIALIZED VIEW restaurant_review_metrics
COMMENT 'Review KPIs per location (zero-filled).'
AS SELECT r.restaurant_id, r.restaurant_name, r.city,
          coalesce(round(avg(f.rating), 2), 0) AS avg_rating, count(f.review_id) AS total_reviews,
          count_if(f.sentiment = 'positive') AS positive_count, count_if(f.sentiment = 'neutral') AS neutral_count,
          count_if(f.sentiment = 'negative') AS negative_count,
          count_if(f.issue_delivery) AS delivery_issue_count, count_if(f.issue_food_quality) AS food_quality_issue_count,
          count_if(f.issue_pricing) AS pricing_issue_count, count_if(f.issue_portion_size) AS portion_issue_count
FROM dim_restaurants r LEFT JOIN fact_reviews f USING (restaurant_id)
GROUP BY r.restaurant_id, r.restaurant_name, r.city;
