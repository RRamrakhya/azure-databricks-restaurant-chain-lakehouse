-- Chain Performance
SELECT order_date, restaurant_name, total_revenue FROM restaurant_catalog.gold.restaurant_daily_sales ORDER BY order_date;
SELECT order_date, total_orders FROM restaurant_catalog.gold.daily_sales_summary ORDER BY order_date;
SELECT item_name, sum(quantity) AS units, sum(subtotal) AS revenue
FROM restaurant_catalog.silver.fact_order_items GROUP BY item_name ORDER BY revenue DESC LIMIT 10;
SELECT day_of_week, order_hour, sum(total_amount) AS revenue
FROM restaurant_catalog.silver.fact_orders GROUP BY day_of_week, order_hour;
-- Review Insights
SELECT restaurant_name, positive_count, neutral_count, negative_count FROM restaurant_catalog.gold.restaurant_review_metrics;
SELECT loyalty_tier, count(*) AS customers, sum(lifetime_spend) AS spend FROM restaurant_catalog.gold.customer_360_profile GROUP BY loyalty_tier;
