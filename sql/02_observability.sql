-- Data-quality & freshness checks (schedule as a SQL alert). Event-log table name differs per workspace setup;
-- see Databricks docs for event_log(<pipeline_id>) and adjust.
-- 1) Quarantine rate (alert if > 2% in the last hour)
SELECT count_if(true) AS quarantined,
       (SELECT count(*) FROM restaurant_catalog.bronze.live_orders_raw WHERE ingest_ts > now() - INTERVAL 1 HOUR) AS received
FROM restaurant_catalog.bronze.live_orders_quarantine WHERE ingest_ts > now() - INTERVAL 1 HOUR;
-- 2) Freshness SLO: newest live order should be < 15 min old during opening hours
SELECT max(ingest_ts) AS newest, timestampdiff(MINUTE, max(ingest_ts), now()) AS minutes_stale
FROM restaurant_catalog.bronze.live_orders;
-- 3) AI parse-failure rate
SELECT avg(CASE WHEN ai_json_valid THEN 0 ELSE 1 END) AS ai_parse_failure_rate FROM restaurant_catalog.silver.fact_reviews;
