-- Silver: AI-enriched reviews. ${ai_endpoint} = a model-serving / Foundation Model endpoint available in YOUR workspace
-- (confirm the name under Serving; endpoint names change over time).
-- Design: CTE isolates the model call; regexp_extract strips code fences/preamble; every parsed field is validated.
CREATE OR REFRESH MATERIALIZED VIEW fact_reviews (
  CONSTRAINT valid_rating   EXPECT (rating BETWEEN 1 AND 5) ON VIOLATION DROP ROW,
  CONSTRAINT valid_sentiment EXPECT (sentiment IN ('positive','neutral','negative'))   -- WARN: tracked in event log
)
COMMENT 'Reviews enriched with LLM sentiment + issue flags. prompt_version tracks the prompt used.'
AS
WITH scored AS (
  SELECT review_id, order_id, customer_id, restaurant_id,
         CAST(rating AS INT) AS rating, review_text,
         ai_query('${ai_endpoint}', CONCAT(
           'Return ONLY a JSON object, no prose, no markdown. Keys: ',
           'sentiment (positive|neutral|negative), issue_delivery (true|false), ',
           'issue_food_quality (true|false), issue_pricing (true|false), ',
           'issue_portion_size (true|false), issue_reason (short string). Review: ', review_text)) AS ai_raw
  FROM restaurant_catalog.bronze.raw_reviews
), cleaned AS (
  SELECT *, regexp_extract(ai_raw, '(?s)(\\{.*\\})', 1) AS ai_json FROM scored
)
SELECT review_id, order_id, customer_id, restaurant_id, rating, review_text,
       lower(get_json_object(ai_json, '$.sentiment'))                          AS sentiment,
       get_json_object(ai_json, '$.issue_delivery')      = 'true'               AS issue_delivery,
       get_json_object(ai_json, '$.issue_food_quality')  = 'true'               AS issue_food_quality,
       get_json_object(ai_json, '$.issue_pricing')       = 'true'               AS issue_pricing,
       get_json_object(ai_json, '$.issue_portion_size')  = 'true'               AS issue_portion_size,
       get_json_object(ai_json, '$.issue_reason')                               AS issue_reason,
       (get_json_object(ai_json, '$.sentiment') IS NOT NULL)                    AS ai_json_valid,
       'v1'                                                                     AS prompt_version
FROM cleaned;
