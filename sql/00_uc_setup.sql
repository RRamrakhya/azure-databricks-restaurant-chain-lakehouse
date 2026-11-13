-- Run once per environment. Replace catalog/group names as needed.
CREATE CATALOG IF NOT EXISTS restaurant_catalog;
USE CATALOG restaurant_catalog;
CREATE SCHEMA IF NOT EXISTS landing; CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;  CREATE SCHEMA IF NOT EXISTS gold;
CREATE VOLUME IF NOT EXISTS landing.live_orders;
CREATE VOLUME IF NOT EXISTS landing.cdc;

-- Least privilege: BI readers see only Gold; engineers own Bronze/Silver.
GRANT USE CATALOG ON CATALOG restaurant_catalog TO `bi_readers`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `bi_readers`;
GRANT ALL PRIVILEGES ON SCHEMA bronze TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA silver TO `data_engineers`;

-- PII protection: mask contact fields unless the user is in pii_readers.
CREATE OR REPLACE FUNCTION silver.mask_pii(v STRING) RETURNS STRING
RETURN CASE WHEN is_account_group_member('pii_readers') THEN v ELSE '***REDACTED***' END;
-- Apply after dim_customers exists (pipeline-managed tables may require applying via pipeline settings/ALTER):
-- ALTER TABLE silver.dim_customers ALTER COLUMN email SET MASK silver.mask_pii;
-- ALTER TABLE silver.dim_customers ALTER COLUMN phone SET MASK silver.mask_pii;

-- Row filter example: a location manager sees only their restaurant (mapping table: governance.manager_access).
-- CREATE FUNCTION silver.rest_filter(rid STRING) RETURN is_account_group_member('chain_admins')
--   OR EXISTS (SELECT 1 FROM governance.manager_access m WHERE m.user = current_user() AND m.restaurant_id = rid);
-- ALTER TABLE silver.fact_orders SET ROW FILTER silver.rest_filter ON (restaurant_id);
