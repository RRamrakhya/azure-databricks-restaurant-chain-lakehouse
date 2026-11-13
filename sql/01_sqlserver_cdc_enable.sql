-- Azure SQL Database source (design-only if the Azure subscription has expired).
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='customers',   @role_name=NULL;
EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='restaurants', @role_name=NULL;
EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='menu_items',  @role_name=NULL;
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = DB_NAME();
