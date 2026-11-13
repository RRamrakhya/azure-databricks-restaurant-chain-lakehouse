# Databricks notebook source
# CDC dimension upserts. In Azure mode the source is the Lakeflow Connect output (SQL Server CDC).
# In file mode, the source is a change-feed CSV in a Volume (columns _change_type and _commit_ts).
import dlt
from pyspark.sql import functions as F

CDC_ROOT = spark.conf.get("cdc_root", "/Volumes/restaurant_catalog/landing/cdc")

DIMS = {  # target -> (key, source folder)
    "dim_customers": ("customer_id", "customers"),
    "dim_restaurants": ("restaurant_id", "restaurants"),
    "dim_menu_items": ("menu_item_id", "menu_items"),
}


def make_dim(target, key, folder):
    @dlt.view(name=f"{target}_cdc_feed")
    def _feed():
        return (spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv")
                .option("header", "true").option("cloudFiles.inferColumnTypes", "true")
                .load(f"{CDC_ROOT}/{folder}")
                # _change_type (INSERT/UPDATE/DELETE) and _commit_ts come with the feed
                .withColumn("_commit_ts", F.coalesce(F.col("_commit_ts").cast("timestamp"), F.current_timestamp())))

    dlt.create_streaming_table(
        name=target,
        expect_all_or_fail={f"valid_{key}": f"{key} IS NOT NULL"},  # a null dimension key is a source bug: stop the line
    )
    dlt.apply_changes(
        target=target, source=f"{target}_cdc_feed", keys=[key],
        sequence_by=F.col("_commit_ts"),
        apply_as_deletes=F.expr("_change_type = 'DELETE'"),
        except_column_list=["_change_type", "_commit_ts", "_rescued_data"],
        stored_as_scd_type=1,
    )


for _t, (_k, _f) in DIMS.items():
    make_dim(_t, _k, _f)
