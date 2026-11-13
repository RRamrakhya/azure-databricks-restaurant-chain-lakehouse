# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, StructType, StructField, StringType, IntegerType, DoubleType)

HIST = spark.conf.get("historical_orders_table", "restaurant_catalog.bronze.raw_historical_orders")

ITEM_SCHEMA = ArrayType(StructType([
    StructField("item_id", StringType()), StructField("item_name", StringType()),
    StructField("item_category", StringType()), StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()), StructField("subtotal", DoubleType()),
]))

# --- fact_orders: two flows (live + historical) into ONE target, keyed on order_id.
# apply_changes makes re-delivery idempotent (at-least-once -> effectively-once): duplicates collapse on order_id.
dlt.create_streaming_table(
    name="fact_orders",
    comment="One row per order. Live + historical, deduplicated by order_id.",
    cluster_by=["order_date"],
    expect_all_or_drop={"positive_amount": "total_amount > 0", "has_items": "item_count > 0"},
)


def _enrich(df):
    return (df.withColumn("order_ts", F.to_timestamp("order_timestamp"))
            .withColumn("order_date", F.to_date("order_ts"))
            .withColumn("order_hour", F.hour("order_ts"))
            .withColumn("day_of_week", F.date_format("order_ts", "EEEE"))
            .withColumn("is_weekend", F.date_format("order_ts", "E").isin("Sat", "Sun"))
            .withColumn("total_amount", F.col("total_amount").cast("decimal(10,2)"))
            .withColumn("item_count", F.size(F.from_json("items", ITEM_SCHEMA)))
            .withColumn("source", F.lit(None).cast("string")))


@dlt.view(name="live_orders_enriched")
def live_enriched():
    return _enrich(dlt.read_stream("live_orders")).withColumn("source", F.lit("live")) \
        .withColumn("seq", F.col("ingest_ts"))


@dlt.view(name="historical_orders_enriched")
def hist_enriched():
    return _enrich(spark.readStream.table(HIST)).withColumn("source", F.lit("historical")) \
        .withColumn("seq", F.to_timestamp("order_timestamp"))


for src in ("live_orders_enriched", "historical_orders_enriched"):
    dlt.apply_changes(target="fact_orders", source=src, keys=["order_id"],
                      sequence_by=F.col("seq"), except_column_list=["seq", "ingest_ts", "order_ts"],
                      stored_as_scd_type=1, flow_name=f"flow_{src}")


# --- fact_order_items: explode the JSON array (grain: one row per order line)
@dlt.table(name="fact_order_items", cluster_by=["order_id"],
           comment="One row per order line item.")
@dlt.expect_all_or_drop({"valid_item_id": "menu_item_id IS NOT NULL",
                         "positive_qty": "quantity > 0", "positive_price": "unit_price > 0"})
def fact_order_items():
    return (dlt.read("fact_orders")
            .select("order_id", "restaurant_id", F.explode(F.from_json("items", ITEM_SCHEMA)).alias("i"))
            .select("order_id", "restaurant_id",
                    F.col("i.item_id").alias("menu_item_id"), F.col("i.item_name").alias("item_name"),
                    F.col("i.item_category").alias("item_category"), F.col("i.quantity").alias("quantity"),
                    F.col("i.unit_price").cast("decimal(8,2)").alias("unit_price"),
                    F.col("i.subtotal").cast("decimal(10,2)").alias("subtotal")))
