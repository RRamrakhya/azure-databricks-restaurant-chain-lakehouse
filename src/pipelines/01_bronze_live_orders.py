# Databricks notebook source
# Spark Declarative Pipeline (Lakeflow). Reference code: runtime values (paths, catalogs, endpoints, secrets) come from pipeline/job configuration.
# source_mode = "files"    -> Auto Loader on a UC Volume (Databricks Free Edition)
# source_mode = "eventhub" -> Kafka endpoint of Azure Event Hubs
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

SOURCE_MODE = spark.conf.get("source_mode", "files")
LANDING = spark.conf.get("live_landing_path", "/Volumes/restaurant_catalog/landing/live_orders")

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType()), StructField("customer_id", StringType()),
    StructField("restaurant_id", StringType()), StructField("order_timestamp", StringType()),
    StructField("total_amount", DoubleType()), StructField("items", StringType()),
    StructField("order_type", StringType()), StructField("status", StringType()),
])


def _raw_stream():
    if SOURCE_MODE == "eventhub":
        ns = spark.conf.get("eh.namespace")  # e.g. mynamespace.servicebus.windows.net
        secret = dbutils.secrets.get(spark.conf.get("eh.secret_scope"), "eh-listen-cs")  # Key Vault-backed scope
        jaas = ('kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
                f'username="$ConnectionString" password="{secret}";')
        return (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", f"{ns}:9093")
                .option("subscribe", "orders")
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config", jaas)
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger", 1000)
                .option("failOnDataLoss", "true")
                .load()
                .select(F.col("value").cast("string").alias("raw_json"),
                        F.col("timestamp").alias("ingest_ts")))
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "text")
            .load(LANDING)
            .select(F.col("value").alias("raw_json"), F.current_timestamp().alias("ingest_ts")))


@dlt.table(name="live_orders_raw", comment="Bronze: raw JSON exactly as received (source fidelity).")
def live_orders_raw():
    return _raw_stream()


# Contract rules mirror src/contracts/order_event.schema.json (keep in sync; CI test checks the rule names).
VALID_RULES = {
    "order_id_present": "order_id IS NOT NULL",
    "customer_id_present": "customer_id IS NOT NULL",
    "restaurant_id_present": "restaurant_id IS NOT NULL",
    "timestamp_parses": "try_to_timestamp(order_timestamp) IS NOT NULL",
    "amount_positive": "total_amount > 0",
    "order_type_valid": "order_type IN ('dine-in','takeaway','delivery')",
    "status_valid": "status IN ('completed','cancelled','pending')",
    "items_is_array": "try_parse_json(items) IS NOT NULL",
}
ALL_VALID = " AND ".join(f"coalesce({r}, false)" for r in VALID_RULES.values())


def _parsed():
    return (dlt.read_stream("live_orders_raw")
            .withColumn("d", F.from_json("raw_json", ORDER_SCHEMA))
            .select("d.*", "raw_json", "ingest_ts"))


@dlt.table(name="live_orders", comment="Bronze (typed): events that satisfy the data contract.")
def live_orders():
    return _parsed().where(F.expr(ALL_VALID)).drop("raw_json")


@dlt.table(name="live_orders_quarantine",
           comment="Dead-letter: events that violate the contract, with reason codes for triage.")
def live_orders_quarantine():
    fails = [F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(name))
             for name, rule in VALID_RULES.items()]
    return (_parsed().where(~F.expr(ALL_VALID))
            .withColumn("quarantine_reasons", F.concat_ws(",", *fails))
            .select("raw_json", "quarantine_reasons", "ingest_ts"))
