"""
Real-time Price Alert Stream (Spark Structured Streaming + Delta Lake).

Source  : Kafka topic `raw-price-events` (--source kafka) or a directory of JSONL
          files (--source files, useful without a broker / in CI).
Logic   : an event becomes an alert when it is flagged as a flash sale or the price
          drops by 30% or more.
Sink    : gold/price_alerts (Delta). Each micro-batch is MERGEd on alert_id inside
          foreachBatch, so replays after a failure never create duplicate alerts;
          progress is tracked in a checkpoint directory.
Latency : every alert stores latency_sec = processing time - event time, so the
          end-to-end delay can be measured instead of assumed.

Note: the price-change events are produced by data_collection/simulate_price_events.py
from real crawled products (Tiki has no public price-change feed).
"""
import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from data_collection.config import RAW_DATA_DIR
from spark.config import (
    CHECKPOINT_DIR, GOLD_PATH, KAFKA_BOOTSTRAP, PRICE_EVENTS_TOPIC,
    SPARK_KAFKA_PACKAGE, table_path,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("price_alert_stream")

PRICE_DROP_THRESHOLD_PCT = -30.0
EVENTS_DIR = RAW_DATA_DIR / "price_events"

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("product_id", T.LongType()),
    T.StructField("product_name", T.StringType()),
    T.StructField("brand_name", T.StringType()),
    T.StructField("seller_name", T.StringType()),
    T.StructField("old_price", T.DoubleType()),
    T.StructField("new_price", T.DoubleType()),
    T.StructField("original_price", T.DoubleType()),
    T.StructField("price_change_pct", T.DoubleType()),
    T.StructField("is_flash_sale", T.BooleanType()),
    T.StructField("event_timestamp", T.StringType()),
])


def evaluate_price_event(event: Dict) -> Dict:
    """Pure-Python reference of the alert rule (unit-tested; build_alerts mirrors it)."""
    old_p = event.get("old_price", 0)
    new_p = event.get("new_price", 0)
    change_pct = event.get("price_change_pct", 0.0)
    is_flash = event.get("is_flash_sale", False)

    if is_flash or change_pct <= PRICE_DROP_THRESHOLD_PCT:
        return {
            "alert_id": f"alt_{uuid.uuid4().hex[:10]}",
            "alert_type": "FLASH_SALE_DETECTED" if is_flash else "STEEP_PRICE_DROP",
            "product_id": event.get("product_id"),
            "product_name": event.get("product_name", ""),
            "brand_name": event.get("brand_name", "Unknown"),
            "seller_name": event.get("seller_name", "Unknown"),
            "old_price": old_p,
            "new_price": new_p,
            "price_drop_pct": abs(change_pct),
            "savings_amount": old_p - new_p,
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        }
    return {}


def build_alerts(events: DataFrame) -> DataFrame:
    """Streaming-safe transformation: parsed events -> alert rows."""
    return (
        events
        .filter(F.col("is_flash_sale") | (F.col("price_change_pct") <= PRICE_DROP_THRESHOLD_PCT))
        .select(
            F.concat(F.lit("alt_"), F.substring(F.sha2("event_id", 256), 1, 12)).alias("alert_id"),
            "event_id",
            F.when(F.col("is_flash_sale"), "FLASH_SALE_DETECTED")
             .otherwise("STEEP_PRICE_DROP").alias("alert_type"),
            "product_id",
            F.coalesce("product_name", F.lit("")).alias("product_name"),
            F.coalesce("brand_name", F.lit("Unknown")).alias("brand_name"),
            F.coalesce("seller_name", F.lit("Unknown")).alias("seller_name"),
            "old_price", "new_price",
            F.abs("price_change_pct").alias("price_drop_pct"),
            (F.col("old_price") - F.col("new_price")).alias("savings_amount"),
            F.to_timestamp("event_timestamp").alias("event_ts"),
        )
    )


def read_events(spark: SparkSession, source: str, events_dir: Path) -> DataFrame:
    if source == "kafka":
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", PRICE_EVENTS_TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        return raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")).select("e.*")

    events_dir.mkdir(parents=True, exist_ok=True)
    return spark.readStream.schema(EVENT_SCHEMA).option("maxFilesPerTrigger", 10).json(events_dir.as_posix())


def make_batch_writer(spark: SparkSession, target: str):
    from delta.tables import DeltaTable

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        alerts = (
            batch_df
            .withColumn("triggered_at", F.current_timestamp())
            .withColumn("latency_sec",
                        F.round(F.col("triggered_at").cast("double") - F.col("event_ts").cast("double"), 3))
            .withColumn("stream_batch_id", F.lit(batch_id))
        )
        if not DeltaTable.isDeltaTable(spark, target):
            alerts.write.format("delta").mode("append").save(target)
        else:
            (
                DeltaTable.forPath(spark, target).alias("t")
                .merge(alerts.alias("s"), "t.alert_id = s.alert_id")
                .whenNotMatchedInsertAll()
                .execute()
            )
        logger.info("micro-batch %d: %d alerts", batch_id, alerts.count())

    return write_batch


def run_price_alert_stream(
    spark: SparkSession,
    source: str = "files",
    trigger: str = "available-now",
    interval_seconds: int = 5,
    run_seconds: int = None,
    events_dir: Path = EVENTS_DIR,
    gold_path: str = GOLD_PATH,
    checkpoint_dir: Path = CHECKPOINT_DIR,
) -> dict:
    """
    trigger='available-now': process everything available, then stop (batch DAG).
    trigger='continuous'    : micro-batch every `interval_seconds` (long-running service).
    """
    target = table_path(gold_path, "price_alerts")
    checkpoint = (checkpoint_dir / f"price_alerts_{source}").as_posix()

    writer = (
        build_alerts(read_events(spark, source, events_dir))
        .writeStream
        .foreachBatch(make_batch_writer(spark, target))
        .option("checkpointLocation", checkpoint)
        .queryName(f"price_alerts_{source}")
    )
    if trigger == "available-now":
        query = writer.trigger(availableNow=True).start()
        query.awaitTermination()
    else:
        query = writer.trigger(processingTime=f"{interval_seconds} seconds").start()
        query.awaitTermination(run_seconds)
        query.stop()

    from delta.tables import DeltaTable
    if not DeltaTable.isDeltaTable(spark, target):
        logger.info("No alerts produced yet.")
        return {"alerts": 0}

    stats = (
        spark.read.format("delta").load(target)
        .agg(
            F.count("*").alias("alerts"),
            F.round(F.expr("percentile_approx(latency_sec, 0.5)"), 2).alias("p50_latency_sec"),
            F.round(F.expr("percentile_approx(latency_sec, 0.95)"), 2).alias("p95_latency_sec"),
        )
        .first().asDict()
    )
    logger.info("[STREAM] price_alerts: %s", stats)
    return stats


def main():
    from spark.spark_session import get_spark_session

    parser = argparse.ArgumentParser(description="Spark Structured Streaming price alerts")
    parser.add_argument("--source", choices=["files", "kafka"], default="files")
    parser.add_argument("--trigger", choices=["available-now", "continuous"], default="available-now")
    parser.add_argument("--interval", type=int, default=5, help="Micro-batch interval (continuous)")
    parser.add_argument("--run-seconds", type=int, default=None,
                        help="Stop a continuous stream after N seconds (default: run forever)")
    args = parser.parse_args()

    packages = [SPARK_KAFKA_PACKAGE] if args.source == "kafka" else None
    spark = get_spark_session("price_alert_stream", extra_packages=packages)
    try:
        run_price_alert_stream(spark, args.source, args.trigger, args.interval, args.run_seconds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
