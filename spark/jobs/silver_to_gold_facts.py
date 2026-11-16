"""
Gold Layer: Star Schema Facts (PySpark + Delta Lake).

1. fact_product_snapshot - grain: one row per product per crawl day.
   Holds the selling price, discount, units sold and rating on that day, so the
   daily price history of every product is kept here. Linked to the dim_product
   version that was valid on that day. Partitioned by date_key; MERGE on
   (product_id, date_key) makes re-runs on the same day idempotent.

2. fact_reviews - grain: one row per review. Point-in-time join: each review is
   linked to the product version valid when the review was written (falls back
   to the first known version for reviews older than our first crawl).
"""
import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from spark.config import GOLD_PATH, SILVER_PATH, table_path
from spark.delta_utils import upsert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("gold_facts")


def process_facts(
    spark: SparkSession,
    silver_path: str = SILVER_PATH,
    gold_path: str = GOLD_PATH,
) -> dict:
    products = spark.read.format("delta").load(table_path(silver_path, "products"))
    reviews = spark.read.format("delta").load(table_path(silver_path, "reviews"))
    dim_product = spark.read.format("delta").load(table_path(gold_path, "dim_product"))

    versions = dim_product.select("product_id", "product_sk", "version", "valid_from", "valid_to")

    # ---- fact_product_snapshot -------------------------------------------------
    snap = (
        products
        .withColumn("snapshot_ts", F.coalesce("crawled_at", F.current_timestamp()))
        .withColumn("date_key", F.date_format("snapshot_ts", "yyyyMMdd").cast("int"))
        .alias("p")
        .join(
            versions.alias("v"),
            (F.col("p.product_id") == F.col("v.product_id"))
            & (F.col("p.snapshot_ts") >= F.col("v.valid_from"))
            & (F.col("p.snapshot_ts") < F.col("v.valid_to")),
            "left",
        )
        .select(
            F.concat_ws("_", F.col("p.product_id"), F.col("date_key")).alias("snapshot_id"),
            "date_key",
            F.col("v.product_sk"),
            F.col("p.product_id"),
            "brand_id", "seller_id", "subcategory_id",
            "price", "original_price", "discount_rate_calc",
            "quantity_sold", "rating_average", "review_count",
            "price_segment", "is_discount_discrepancy",
        )
    )
    snap_path = table_path(gold_path, "fact_product_snapshot")
    upsert(spark, snap, snap_path, keys=["product_id", "date_key"], partition_by=["date_key"])

    # ---- fact_reviews (point-in-time join to dim_product) ----------------------
    first_version = versions.withColumn(
        "_rn", F.row_number().over(Window.partitionBy("product_id").orderBy("version"))
    ).filter("_rn = 1").select("product_id", F.col("product_sk").alias("first_sk"))

    r = reviews.alias("r")
    pit = (
        r.join(
            versions.alias("v"),
            (F.col("r.product_id") == F.col("v.product_id"))
            & (F.col("r.created_at") >= F.col("v.valid_from"))
            & (F.col("r.created_at") < F.col("v.valid_to")),
            "left",
        )
        .join(first_version.alias("f"), F.col("r.product_id") == F.col("f.product_id"), "left")
        .select(
            F.col("r.review_id"),
            F.coalesce(F.col("v.product_sk"), F.col("f.first_sk")).alias("product_sk"),
            F.col("r.product_id"),
            F.date_format(F.coalesce("r.created_at", "r.crawled_at"), "yyyyMMdd").cast("int").alias("date_key"),
            "r.customer_hash", "r.rating", "r.sentiment_label",
            "r.is_after_sales_complaint", "r.is_defect_complaint",
            "r.thank_count", "r.tokenized_text",
        )
    )
    rev_path = table_path(gold_path, "fact_reviews")
    upsert(spark, pit, rev_path, keys=["review_id"])

    stats = {
        "fact_product_snapshot": spark.read.format("delta").load(snap_path).count(),
        "fact_reviews": spark.read.format("delta").load(rev_path).count(),
    }
    logger.info("[GOLD] facts: %s", stats)
    return stats


def main():
    from spark.spark_session import get_spark_session

    argparse.ArgumentParser(description="Build Gold facts").parse_args()
    spark = get_spark_session("gold_facts")
    try:
        process_facts(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
