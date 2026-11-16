"""
Silver Layer: Products Cleaning, Enrichment and Anomaly Flags (PySpark + Delta).

Reads raw JSON payloads from bronze/products and:
1. Parses them with an explicit schema (schema-on-read).
2. Normalizes brand names (alias map) and text fields.
3. Recomputes the discount rate and flags discrepancies with Tiki's displayed rate.
4. Flags anomalies (is_price_anomaly, is_rating_invalid).
5. Keeps the latest record per product_id (window over crawled_at).
6. Upserts into silver/products with a Delta MERGE (only newer crawls overwrite).

All transformations are native Spark SQL expressions (no Python UDFs), so they run
inside the JVM and scale with the cluster.
"""
import argparse
import logging
import sys
from itertools import chain
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from spark.config import BRONZE_PATH, SILVER_PATH, table_path
from spark.delta_utils import last_operation_metrics, upsert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("silver_products")

PRODUCT_SCHEMA = T.StructType([
    T.StructField("product_id", T.LongType()),
    T.StructField("name", T.StringType()),
    T.StructField("url_key", T.StringType()),
    T.StructField("price", T.DoubleType()),
    T.StructField("original_price", T.DoubleType()),
    T.StructField("discount", T.DoubleType()),
    T.StructField("discount_rate", T.DoubleType()),
    T.StructField("rating_average", T.DoubleType()),
    T.StructField("review_count", T.LongType()),
    T.StructField("quantity_sold", T.LongType()),
    T.StructField("brand_id", T.StringType()),
    T.StructField("brand_name", T.StringType()),
    T.StructField("seller_id", T.StringType()),
    T.StructField("seller_name", T.StringType()),
    T.StructField("is_official_seller", T.BooleanType()),
    T.StructField("subcategory_id", T.StringType()),
    T.StructField("subcategory_name", T.StringType()),
    T.StructField("origin", T.StringType()),
    T.StructField("warranty_type", T.StringType()),
    T.StructField("power_wattage", T.StringType()),
    T.StructField("capacity", T.StringType()),
    T.StructField("is_active", T.BooleanType()),
    T.StructField("crawled_at", T.StringType()),
])

# Brand normalization dictionary (lower-case alias -> canonical name)
BRAND_ALIASES = {
    "locknlock": "Lock&Lock",
    "lock&lock": "Lock&Lock",
    "lock and lock": "Lock&Lock",
    "philip": "Philips",
    "philips": "Philips",
    "sunhouse": "Sunhouse",
    "tefal": "Tefal",
    "elmich": "Elmich",
    "daewoo": "Daewoo",
    "sharp": "Sharp",
    "panasonic": "Panasonic",
    "bluestone": "Bluestone",
}

PRICE_SEGMENTS = [
    (200_000, "Dưới 200k"),
    (500_000, "200k - 500k"),
    (1_500_000, "500k - 1.5M"),
    (3_000_000, "1.5M - 3M"),
]


def normalize_brand(name) -> str:
    """Pure-Python reference of the brand rule (used in unit tests and docs)."""
    if name is None or str(name).strip() == "" or str(name) == "nan":
        return "Unknown"
    cleaned = str(name).strip()
    return BRAND_ALIASES.get(cleaned.lower(), cleaned)


def normalize_brand_col(col: Column) -> Column:
    """Spark expression equivalent of normalize_brand()."""
    alias_map = F.create_map(*[F.lit(x) for x in chain(*BRAND_ALIASES.items())])
    trimmed = F.trim(col)
    return (
        F.when(col.isNull() | (trimmed == ""), F.lit("Unknown"))
        .otherwise(F.coalesce(alias_map[F.lower(trimmed)], trimmed))
    )


def price_segment_col(price: Column) -> Column:
    expr = None
    for upper, label in PRICE_SEGMENTS:
        expr = F.when(price < upper, label) if expr is None else expr.when(price < upper, label)
    return expr.otherwise("Trên 3M")


def transform_products(bronze_df: DataFrame) -> DataFrame:
    """Bronze raw payloads -> one clean, enriched row per product_id."""
    parsed = (
        bronze_df
        .withColumn("p", F.from_json("raw_json", PRODUCT_SCHEMA))
        .select("p.*", "_ingested_at", "_batch_id")
        .filter(F.col("product_id").isNotNull())
    )

    clean = (
        parsed
        .withColumn("price", F.coalesce("price", F.lit(0.0)))
        .withColumn("original_price", F.coalesce("original_price", "price"))
        .withColumn("discount_rate", F.coalesce("discount_rate", F.lit(0.0)))
        .withColumn("rating_average", F.coalesce("rating_average", F.lit(0.0)))
        .withColumn("review_count", F.coalesce("review_count", F.lit(0)).cast("long"))
        .withColumn("quantity_sold", F.coalesce("quantity_sold", F.lit(0)).cast("long"))
        .withColumn("name", F.trim(F.coalesce("name", F.lit(""))))
        .withColumn("brand_name", normalize_brand_col(F.col("brand_name")))
        .withColumn("seller_name", F.trim(F.coalesce("seller_name", F.lit("Unknown"))))
        .withColumn("subcategory_name", F.trim(F.coalesce("subcategory_name", F.lit("Chưa phân loại"))))
        .withColumn("crawled_at", F.to_timestamp("crawled_at"))
    )

    calc_rate = F.when(
        F.col("original_price") > 0,
        (F.col("original_price") - F.col("price")) / F.col("original_price") * 100,
    ).otherwise(F.lit(0.0))

    enriched = (
        clean
        .withColumn("discount_rate_calc", F.round(F.greatest(F.lit(0.0), calc_rate), 2))
        .withColumn("is_price_anomaly",
                    (F.col("price") <= 0) | (F.col("original_price") < F.col("price")))
        .withColumn("is_rating_invalid",
                    (F.col("rating_average") < 0) | (F.col("rating_average") > 5.0))
        # Tiki display bug: reported discount differs from the computed one by > 5 points
        .withColumn("is_discount_discrepancy",
                    (F.abs(F.col("discount_rate") - F.col("discount_rate_calc")) > 5.0)
                    & ~F.col("is_price_anomaly"))
        .withColumn("price_segment", price_segment_col(F.col("price")))
    )

    latest = Window.partitionBy("product_id").orderBy(
        F.col("crawled_at").desc_nulls_last(), F.col("_ingested_at").desc()
    )
    return (
        enriched
        .withColumn("_rn", F.row_number().over(latest))
        .filter("_rn = 1")
        .drop("_rn", "_ingested_at")
        .withColumn("_silver_processed_at", F.current_timestamp())
    )


def process_bronze_to_silver_products(
    spark: SparkSession,
    bronze_path: str = BRONZE_PATH,
    silver_path: str = SILVER_PATH,
) -> int:
    source = table_path(bronze_path, "products")
    target = table_path(silver_path, "products")

    bronze_df = spark.read.format("delta").load(source)
    silver_df = transform_products(bronze_df)

    upsert(
        spark, silver_df, target, keys=["product_id"],
        # only a newer crawl overwrites; re-running the same data rewrites nothing
        update_condition="s.crawled_at > t.crawled_at OR t.crawled_at IS NULL",
    )
    total = spark.read.format("delta").load(target).count()
    logger.info("[SILVER] products: %d rows in table | last commit: %s",
                total, last_operation_metrics(spark, target))
    return total


def main():
    from spark.spark_session import get_spark_session

    argparse.ArgumentParser(description="Bronze products -> Silver").parse_args()
    spark = get_spark_session("silver_products")
    try:
        process_bronze_to_silver_products(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
