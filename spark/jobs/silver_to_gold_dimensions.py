"""
Gold Layer: Star Schema Dimensions (PySpark + Delta Lake).

- dim_brand, dim_seller, dim_subcategory: Type 1 (MERGE upsert on the natural key)
- dim_date: generated calendar (2025-2027)
- dim_product: **SCD Type 2** implemented with a Delta MERGE.

SCD2 design for dim_product
---------------------------
Tracked attributes: name, original_price (list price), brand, seller, subcategory.
(The selling price changes almost daily, so it lives in fact_product_snapshot
instead of exploding the dimension.)

Each run:
1. Hash the tracked attributes of the latest Silver record (row_hash).
2. Compare with the current dimension row (is_current = true).
3. Changed products: expire the current row (is_current=false, valid_to=new valid_from)
   and insert a new version (version + 1).
   New products: insert version 1.
   Unchanged products: no-op (re-running is idempotent).
Both steps happen in ONE atomic MERGE using the standard "staged updates" pattern
(rows to expire are keyed by product_id, rows to insert get a NULL merge key).
"""
import argparse
import logging
import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from spark.config import GOLD_PATH, SILVER_PATH, table_path
from spark.delta_utils import last_operation_metrics, overwrite, upsert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("gold_dimensions")

SCD2_TRACKED_COLUMNS = [
    "name", "original_price", "brand_id", "seller_id", "subcategory_id",
]
PRODUCT_ATTRIBUTES = SCD2_TRACKED_COLUMNS + [
    "origin", "warranty_type", "power_wattage", "capacity",
]
# "Open" end date of the current version. 2099 instead of 9999 keeps the value inside
# the pandas datetime64[ns] range (max 2262) used by the serving export / Power BI.
OPEN_END = "2099-12-31 23:59:59"


def build_date_dimension(spark: SparkSession) -> DataFrame:
    return (
        spark.sql("SELECT explode(sequence(to_date('2025-01-01'), to_date('2027-12-31'))) AS d")
        .select(
            F.date_format("d", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("d").alias("full_date"),
            F.dayofweek("d").alias("day_of_week"),
            F.date_format("d", "EEEE").alias("day_name"),
            F.month("d").alias("month"),
            F.date_format("d", "MMMM").alias("month_name"),
            F.quarter("d").alias("quarter"),
            F.year("d").alias("year"),
            F.dayofweek("d").isin(1, 7).alias("is_weekend"),
        )
    )


def prepare_product_changes(silver_products: DataFrame) -> DataFrame:
    """Latest Silver state per product + hash of SCD2-tracked attributes."""
    return (
        silver_products
        .select("product_id", *PRODUCT_ATTRIBUTES, "crawled_at")
        .withColumn(
            "row_hash",
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("∅"))
                                       for c in SCD2_TRACKED_COLUMNS]), 256),
        )
        .withColumn("valid_from", F.coalesce("crawled_at", F.current_timestamp()))
        .drop("crawled_at")
    )


def apply_scd2(spark: SparkSession, incoming: DataFrame, target: str) -> dict:
    """Apply an SCD Type 2 MERGE of `incoming` into the dim_product Delta table."""
    def finalize(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("product_sk",
                          F.concat(F.col("product_id").cast("string"), F.lit("_v"),
                                   F.col("version").cast("string")))
            .withColumn("valid_to", F.to_timestamp(F.lit(OPEN_END)))
            .withColumn("is_current", F.lit(True))
        )

    if not DeltaTable.isDeltaTable(spark, target):
        first = finalize(incoming.withColumn("version", F.lit(1)))
        first.write.format("delta").mode("overwrite").save(target)
        n = first.count()
        return {"operation": "CREATE", "inserted_versions": n, "expired_versions": 0}

    dim = DeltaTable.forPath(spark, target)
    current = dim.toDF().filter("is_current").select(
        "product_id", F.col("row_hash").alias("cur_hash"), F.col("version").alias("cur_version")
    )

    changed = (
        incoming.join(current, "product_id")
        .filter(F.col("row_hash") != F.col("cur_hash"))
        .withColumn("version", F.col("cur_version") + 1)
        .drop("cur_hash", "cur_version")
    )
    brand_new = incoming.join(current, "product_id", "left_anti").withColumn("version", F.lit(1))
    changed = changed.cache()
    brand_new = brand_new.cache()
    n_changed, n_new = changed.count(), brand_new.count()
    if n_changed == 0 and n_new == 0:
        # nothing to do; note Delta does not commit an empty MERGE
        return {"operation": "NOOP", "inserted_versions": 0, "expired_versions": 0}
    to_insert = finalize(changed.unionByName(brand_new))

    staged = (
        changed.select("product_id", "valid_from", "row_hash")
        .withColumn("merge_key", F.col("product_id"))
        .unionByName(
            to_insert.select("product_id", "valid_from", "row_hash")
            .withColumn("merge_key", F.lit(None).cast("long")),
        )
        .join(to_insert.drop("valid_from", "row_hash"), "product_id", "left")
    )
    # rows with merge_key = product_id expire the current version;
    # rows with merge_key = NULL never match and are inserted as new versions.
    insert_values = {c: f"s.{c}" for c in to_insert.columns}
    (
        dim.alias("t")
        .merge(staged.alias("s"), "t.product_id = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={"is_current": "false", "valid_to": "s.valid_from"},
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
    m = last_operation_metrics(spark, target)
    changed.unpersist()
    brand_new.unpersist()
    return {
        "operation": m["operation"],
        "inserted_versions": int(m.get("numTargetRowsInserted", 0)),
        "expired_versions": int(m.get("numTargetRowsUpdated", 0)),
        "changed_products": n_changed,
        "new_products": n_new,
    }


def process_dimensions(
    spark: SparkSession,
    silver_path: str = SILVER_PATH,
    gold_path: str = GOLD_PATH,
) -> dict:
    silver = spark.read.format("delta").load(table_path(silver_path, "products"))

    upsert(spark,
           silver.select("brand_id", "brand_name")
           .withColumn("brand_id", F.coalesce("brand_id", F.lit("unknown")))
           .dropDuplicates(["brand_id"]),
           table_path(gold_path, "dim_brand"), keys=["brand_id"])
    upsert(spark,
           silver.select("seller_id", "seller_name", "is_official_seller")
           .withColumn("seller_id", F.coalesce("seller_id", F.lit("unknown")))
           .dropDuplicates(["seller_id"]),
           table_path(gold_path, "dim_seller"), keys=["seller_id"])
    upsert(spark,
           silver.select("subcategory_id", "subcategory_name")
           .withColumn("subcategory_id", F.coalesce("subcategory_id", F.lit("unknown")))
           .dropDuplicates(["subcategory_id"]),
           table_path(gold_path, "dim_subcategory"), keys=["subcategory_id"])
    overwrite(spark, build_date_dimension(spark), table_path(gold_path, "dim_date"))

    stats = apply_scd2(spark, prepare_product_changes(silver), table_path(gold_path, "dim_product"))
    dim = spark.read.format("delta").load(table_path(gold_path, "dim_product"))
    stats["total_rows"] = dim.count()
    stats["current_rows"] = dim.filter("is_current").count()
    logger.info("[GOLD] dim_product SCD2: %s", stats)
    return stats


def main():
    from spark.spark_session import get_spark_session

    argparse.ArgumentParser(description="Build Gold dimensions (SCD2)").parse_args()
    spark = get_spark_session("gold_dimensions")
    try:
        process_dimensions(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
