"""
Bronze Layer Ingestion Job (PySpark + Delta Lake).

Lands raw JSONL records unchanged in the Bronze layer (schema-on-read):
- raw_json:       the original payload as a string (nothing is lost if Tiki adds/changes fields)
- _record_hash:   SHA-256 of the payload, used to make re-ingestion idempotent
- _ingested_at, _batch_id, _source_file, ingest_date: audit metadata

Table layout: bronze/<dataset> (Delta, partitioned by ingest_date).
Re-running the job on the same file inserts nothing new (insert-only MERGE on _record_hash).
"""
import argparse
import logging
import sys
import uuid
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from data_collection.config import RAW_DATA_DIR
from spark.config import BRONZE_PATH, table_path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ingest_to_bronze")


def ingest_jsonl_to_bronze(
    spark: SparkSession,
    input_file: Path,
    dataset_name: str,
    bronze_path: str = BRONZE_PATH,
) -> int:
    """Append new raw JSONL lines to bronze/<dataset_name>. Returns rows newly inserted."""
    if not input_file.exists():
        logger.error("Input file %s does not exist!", input_file)
        return 0

    batch_id = uuid.uuid4().hex[:8]
    df = (
        spark.read.text(input_file.as_posix())
        .withColumnRenamed("value", "raw_json")
        .filter(F.length(F.trim("raw_json")) > 0)
        .withColumn("_record_hash", F.sha2("raw_json", 256))
        .dropDuplicates(["_record_hash"])
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_file", F.lit(input_file.name))
        .withColumn("ingest_date", F.to_date(F.current_timestamp()))
    )

    target = table_path(bronze_path, dataset_name)
    if not DeltaTable.isDeltaTable(spark, target):
        df.write.format("delta").mode("overwrite").partitionBy("ingest_date").save(target)
        inserted = spark.read.format("delta").load(target).count()
    else:
        before = spark.read.format("delta").load(target).count()
        (
            DeltaTable.forPath(spark, target)
            .alias("t")
            .merge(df.alias("s"), "t._record_hash = s._record_hash")
            .whenNotMatchedInsertAll()
            .execute()
        )
        inserted = spark.read.format("delta").load(target).count() - before

    logger.info("[BRONZE] %s: %d new raw records (batch %s) -> %s",
                dataset_name, inserted, batch_id, target)
    return inserted


def main():
    from spark.spark_session import get_spark_session

    parser = argparse.ArgumentParser(description="Ingest Raw Data to Bronze Layer")
    parser.add_argument("--products", default=str(RAW_DATA_DIR / "products_raw.jsonl"))
    parser.add_argument("--reviews", default=str(RAW_DATA_DIR / "reviews_raw.jsonl"))
    args = parser.parse_args()

    spark = get_spark_session("bronze_ingestion")
    try:
        p = ingest_jsonl_to_bronze(spark, Path(args.products), "products")
        r = ingest_jsonl_to_bronze(spark, Path(args.reviews), "reviews")
        logger.info("Bronze ingestion complete: %d products, %d reviews", p, r)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
