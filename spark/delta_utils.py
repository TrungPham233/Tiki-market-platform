"""
Shared Delta Lake helpers used by the Medallion jobs.
"""
import logging
from pathlib import Path
from typing import List

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger("delta_utils")


def is_delta(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def upsert(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    keys: List[str],
    partition_by: List[str] = None,
    update_condition: str = None,
) -> None:
    """
    Idempotent upsert: MERGE on business keys if the table exists,
    otherwise create it. Re-running a load never creates duplicates.
    update_condition: optional SQL predicate (aliases t/s) that must hold to
    overwrite an existing row, e.g. only accept newer records.
    """
    if not is_delta(spark, path):
        writer = df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        logger.info("Created Delta table %s", path)
        return

    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll(condition=update_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info("Merged into Delta table %s on %s", path, keys)


def overwrite(spark: SparkSession, df: DataFrame, path: str) -> None:
    """Full refresh of a small table (e.g. aggregates, conformed lookups)."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)


def last_operation_metrics(spark: SparkSession, path: str) -> dict:
    """Return operation + metrics of the latest commit (rows inserted/updated...)."""
    row = DeltaTable.forPath(spark, path).history(1).select("operation", "operationMetrics").first()
    return {"operation": row["operation"], **(row["operationMetrics"] or {})}


def export_to_parquet(spark: SparkSession, path: str, out_file: Path) -> int:
    """
    Export a Gold Delta table to a single Parquet file for the serving layer
    (DWH loader / Power BI). Gold tables are small aggregates and dimensions,
    so collecting to the driver is acceptable here.
    """
    pdf = spark.read.format("delta").load(path).toPandas()
    out_file.parent.mkdir(parents=True, exist_ok=True)
    pdf.to_parquet(out_file, index=False, engine="pyarrow")
    return len(pdf)
