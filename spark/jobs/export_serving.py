"""
Serving Layer Export.

Exports the Gold Delta tables to single Parquet files under data/lakehouse/serving/
for the DWH loader (PostgreSQL/SQLite) and Power BI.
"""
import logging
import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from spark.config import GOLD_PATH, SERVING_DIR, table_path
from spark.delta_utils import export_to_parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("export_serving")

GOLD_TABLES = [
    "dim_brand", "dim_seller", "dim_subcategory", "dim_date", "dim_product",
    "fact_product_snapshot", "fact_reviews", "price_alerts",
    "agg_brand_market_share", "agg_discount_saturation", "agg_quality_risk_watchlist",
    "agg_assortment_gaps", "agg_voc_complaint_causes",
]


def export_serving_layer(spark: SparkSession, gold_path: str = GOLD_PATH,
                         serving_dir: Path = SERVING_DIR) -> dict:
    counts = {}
    for name in GOLD_TABLES:
        path = table_path(gold_path, name)
        if DeltaTable.isDeltaTable(spark, path):
            counts[name] = export_to_parquet(spark, path, serving_dir / f"{name}.parquet")
    logger.info("[SERVING] exported %d tables to %s: %s", len(counts), serving_dir, counts)
    return counts


def main():
    from spark.spark_session import get_spark_session

    spark = get_spark_session("export_serving")
    try:
        export_serving_layer(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
