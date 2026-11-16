"""
Gold Layer: Business Aggregates (PySpark + Delta Lake).

Built from the latest snapshot of each product in fact_product_snapshot:
- BQ1 agg_discount_saturation    : units sold & rating by discount bracket
- BQ2 agg_brand_market_share     : estimated revenue/volume share per brand + CR5
- BQ3 agg_quality_risk_watchlist : many reviews but rating < 4.5
- BQ4 agg_assortment_gaps        : SKU count per subcategory x price segment
- BQ6 agg_voc_complaint_causes   : complaint causes among negative reviews

Every aggregate carries its sample size (sku_count / review counts) so the
dashboard can show how much data each number is based on.
"""
import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from spark.config import GOLD_PATH, table_path
from spark.delta_utils import overwrite

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("gold_aggregations")

# Products without reviews have rating_average = 0; leave them out of rating averages.
def rated():
    return F.when(F.col("review_count") > 0, F.col("rating_average"))

DISCOUNT_BRACKETS = [(10, "0 - 10%"), (20, "10 - 20%"), (30, "20 - 30%"), (50, "30 - 50%")]


def compute_business_aggregates(spark: SparkSession, gold_path: str = GOLD_PATH) -> dict:
    def gold(name):
        return spark.read.format("delta").load(table_path(gold_path, name))

    latest = Window.partitionBy("product_id").orderBy(F.col("date_key").desc())
    snap = (
        gold("fact_product_snapshot")
        .withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn")
        .join(gold("dim_brand"), "brand_id", "left")
        .join(gold("dim_product").filter("is_current").select("product_id", "name"), "product_id", "left")
        .withColumn("brand_name", F.coalesce("brand_name", F.lit("Unknown")))
        .withColumn("estimated_revenue", F.col("price") * F.col("quantity_sold"))
        .cache()
    )
    total_skus = snap.count()

    # BQ2: brand share + CR5
    totals = snap.agg(F.sum("estimated_revenue").alias("rev"), F.sum("quantity_sold").alias("vol")).first()
    rev_total, vol_total = totals["rev"] or 0, totals["vol"] or 0
    brand = (
        snap.groupBy("brand_name").agg(
            F.count("product_id").alias("sku_count"),
            F.sum("quantity_sold").alias("total_quantity_sold"),
            F.sum("estimated_revenue").alias("total_estimated_revenue"),
            F.round(F.avg(rated()), 2).alias("avg_rating"),
        )
        .withColumn("revenue_share_pct",
                    F.round(F.col("total_estimated_revenue") / F.lit(rev_total or 1) * 100, 2))
        .withColumn("volume_share_pct",
                    F.round(F.col("total_quantity_sold") / F.lit(vol_total or 1) * 100, 2))
        .withColumn("revenue_rank", F.row_number().over(Window.orderBy(F.col("total_estimated_revenue").desc())))
    )
    overwrite(spark, brand, table_path(gold_path, "agg_brand_market_share"))
    cr5 = brand.filter("revenue_rank <= 5").agg(F.sum("revenue_share_pct")).first()[0] or 0.0
    n_brands = brand.count()

    # BQ1: discount saturation
    bracket = None
    for upper, label in DISCOUNT_BRACKETS:
        cond = F.col("discount_rate_calc") < upper
        bracket = F.when(cond, label) if bracket is None else bracket.when(cond, label)
    disc = (
        snap.withColumn("discount_bracket", bracket.otherwise("Trên 50%"))
        .groupBy("discount_bracket").agg(
            F.count("product_id").alias("sku_count"),
            F.round(F.avg("quantity_sold"), 1).alias("avg_quantity_sold"),
            F.percentile_approx("quantity_sold", 0.5).alias("median_quantity_sold"),
            F.round(F.avg(rated()), 2).alias("avg_rating"),
            F.count(rated()).alias("rated_sku_count"),
        )
    )
    overwrite(spark, disc, table_path(gold_path, "agg_discount_saturation"))

    # BQ3: quality risk watchlist
    risk = (
        snap.filter((F.col("review_count") >= 5) & (F.col("rating_average") < 4.5))
        .select("product_id", "name", "brand_name", "price", "rating_average", "review_count")
        .orderBy("rating_average")
    )
    overwrite(spark, risk, table_path(gold_path, "agg_quality_risk_watchlist"))

    # BQ4: assortment gaps, long format (subcategory x price segment, zero-filled)
    segments = snap.select("price_segment").distinct()
    subcats = snap.select("subcategory_id").distinct()
    gaps = (
        subcats.crossJoin(segments)
        .join(snap.groupBy("subcategory_id", "price_segment").agg(F.count("product_id").alias("sku_count")),
              ["subcategory_id", "price_segment"], "left")
        .fillna(0, ["sku_count"])
    )
    overwrite(spark, gaps, table_path(gold_path, "agg_assortment_gaps"))

    # BQ6: voice of customer among negative reviews
    neg = gold("fact_reviews").filter("sentiment_label = 'negative'")
    n_neg = neg.count()
    voc_row = neg.agg(
        F.sum(F.col("is_after_sales_complaint").cast("int")).alias("after_sales"),
        F.sum(F.col("is_defect_complaint").cast("int")).alias("defect"),
    ).first()
    after_sales, defect = int(voc_row["after_sales"] or 0), int(voc_row["defect"] or 0)
    # built from pandas (Arrow path, no Python worker needed)
    voc = spark.createDataFrame(pd.DataFrame({
        "complaint_category": ["Quy trình Hậu mãi (Đổi trả / Hoàn tiền / CSKH)",
                               "Lỗi Kỹ thuật / Hỏng hóc Sản phẩm"],
        "complaint_count": [after_sales, defect],
        "negative_reviews": [n_neg, n_neg],
    })).withColumn(
        "percentage_of_negative",
        F.round(F.when(F.col("negative_reviews") > 0,
                       F.col("complaint_count") / F.col("negative_reviews") * 100).otherwise(0.0), 2),
    )
    overwrite(spark, voc, table_path(gold_path, "agg_voc_complaint_causes"))
    snap.unpersist()

    stats = {
        "skus": total_skus, "brands": n_brands, "cr5_pct": round(float(cr5), 2),
        "negative_reviews": n_neg, "after_sales_complaints": after_sales, "defect_complaints": defect,
    }
    logger.info("[GOLD] aggregates: %s", stats)
    return stats


def main():
    from spark.spark_session import get_spark_session

    argparse.ArgumentParser(description="Build Gold aggregates").parse_args()
    spark = get_spark_session("gold_aggregations")
    try:
        compute_business_aggregates(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
