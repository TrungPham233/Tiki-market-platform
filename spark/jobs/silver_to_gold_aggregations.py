"""
Gold Layer: Business Intelligence & Aggregation Tables Job.

Answers the 6 Core Business Questions (BQ1 to BQ6):
- BQ1: agg_discount_saturation (Effectiveness vs Diminishing Returns)
- BQ2: agg_brand_market_share (Market share, Sales Volume, CR5 Concentration)
- BQ3: agg_quality_risk_watchlist (High reviews + Low rating SKUs)
- BQ4: agg_assortment_gaps (Price segments vs Subcategories gap analysis)
- BQ6: agg_voc_complaint_causes (After-sales complaints vs Hardware defects)
"""
import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from spark.config import GOLD_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("gold_aggregations")


def compute_business_aggregates(gold_dir: Path = GOLD_DIR) -> None:
    """Generate business aggregation datasets in Gold layer."""
    fact_prod_file = gold_dir / "fact_product_snapshot.parquet"
    fact_rev_file = gold_dir / "fact_reviews.parquet"
    dim_prod_file = gold_dir / "dim_product.parquet"
    dim_brand_file = gold_dir / "dim_brand.parquet"

    if not fact_prod_file.exists():
        logger.error("fact_product_snapshot not found in %s", gold_dir)
        return

    df_p = pd.read_parquet(fact_prod_file)
    df_dim_p = pd.read_parquet(dim_prod_file) if dim_prod_file.exists() else df_p
    df_brand = pd.read_parquet(dim_brand_file) if dim_brand_file.exists() else None

    # Join brand name
    if df_brand is not None:
        df_p = df_p.merge(df_brand, on="brand_id", how="left")
    else:
        df_p["brand_name"] = "Unknown"

    # Merge product details for names
    if "name" in df_dim_p.columns:
        df_p = df_p.merge(df_dim_p[["product_sk", "name"]], on="product_sk", how="left")
    else:
        df_p["name"] = "Product " + df_p["product_id"].astype(str)

    # ----------------------------------------------------
    # BQ2: Brand Market Share & CR5 Index
    # ----------------------------------------------------
    df_p["estimated_revenue"] = df_p["price"] * df_p["quantity_sold"]
    total_market_rev = df_p["estimated_revenue"].sum()
    total_market_vol = df_p["quantity_sold"].sum()

    brand_grp = df_p.groupby("brand_name").agg(
        sku_count=("product_id", "count"),
        total_quantity_sold=("quantity_sold", "sum"),
        total_estimated_revenue=("estimated_revenue", "sum"),
        avg_rating=("rating_average", "mean"),
    ).reset_index()

    brand_grp["revenue_share_pct"] = np.round(
        (brand_grp["total_estimated_revenue"] / total_market_rev) * 100, 2
    ) if total_market_rev > 0 else 0.0

    brand_grp["volume_share_pct"] = np.round(
        (brand_grp["total_quantity_sold"] / total_market_vol) * 100, 2
    ) if total_market_vol > 0 else 0.0

    brand_grp = brand_grp.sort_values("revenue_share_pct", ascending=False)
    # Compute Top 5 Concentration Ratio (CR5)
    cr5_val = brand_grp.head(5)["revenue_share_pct"].sum()
    logger.info("CR5 Index (Top 5 Brands Revenue Share): %.2f%%", cr5_val)

    brand_grp.to_parquet(gold_dir / "agg_brand_market_share.parquet", index=False)
    logger.info("✅ Saved agg_brand_market_share (%d brands)", len(brand_grp))

    # ----------------------------------------------------
    # BQ1: Discount Saturation Analysis
    # ----------------------------------------------------
    discount_bins = [0, 10, 20, 30, 50, 100]
    discount_labels = ["0 - 10%", "10 - 20%", "20 - 30%", "30 - 50%", "Trên 50%"]
    df_p["discount_bracket"] = pd.cut(
        df_p["discount_rate_calc"], bins=discount_bins, labels=discount_labels, right=False
    )

    disc_grp = df_p.groupby("discount_bracket", observed=False).agg(
        sku_count=("product_id", "count"),
        avg_quantity_sold=("quantity_sold", "mean"),
        median_quantity_sold=("quantity_sold", "median"),
        avg_rating=("rating_average", "mean"),
    ).reset_index()
    disc_grp.to_parquet(gold_dir / "agg_discount_saturation.parquet", index=False)
    logger.info("✅ Saved agg_discount_saturation")

    # ----------------------------------------------------
    # BQ3: Quality Risk Watchlist (High reviews, low rating)
    # ----------------------------------------------------
    risk_skus = df_p[
        (df_p["review_count"] >= 5) & (df_p["rating_average"] < 4.5)
    ][["product_id", "name", "brand_name", "price", "rating_average", "review_count"]].copy()
    risk_skus = risk_skus.sort_values("rating_average", ascending=True)
    risk_skus.to_parquet(gold_dir / "agg_quality_risk_watchlist.parquet", index=False)
    logger.info("✅ Saved agg_quality_risk_watchlist (%d risky SKUs identified)", len(risk_skus))

    # ----------------------------------------------------
    # BQ4: Assortment Gaps (Price Segment x Subcategory)
    # ----------------------------------------------------
    assort_matrix = pd.crosstab(
        df_p["subcategory_id"],
        df_p["price_segment"],
        values=df_p["product_id"],
        aggfunc="count",
    ).fillna(0).reset_index()
    assort_matrix.to_parquet(gold_dir / "agg_assortment_gaps.parquet", index=False)
    logger.info("✅ Saved agg_assortment_gaps")

    # ----------------------------------------------------
    # BQ6: Voice of Customer (VoC) Root Cause Complaints
    # ----------------------------------------------------
    if fact_rev_file.exists():
        df_r = pd.read_parquet(fact_rev_file)
        neg_reviews = df_r[df_r["sentiment_label"] == "negative"]
        total_neg = len(neg_reviews)

        after_sales_count = int(neg_reviews["is_after_sales_complaint"].sum()) if total_neg > 0 else 0
        defect_count = int(neg_reviews["is_defect_complaint"].sum()) if total_neg > 0 else 0

        voc_summary = pd.DataFrame([
            {
                "complaint_category": "Quy trình Hậu mãi (Đổi trả / Hoàn tiền / CSKH)",
                "complaint_count": after_sales_count,
                "percentage_of_negative": round((after_sales_count / total_neg) * 100, 2) if total_neg > 0 else 0.0,
            },
            {
                "complaint_category": "Lỗi Kỹ thuật / Hỏng hóc Sản phẩm",
                "complaint_count": defect_count,
                "percentage_of_negative": round((defect_count / total_neg) * 100, 2) if total_neg > 0 else 0.0,
            }
        ])
        voc_summary.to_parquet(gold_dir / "agg_voc_complaint_causes.parquet", index=False)
        logger.info("✅ Saved agg_voc_complaint_causes: After-sales=%d, Defect=%d",
                    after_sales_count, defect_count)


def main():
    parser = argparse.ArgumentParser(description="Generate Gold Aggregations")
    parser.add_argument("--gold-dir", default=str(GOLD_DIR))
    args = parser.parse_args()

    compute_business_aggregates(Path(args.gold_dir))


if __name__ == "__main__":
    main()
