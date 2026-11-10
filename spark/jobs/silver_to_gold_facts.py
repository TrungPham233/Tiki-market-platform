"""
Gold Layer: Fact Tables Generation Job.

Constructs Star Schema Fact Tables:
1. fact_product_snapshot: Daily metric snapshot of product sales, pricing, and ratings.
2. fact_reviews: Customer feedback facts linked to products and calendar dates.
"""
import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from spark.config import GOLD_DIR, SILVER_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("gold_facts")


def process_facts(
    silver_products_file: Path = SILVER_DIR / "products" / "clean_products.parquet",
    silver_reviews_file: Path = SILVER_DIR / "reviews" / "clean_reviews.parquet",
    gold_dir: Path = GOLD_DIR,
) -> None:
    """Build Fact tables from Silver cleaned datasets."""
    gold_dir.mkdir(parents=True, exist_ok=True)
    today_key = int(datetime.now(timezone.utc).strftime("%Y%m%d"))

    # 1. Build fact_product_snapshot
    if silver_products_file.exists():
        df_prod = pd.read_parquet(silver_products_file)
        df_prod["snapshot_id"] = [f"snap_{uuid.uuid4().hex[:10]}" for _ in range(len(df_prod))]
        df_prod["date_key"] = today_key
        df_prod["product_sk"] = df_prod["product_id"].astype(str) + "_v1"

        fact_prod = df_prod[[
            "snapshot_id", "date_key", "product_sk", "product_id",
            "brand_id", "seller_id", "subcategory_id",
            "price", "original_price", "discount_rate_calc",
            "quantity_sold", "rating_average", "review_count",
            "price_segment", "is_discount_discrepancy"
        ]].copy()

        out_prod = gold_dir / "fact_product_snapshot.parquet"
        fact_prod.to_parquet(out_prod, index=False, engine="pyarrow")
        logger.info("✅ Created fact_product_snapshot with %d rows -> %s", len(fact_prod), out_prod)

    # 2. Build fact_reviews
    if silver_reviews_file.exists():
        df_rev = pd.read_parquet(silver_reviews_file)
        df_rev["product_sk"] = df_rev["product_id"].astype(str) + "_v1"

        # Extract date_key from created_at
        def parse_date_key(dt_str: str) -> int:
            if not dt_str or pd.isna(dt_str):
                return today_key
            try:
                clean_dt = str(dt_str)[:10].replace("-", "")
                return int(clean_dt)
            except Exception:
                return today_key

        df_rev["date_key"] = df_rev["created_at"].apply(parse_date_key)

        fact_rev = df_rev[[
            "review_id", "product_sk", "product_id", "date_key",
            "customer_hash", "rating", "sentiment_label",
            "is_after_sales_complaint", "is_defect_complaint",
            "thank_count", "tokenized_text"
        ]].copy()

        out_rev = gold_dir / "fact_reviews.parquet"
        fact_rev.to_parquet(out_rev, index=False, engine="pyarrow")
        logger.info("✅ Created fact_reviews with %d rows -> %s", len(fact_rev), out_rev)


def main():
    parser = argparse.ArgumentParser(description="Generate Gold Fact Tables")
    parser.add_argument("--products", default=str(SILVER_DIR / "products" / "clean_products.parquet"))
    parser.add_argument("--reviews", default=str(SILVER_DIR / "reviews" / "clean_reviews.parquet"))
    parser.add_argument("--gold-dir", default=str(GOLD_DIR))
    args = parser.parse_args()

    process_facts(Path(args.products), Path(args.reviews), Path(args.gold_dir))


if __name__ == "__main__":
    main()

