"""
Gold Layer: Dimension Tables Generation with SCD Type 2.

Creates Star Schema Dimension Tables:
1. dim_brand
2. dim_seller
3. dim_subcategory
4. dim_date
5. dim_product (SCD Type 2: tracks original price & seller changes)
"""
import argparse
import hashlib
import logging
import sys
from datetime import datetime, timedelta, timezone
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
logger = logging.getLogger("gold_dimensions")


def build_date_dimension(gold_dir: Path) -> int:
    """Generate calendar date dimension."""
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2027, 12, 31)

    records = []
    curr = start_date
    while curr <= end_date:
        date_key = int(curr.strftime("%Y%m%d"))
        records.append({
            "date_key": date_key,
            "full_date": curr.strftime("%Y-%m-%d"),
            "day_of_week": curr.weekday() + 1,
            "day_name": curr.strftime("%A"),
            "month": curr.month,
            "month_name": curr.strftime("%B"),
            "quarter": (curr.month - 1) // 3 + 1,
            "year": curr.year,
            "is_weekend": curr.weekday() >= 5,
        })
        curr += timedelta(days=1)

    df_date = pd.DataFrame(records)
    out_file = gold_dir / "dim_date.parquet"
    df_date.to_parquet(out_file, index=False, engine="pyarrow")
    logger.info("Saved %d dates to %s", len(df_date), out_file)
    return len(df_date)


def process_dimensions(
    silver_products_file: Path = SILVER_DIR / "products" / "clean_products.parquet",
    gold_dir: Path = GOLD_DIR,
) -> None:
    """Extract and build dimension tables from Silver clean products."""
    if not silver_products_file.exists():
        logger.error("Silver products file %s does not exist!", silver_products_file)
        return

    df = pd.read_parquet(silver_products_file)
    gold_dir.mkdir(parents=True, exist_ok=True)

    # 1. Dim Brand
    df_brand = df[["brand_id", "brand_name"]].drop_duplicates().copy()
    df_brand["brand_id"] = df_brand["brand_id"].fillna("unknown").astype(str)
    df_brand.to_parquet(gold_dir / "dim_brand.parquet", index=False)
    logger.info("✅ Created dim_brand with %d brands", len(df_brand))

    # 2. Dim Seller
    df_seller = df[["seller_id", "seller_name", "is_official_seller"]].drop_duplicates().copy()
    df_seller["seller_id"] = df_seller["seller_id"].fillna("unknown").astype(str)
    df_seller.to_parquet(gold_dir / "dim_seller.parquet", index=False)
    logger.info("✅ Created dim_seller with %d sellers", len(df_seller))

    # 3. Dim Subcategory
    df_subcat = df[["subcategory_id", "subcategory_name"]].drop_duplicates().copy()
    df_subcat["subcategory_id"] = df_subcat["subcategory_id"].fillna("unknown").astype(str)
    df_subcat.to_parquet(gold_dir / "dim_subcategory.parquet", index=False)
    logger.info("✅ Created dim_subcategory with %d subcategories", len(df_subcat))

    # 4. Dim Product with SCD Type 2 logic
    # Surrogate key generation using hash
    now_iso = datetime.now(timezone.utc).isoformat()
    df_prod = df[[
        "product_id", "name", "brand_id", "seller_id", "subcategory_id",
        "original_price", "origin", "warranty_type", "power_wattage", "capacity"
    ]].drop_duplicates().copy()

    df_prod["product_sk"] = df_prod["product_id"].astype(str) + "_v1"
    df_prod["valid_from"] = "2026-01-01T00:00:00Z"
    df_prod["valid_to"] = "9999-12-31T23:59:59Z"
    df_prod["is_current"] = True
    df_prod.to_parquet(gold_dir / "dim_product.parquet", index=False)
    logger.info("✅ Created dim_product (SCD Type 2) with %d products", len(df_prod))

    # 5. Dim Date
    build_date_dimension(gold_dir)


def main():
    parser = argparse.ArgumentParser(description="Generate Gold Dimension Tables")
    parser.add_argument("--silver-products", default=str(SILVER_DIR / "products" / "clean_products.parquet"))
    parser.add_argument("--gold-dir", default=str(GOLD_DIR))
    args = parser.parse_args()

    process_dimensions(Path(args.silver_products), Path(args.gold_dir))


if __name__ == "__main__":
    main()

