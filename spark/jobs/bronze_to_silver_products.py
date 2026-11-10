"""
Silver Layer: Products Cleaning, Enrichment, and Anomaly Detection Job.

Processes Bronze products:
1. Normalizes brand & seller names (resolves aliases like Lock&Lock / LocknLock).
2. Computes discount_rate_calc and detects discount discrepancies.
3. Flags data anomalies (is_price_anomaly, is_rating_invalid).
4. Deduplicates by product_id keeping the latest record.
5. Saves clean dataset to Silver layer.
"""
import argparse
import glob
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from spark.config import BRONZE_DIR, SILVER_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("silver_products")

# Brand normalization dictionary
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


def normalize_brand(name: str) -> str:
    """Standardize brand name variations."""
    if not name or pd.isna(name) or name.strip() == "":
        return "Unknown"
    cleaned = name.strip()
    return BRAND_ALIASES.get(cleaned.lower(), cleaned)


def process_bronze_to_silver_products(
    bronze_dir: Path = BRONZE_DIR / "products",
    output_dir: Path = SILVER_DIR / "products",
) -> int:
    """Execute product cleaning and transformation."""
    parquet_files = list(bronze_dir.glob("**/*.parquet"))
    if not parquet_files:
        logger.error("No Bronze products parquet files found in %s", bronze_dir)
        return 0

    logger.info("Reading %d Bronze parquet files...", len(parquet_files))
    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs, ignore_index=True)
    initial_count = len(df)
    logger.info("Loaded %d raw records from Bronze", initial_count)

    # 1. Type casting
    df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").fillna(0).astype(int)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    df["original_price"] = pd.to_numeric(df["original_price"], errors="coerce").fillna(df["price"])
    df["discount_rate"] = pd.to_numeric(df["discount_rate"], errors="coerce").fillna(0)
    df["rating_average"] = pd.to_numeric(df["rating_average"], errors="coerce").fillna(0.0)
    df["review_count"] = pd.to_numeric(df["review_count"], errors="coerce").fillna(0).astype(int)
    df["quantity_sold"] = pd.to_numeric(df["quantity_sold"], errors="coerce").fillna(0).astype(int)

    # 2. String cleaning & normalization
    df["name"] = df["name"].fillna("").astype(str).str.strip()
    df["brand_name"] = df["brand_name"].apply(normalize_brand)
    df["seller_name"] = df["seller_name"].fillna("Unknown").astype(str).str.strip()
    df["subcategory_name"] = df["subcategory_name"].fillna("Chưa phân loại").astype(str).str.strip()

    # 3. Calculated Columns
    # Calculated discount rate
    calc_rate = np.where(
        df["original_price"] > 0,
        ((df["original_price"] - df["price"]) / df["original_price"]) * 100,
        0.0,
    )
    df["discount_rate_calc"] = np.round(np.maximum(0.0, calc_rate), 2)

    # 4. Data Quality & Anomaly Flags
    df["is_price_anomaly"] = (df["price"] <= 0) | (df["original_price"] < df["price"])
    df["is_rating_invalid"] = (df["rating_average"] < 0) | (df["rating_average"] > 5.0)
    # Detect Tiki display discount rate bug (> 5% discrepancy between reported and calculated rate)
    df["is_discount_discrepancy"] = (
        (df["discount_rate"] - df["discount_rate_calc"]).abs() > 5.0
    ) & (~df["is_price_anomaly"])

    # 5. Price Segments for Merchandising Analysis
    bins = [0, 200000, 500000, 1500000, 3000000, float("inf")]
    labels = ["Dưới 200k", "200k - 500k", "500k - 1.5M", "1.5M - 3M", "Trên 3M"]
    df["price_segment"] = pd.cut(df["price"], bins=bins, labels=labels, right=False)

    # 6. Deduplication by product_id keeping latest crawled_at
    if "crawled_at" in df.columns:
        df = df.sort_values("crawled_at", ascending=False)
    df = df.drop_duplicates(subset=["product_id"], keep="first")

    # 7. Audit metadata
    df["_silver_processed_at"] = datetime.now(timezone.utc).isoformat()

    # Save to Silver layer
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / "clean_products.parquet"
    df.to_parquet(out_file, index=False, engine="pyarrow")

    anomalies = int(df["is_discount_discrepancy"].sum())
    logger.info("✅ [SILVER] Cleaned %d products (from %d Bronze records). Discrepancies: %d",
                len(df), initial_count, anomalies)
    logger.info("Saved to: %s", out_file)
    return len(df)


def main():
    parser = argparse.ArgumentParser(description="Process Bronze products to Silver")
    parser.add_argument("--bronze", default=str(BRONZE_DIR / "products"))
    parser.add_argument("--silver", default=str(SILVER_DIR / "products"))
    args = parser.parse_args()

    process_bronze_to_silver_products(Path(args.bronze), Path(args.silver))


if __name__ == "__main__":
    main()
