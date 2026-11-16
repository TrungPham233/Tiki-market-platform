"""
Refresh the Power BI source files from the Lakehouse serving layer.

Reads data/lakehouse/serving/*.parquet (exported from the Gold Delta tables) and writes:
- dashboard/data/tiki_market_dwh_gold.xlsx  (one sheet per table, read by the PBIP model)
- dashboard/data/<table>.csv                (same data, handy for review / tests)

The sheet columns are kept identical to the semantic model (TMDL) so the report
refreshes without schema changes; extra lakehouse columns are left out of the sheets.
"""
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from spark.config import SERVING_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("export_dashboard_data")

OUT_DIR = Path(__file__).resolve().parent / "data"

# sheet name -> (serving table, columns expected by the Power BI model)
SHEETS = {
    "fact_product_snapshot": ("fact_product_snapshot", [
        "snapshot_id", "date_key", "product_sk", "product_id", "brand_id", "seller_id", "subcategory_id",
        "price", "original_price", "discount_rate_calc", "quantity_sold", "rating_average", "review_count",
        "price_segment", "is_discount_discrepancy"]),
    "fact_reviews": ("fact_reviews", [
        "review_id", "product_sk", "product_id", "date_key", "customer_hash", "rating", "sentiment_label",
        "is_after_sales_complaint", "is_defect_complaint", "thank_count", "tokenized_text"]),
    "dim_product": ("dim_product", [
        "product_id", "name", "brand_id", "seller_id", "subcategory_id", "original_price", "origin",
        "warranty_type", "power_wattage", "capacity", "product_sk", "valid_from", "valid_to", "is_current"]),
    "dim_brand": ("dim_brand", ["brand_id", "brand_name"]),
    "dim_seller": ("dim_seller", ["seller_id", "seller_name", "is_official_seller"]),
    "dim_subcategory": ("dim_subcategory", ["subcategory_id", "subcategory_name"]),
    "dim_date": ("dim_date", ["date_key", "full_date", "day_of_week", "day_name", "month", "month_name",
                              "quarter", "year", "is_weekend"]),
    "agg_brand_market_share": ("agg_brand_market_share", [
        "brand_name", "sku_count", "total_quantity_sold", "total_estimated_revenue", "avg_rating",
        "revenue_share_pct", "volume_share_pct"]),
    "agg_discount_saturation": ("agg_discount_saturation", [
        "discount_bracket", "sku_count", "avg_quantity_sold", "median_quantity_sold", "avg_rating"]),
    "agg_voc_complaint_causes": ("agg_voc_complaint_causes", [
        "complaint_category", "complaint_count", "percentage_of_negative"]),
    "alerts": ("price_alerts", [
        "alert_id", "alert_type", "product_id", "product_name", "brand_name", "seller_name", "old_price",
        "new_price", "price_drop_pct", "savings_amount", "triggered_at"]),
}
CSV_ONLY = ["agg_quality_risk_watchlist", "agg_assortment_gaps"]


def strip_tz(df: pd.DataFrame) -> pd.DataFrame:
    """Excel cannot store timezone-aware datetimes."""
    for c in df.columns:
        if isinstance(df[c].dtype, pd.DatetimeTZDtype):
            df[c] = df[c].dt.tz_localize(None)
    return df


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    xlsx = OUT_DIR / "tiki_market_dwh_gold.xlsx"
    with pd.ExcelWriter(xlsx, engine="openpyxl") as writer:
        for sheet, (table, cols) in SHEETS.items():
            df = strip_tz(pd.read_parquet(SERVING_DIR / f"{table}.parquet")[cols])
            df.to_excel(writer, sheet_name=sheet, index=False)
            df.to_csv(OUT_DIR / f"{sheet}.csv", index=False, encoding="utf-8")
            logger.info("%-26s %6d rows", sheet, len(df))
    for table in CSV_ONLY:
        df = pd.read_parquet(SERVING_DIR / f"{table}.parquet")
        df.to_csv(OUT_DIR / f"{table}.csv", index=False, encoding="utf-8")
        logger.info("%-26s %6d rows (csv only)", table, len(df))
    logger.info("Dashboard sources refreshed in %s", OUT_DIR)


if __name__ == "__main__":
    main()
