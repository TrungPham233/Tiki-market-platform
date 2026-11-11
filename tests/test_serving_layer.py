"""
Unit Tests for Phase 5: Data Warehouse Sync & Analytics Views.
"""
import sys
from pathlib import Path
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_warehouse.load_gold_to_postgres import load_parquet_table, execute_analytics_views
from spark.config import GOLD_DIR


def test_sqlite_dwh_sync_and_views(tmp_path):
    sqlite_db = tmp_path / "test_dwh.db"
    engine = create_engine(f"sqlite:///{sqlite_db}")

    # Prepare gold parquet files: use actual lakehouse gold files if present,
    # or fallback to converting sample CSV artifacts (e.g. in fresh CI environment)
    csv_dir = Path(__file__).resolve().parent.parent / "dashboard" / "data"

    brand_pq = GOLD_DIR / "dim_brand.parquet"
    if not brand_pq.exists():
        brand_pq = tmp_path / "dim_brand.parquet"
        pd.read_csv(csv_dir / "dim_brand.csv").to_parquet(brand_pq)

    snapshot_pq = GOLD_DIR / "fact_product_snapshot.parquet"
    if not snapshot_pq.exists():
        snapshot_pq = tmp_path / "fact_product_snapshot.parquet"
        pd.read_csv(csv_dir / "fact_product_snapshot.csv").to_parquet(snapshot_pq)

    reviews_pq = GOLD_DIR / "fact_reviews.parquet"
    if not reviews_pq.exists():
        reviews_pq = tmp_path / "fact_reviews.parquet"
        pd.read_csv(csv_dir / "fact_reviews.csv").to_parquet(reviews_pq)

    # Load tables
    b_count = load_parquet_table(engine, brand_pq, "dim_brand", "sqlite")
    p_count = load_parquet_table(engine, snapshot_pq, "fact_product_snapshot", "sqlite")
    r_count = load_parquet_table(engine, reviews_pq, "fact_reviews", "sqlite")

    assert b_count > 0
    assert p_count > 0
    assert r_count > 0

    # Execute views
    execute_analytics_views(engine, "sqlite")

    # Query view
    with engine.connect() as conn:
        res = conn.execute(text("SELECT total_skus, total_units_sold, category_avg_rating FROM analytics_vw_market_overview"))
        row = res.fetchone()
        assert row is not None
        assert row[0] == p_count  # total_skus matches product count
        assert row[2] > 0.0      # valid avg rating

