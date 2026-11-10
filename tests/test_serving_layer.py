"""
Unit Tests for Phase 5: Data Warehouse Sync & Analytics Views.
"""
from pathlib import Path
import pytest
from sqlalchemy import create_engine, text
from data_warehouse.load_gold_to_postgres import load_parquet_table, execute_analytics_views
from spark.config import GOLD_DIR


def test_sqlite_dwh_sync_and_views(tmp_path):
    sqlite_db = tmp_path / "test_dwh.db"
    engine = create_engine(f"sqlite:///{sqlite_db}")

    # Load tables from actual Gold artifacts
    b_count = load_parquet_table(engine, GOLD_DIR / "dim_brand.parquet", "dim_brand", "sqlite")
    p_count = load_parquet_table(engine, GOLD_DIR / "fact_product_snapshot.parquet", "fact_product_snapshot", "sqlite")
    r_count = load_parquet_table(engine, GOLD_DIR / "fact_reviews.parquet", "fact_reviews", "sqlite")

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

