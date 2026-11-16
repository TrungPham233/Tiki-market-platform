"""
Unit Tests for Phase 5: Data Warehouse Sync & Analytics Views.

Uses the versioned sample extracts in dashboard/data/ so the test is deterministic
and independent of whatever the local pipeline last produced.
"""
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_warehouse.load_gold_to_postgres import execute_analytics_views, load_parquet_table

CSV_DIR = PROJECT_ROOT / "dashboard" / "data"


def to_parquet(tmp_path: Path, name: str) -> Path:
    out = tmp_path / f"{name}.parquet"
    pd.read_csv(CSV_DIR / f"{name}.csv").to_parquet(out)
    return out


def test_sqlite_dwh_sync_and_views(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'test_dwh.db'}")

    b_count = load_parquet_table(engine, to_parquet(tmp_path, "dim_brand"), "dim_brand", "sqlite")
    p_count = load_parquet_table(engine, to_parquet(tmp_path, "fact_product_snapshot"), "fact_product_snapshot", "sqlite")
    r_count = load_parquet_table(engine, to_parquet(tmp_path, "fact_reviews"), "fact_reviews", "sqlite")
    assert b_count > 0 and p_count > 0 and r_count > 0

    execute_analytics_views(engine, "sqlite")

    snap = pd.read_csv(CSV_DIR / "fact_product_snapshot.csv")
    latest = snap[snap["date_key"] == snap["date_key"].max()]
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT total_skus, total_units_sold, category_avg_rating FROM analytics_vw_market_overview"
        )).fetchone()
    assert row is not None
    assert row[0] == latest["product_id"].nunique()  # views use the latest snapshot day
    assert row[2] > 0.0
