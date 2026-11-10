"""
Gold Layer to PostgreSQL Data Warehouse Loader.

Reads Star Schema Parquet files from Gold Lakehouse and synchronizes
Dimension and Fact tables into PostgreSQL (or local SQLite fallback).
Executes analytics views creation.
"""
import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine, text

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from spark.config import GOLD_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [DWHLoader] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("dwh_loader")


def get_db_engine():
    """Attempt connecting to PostgreSQL, with graceful fallback to local SQLite."""
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "tiki_dwh")
    pg_user = os.getenv("POSTGRES_USER", "postgres")
    pg_pw = os.getenv("POSTGRES_PASSWORD", "postgres")

    pg_url = f"postgresql+psycopg2://{pg_user}:{pg_pw}@{pg_host}:{pg_port}/{pg_db}"
    try:
        engine = create_engine(pg_url, connect_args={"connect_timeout": 3})
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Connected to PostgreSQL DWH at %s:%s/%s", pg_host, pg_port, pg_db)
        return engine, "postgres"
    except Exception as err:
        logger.warning("Could not connect to PostgreSQL (%s). Falling back to local SQLite DWH.", err)
        sqlite_path = Path(__file__).resolve().parent.parent / "data" / "tiki_dwh.db"
        sqlite_url = f"sqlite:///{sqlite_path}"
        engine = create_engine(sqlite_url)
        logger.info("Using local SQLite Data Warehouse at: %s", sqlite_path)
        return engine, "sqlite"


def load_parquet_table(engine, file_path: Path, table_name: str, db_type: str) -> int:
    """Load a single parquet file into SQL table."""
    if not file_path.exists():
        logger.warning("File %s does not exist, skipping table %s", file_path, table_name)
        return 0

    df = pd.read_parquet(file_path)
    schema_name = "dwh" if db_type == "postgres" else None

    # SQLite does not support schemas, prefix table name
    target_table = table_name if db_type == "postgres" else f"dwh_{table_name}"

    df.to_sql(
        name=target_table,
        con=engine,
        schema=schema_name,
        if_exists="replace",
        index=False,
        chunksize=1000,
    )
    logger.info("✅ Loaded %d records into %s", len(df), target_table)
    return len(df)


def execute_analytics_views(engine, db_type: str) -> None:
    """Execute analytics views creation."""
    sql_dir = Path(__file__).resolve().parent / "sql"
    views_file = sql_dir / "views.sql"

    if db_type == "postgres" and views_file.exists():
        with open(views_file, "r", encoding="utf-8") as f:
            sql_script = f.read()
        with engine.begin() as conn:
            # Execute views script
            for statement in sql_script.split(";"):
                stmt = statement.strip()
                if stmt:
                    try:
                        conn.execute(text(stmt))
                    except Exception as err:
                        logger.warning("Error creating view: %s", err)
        logger.info("✅ PostgreSQL analytics views created successfully.")
    elif db_type == "sqlite":
        # Create simplified SQLite views for local testing/Power BI
        sqlite_views = [
            """
            CREATE VIEW IF NOT EXISTS analytics_vw_market_overview AS
            SELECT
                COUNT(DISTINCT product_id) AS total_skus,
                SUM(quantity_sold) AS total_units_sold,
                SUM(price * quantity_sold) AS total_estimated_revenue,
                ROUND(AVG(rating_average), 2) AS category_avg_rating
            FROM dwh_fact_product_snapshot;
            """,
            """
            CREATE VIEW IF NOT EXISTS analytics_vw_brand_market_share AS
            SELECT
                COALESCE(b.brand_name, 'Unknown') AS brand_name,
                COUNT(f.product_id) AS sku_count,
                SUM(f.quantity_sold) AS total_units_sold,
                SUM(f.price * f.quantity_sold) AS estimated_revenue,
                ROUND(AVG(f.rating_average), 2) AS avg_rating
            FROM dwh_fact_product_snapshot f
            LEFT JOIN dwh_dim_brand b ON f.brand_id = b.brand_id
            GROUP BY b.brand_name
            ORDER BY estimated_revenue DESC;
            """,
            """
            CREATE VIEW IF NOT EXISTS analytics_vw_discount_saturation AS
            SELECT
                f.price_segment,
                COUNT(f.product_id) AS sku_count,
                ROUND(AVG(f.quantity_sold), 1) AS avg_units_sold,
                ROUND(AVG(f.rating_average), 2) AS avg_rating
            FROM dwh_fact_product_snapshot f
            GROUP BY f.price_segment;
            """,
        ]
        with engine.begin() as conn:
            for v_sql in sqlite_views:
                conn.execute(text(v_sql))
        logger.info("✅ SQLite analytics views created successfully.")


def run_dwh_sync(gold_dir: Path = GOLD_DIR) -> Dict[str, int]:
    """Execute complete sync from Gold Lakehouse to Data Warehouse."""
    engine, db_type = get_db_engine()

    # Pre-create schemas in postgres
    if db_type == "postgres":
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS dwh;"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics;"))

    tables_loaded = {}

    # Dimensions
    tables_loaded["dim_brand"] = load_parquet_table(engine, gold_dir / "dim_brand.parquet", "dim_brand", db_type)
    tables_loaded["dim_seller"] = load_parquet_table(engine, gold_dir / "dim_seller.parquet", "dim_seller", db_type)
    tables_loaded["dim_subcategory"] = load_parquet_table(engine, gold_dir / "dim_subcategory.parquet", "dim_subcategory", db_type)
    tables_loaded["dim_date"] = load_parquet_table(engine, gold_dir / "dim_date.parquet", "dim_date", db_type)
    tables_loaded["dim_product"] = load_parquet_table(engine, gold_dir / "dim_product.parquet", "dim_product", db_type)

    # Facts
    tables_loaded["fact_product_snapshot"] = load_parquet_table(engine, gold_dir / "fact_product_snapshot.parquet", "fact_product_snapshot", db_type)
    tables_loaded["fact_reviews"] = load_parquet_table(engine, gold_dir / "fact_reviews.parquet", "fact_reviews", db_type)

    # Alerts
    alerts_file = gold_dir / "alerts" / "price_alerts.parquet"
    if alerts_file.exists():
        tables_loaded["alerts"] = load_parquet_table(engine, alerts_file, "alerts", db_type)

    # Execute analytics views
    execute_analytics_views(engine, db_type)

    logger.info("=====================================================")
    logger.info("🎉 DWH SYNC COMPLETED SUCCESSFULLY (%s mode)", db_type.upper())
    for tbl, count in tables_loaded.items():
        logger.info("   - %s: %d records", tbl, count)
    logger.info("=====================================================")

    return tables_loaded


def main():
    parser = argparse.ArgumentParser(description="Load Gold Lakehouse into Data Warehouse")
    parser.add_argument("--gold-dir", default=str(GOLD_DIR))
    args = parser.parse_args()

    run_dwh_sync(Path(args.gold_dir))


if __name__ == "__main__":
    main()

