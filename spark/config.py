"""
Lakehouse Storage & Pipeline Configuration for Apache Spark.

Every Medallion table is stored as a Delta Lake table (a directory with a
_delta_log). The serving layer holds plain Parquet snapshots exported from
Gold, consumed by the DWH loader and Power BI.
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

# Local Lakehouse Directories (Medallion Layers)
LAKEHOUSE_DIR = DATA_DIR / "lakehouse"
BRONZE_DIR = LAKEHOUSE_DIR / "bronze"
SILVER_DIR = LAKEHOUSE_DIR / "silver"
GOLD_DIR = LAKEHOUSE_DIR / "gold"
SERVING_DIR = LAKEHOUSE_DIR / "serving"
CHECKPOINT_DIR = LAKEHOUSE_DIR / "_checkpoints"

for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, SERVING_DIR, CHECKPOINT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# MinIO / Cloud S3A URIs (if running with MinIO)
USE_MINIO = os.getenv("USE_MINIO", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")

if USE_MINIO:
    BRONZE_PATH = f"s3a://{MINIO_BUCKET}/bronze"
    SILVER_PATH = f"s3a://{MINIO_BUCKET}/silver"
    GOLD_PATH = f"s3a://{MINIO_BUCKET}/gold"
else:
    BRONZE_PATH = BRONZE_DIR.as_posix()
    SILVER_PATH = SILVER_DIR.as_posix()
    GOLD_PATH = GOLD_DIR.as_posix()


def table_path(layer_path: str, table: str) -> str:
    """Return the storage path of a Delta table inside a Medallion layer."""
    return f"{layer_path}/{table}"


# Kafka streaming source
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PRICE_EVENTS_TOPIC = os.getenv("PRICE_EVENTS_TOPIC", "raw-price-events")
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
