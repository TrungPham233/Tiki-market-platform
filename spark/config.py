"""
Lakehouse Storage & Pipeline Configuration for Apache Spark.
Supports local filesystem and MinIO S3A storage.
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

for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# MinIO / Cloud S3A URIs (if running with MinIO)
USE_MINIO = os.getenv("USE_MINIO", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")

if USE_MINIO:
    BRONZE_PATH = f"s3a://{MINIO_BUCKET}/bronze"
    SILVER_PATH = f"s3a://{MINIO_BUCKET}/silver"
    GOLD_PATH = f"s3a://{MINIO_BUCKET}/gold"
else:
    BRONZE_PATH = str(BRONZE_DIR)
    SILVER_PATH = str(SILVER_DIR)
    GOLD_PATH = str(GOLD_DIR)
