"""
Silver Layer: Customer Reviews Cleaning & Vietnamese Text Processing (PySpark + Delta).

Reads raw JSON payloads from bronze/reviews and:
1. Parses them with an explicit schema.
2. Strips HTML and collapses whitespace (native Spark regex).
3. Labels sentiment from the star rating (4-5 positive, 3 neutral, 1-2 negative).
4. Tags complaint causes with keyword rules (after-sales vs product defect).
5. Tokenizes Vietnamese text with underthesea.
6. Keeps one row per review_id and upserts into silver/reviews (Delta MERGE).

Note on step 5: underthesea is a Python library, so tokenization runs on the
driver over the (small) set of distinct review texts and is joined back.
Everything else stays in native Spark expressions.
"""
import argparse
import logging
import re
import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from spark.config import BRONZE_PATH, SILVER_PATH, table_path
from spark.delta_utils import last_operation_metrics, upsert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("silver_reviews")

REVIEW_SCHEMA = T.StructType([
    T.StructField("review_id", T.LongType()),
    T.StructField("product_id", T.LongType()),
    T.StructField("customer_hash", T.StringType()),
    T.StructField("rating", T.IntegerType()),
    T.StructField("title", T.StringType()),
    T.StructField("content", T.StringType()),
    T.StructField("thank_count", T.LongType()),
    T.StructField("is_buyer", T.BooleanType()),
    T.StructField("created_at", T.StringType()),
    T.StructField("crawled_at", T.StringType()),
])

# Complaint keywords in Vietnamese e-commerce
AFTER_SALES_KEYWORDS = [
    "trả hàng", "hoàn tiền", "đổi trả", "giải quyết", "khiếu nại", "thu hồi",
    "chăm sóc khách hàng", "cskh", "thái độ", "giao chậm", "lừa đảo"
]

DEFECT_KEYWORDS = [
    "hỏng", "bể", "vỡ", "cháy", "không lên", "không nóng", "kêu to",
    "rò rỉ", "móp méo", "liệt", "lỗi kỹ thuật", "chập điện"
]


# ---- Pure-Python reference rules (unit-tested; Spark versions below mirror them) ----
def clean_html(text) -> str:
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    text = re.sub(r"<[^>]+>", " ", str(text))
    return re.sub(r"\s+", " ", text).strip()


def contains_any(text: str, keywords: list) -> bool:
    if not text:
        return False
    lower = text.lower()
    return any(kw in lower for kw in keywords)


def assign_sentiment(r: int) -> str:
    if r >= 4:
        return "positive"
    elif r == 3:
        return "neutral"
    return "negative"


# ---- Spark expressions ----
def clean_html_col(col: Column) -> Column:
    no_tags = F.regexp_replace(F.coalesce(col, F.lit("")), r"<[^>]+>", " ")
    return F.trim(F.regexp_replace(no_tags, r"\s+", " "))


def keyword_flag_col(text: Column, keywords: list) -> Column:
    pattern = "|".join(re.escape(k) for k in keywords)
    return F.lower(text).rlike(pattern)


def sentiment_col(rating: Column) -> Column:
    return (
        F.when(rating >= 4, "positive")
        .when(rating == 3, "neutral")
        .otherwise("negative")
    )


def transform_reviews(bronze_df: DataFrame) -> DataFrame:
    parsed = (
        bronze_df
        .withColumn("r", F.from_json("raw_json", REVIEW_SCHEMA))
        .select("r.*", "_ingested_at")
        .filter(F.col("review_id").isNotNull())
    )
    latest = Window.partitionBy("review_id").orderBy(F.col("_ingested_at").desc())
    return (
        parsed
        .withColumn("_rn", F.row_number().over(latest))
        .filter("_rn = 1")
        .drop("_rn", "_ingested_at")
        .withColumn("rating", F.coalesce("rating", F.lit(5)))
        .withColumn("thank_count", F.coalesce("thank_count", F.lit(0)))
        .withColumn("title", clean_html_col(F.col("title")))
        .withColumn("content", clean_html_col(F.col("content")))
        .withColumn("full_text", F.trim(F.concat_ws(" ", "title", "content")))
        .withColumn("sentiment_label", sentiment_col(F.col("rating")))
        .withColumn("is_after_sales_complaint", keyword_flag_col(F.col("full_text"), AFTER_SALES_KEYWORDS))
        .withColumn("is_defect_complaint", keyword_flag_col(F.col("full_text"), DEFECT_KEYWORDS))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("crawled_at", F.to_timestamp("crawled_at"))
    )


def add_vietnamese_tokens(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Tokenize distinct non-empty texts with underthesea on the driver, then join back."""
    from underthesea import word_tokenize

    texts = df.select("full_text").where(F.length("full_text") > 0).distinct().toPandas()

    def tok(t: str) -> str:
        try:
            return word_tokenize(t, format="text")
        except Exception:
            return t

    texts["tokenized_text"] = texts["full_text"].map(tok)
    logger.info("Tokenized %d distinct review texts with underthesea", len(texts))
    if texts.empty:
        return df.withColumn("tokenized_text", F.lit(""))

    tokens = spark.createDataFrame(texts)
    return (
        df.join(F.broadcast(tokens), on="full_text", how="left")
        .withColumn("tokenized_text", F.coalesce("tokenized_text", F.lit("")))
    )


def process_bronze_to_silver_reviews(
    spark: SparkSession,
    bronze_path: str = BRONZE_PATH,
    silver_path: str = SILVER_PATH,
) -> int:
    source = table_path(bronze_path, "reviews")
    target = table_path(silver_path, "reviews")

    reviews = transform_reviews(spark.read.format("delta").load(source))
    reviews = add_vietnamese_tokens(spark, reviews).withColumn(
        "_silver_processed_at", F.current_timestamp()
    )

    upsert(spark, reviews, target, keys=["review_id"],
           update_condition="s.crawled_at > t.crawled_at OR t.crawled_at IS NULL")
    silver = spark.read.format("delta").load(target)
    total = silver.count()
    negative = silver.filter("sentiment_label = 'negative'").count()
    logger.info("[SILVER] reviews: %d rows (%d negative) | last commit: %s",
                total, negative, last_operation_metrics(spark, target))
    return total


def main():
    from spark.spark_session import get_spark_session

    argparse.ArgumentParser(description="Bronze reviews -> Silver").parse_args()
    spark = get_spark_session("silver_reviews")
    try:
        process_bronze_to_silver_reviews(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
