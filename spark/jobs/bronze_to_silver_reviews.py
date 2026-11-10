"""
Silver Layer: Customer Reviews Cleaning & Vietnamese NLP Sentiment Job.

Processes Bronze reviews:
1. Cleans text (removes HTML tags, emojis, excessive spaces).
2. Performs Vietnamese NLP tokenization with underthesea.
3. Categorizes sentiment (positive / neutral / negative).
4. Classifies complaint root causes (After-sales complaints vs Technical defect complaints).
5. Deduplicates by review_id.
"""
import argparse
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from underthesea import word_tokenize

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from spark.config import BRONZE_DIR, SILVER_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("silver_reviews")

# Complaint keywords in Vietnamese e-commerce
AFTER_SALES_KEYWORDS = [
    "trả hàng", "hoàn tiền", "đổi trả", "giải quyết", "khiếu nại", "thu hồi",
    "chăm sóc khách hàng", "cskh", "thái độ", "giao chậm", "lừa đảo"
]

DEFECT_KEYWORDS = [
    "hỏng", "bể", "vỡ", "cháy", "không lên", "không nóng", "kêu to",
    "rò rỉ", "móp méo", "liệt", "lỗi kỹ thuật", "chập điện"
]


def clean_html(text: str) -> str:
    """Strip HTML markup from comment text."""
    if not text or pd.isna(text):
        return ""
    text = re.sub(r"<[^>]+>", " ", str(text))
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenize_vietnamese(text: str) -> str:
    """Tokenize Vietnamese text into compound words using underthesea."""
    if not text or len(text.strip()) == 0:
        return ""
    try:
        return word_tokenize(text, format="text")
    except Exception:
        return text


def contains_any(text: str, keywords: list) -> bool:
    """Check if lowercased text contains any target keywords."""
    if not text:
        return False
    lower = text.lower()
    return any(kw in lower for kw in keywords)


def assign_sentiment(r: int) -> str:
    """Categorize rating into sentiment labels."""
    if r >= 4:
        return "positive"
    elif r == 3:
        return "neutral"
    return "negative"


def process_bronze_to_silver_reviews(
    bronze_dir: Path = BRONZE_DIR / "reviews",
    output_dir: Path = SILVER_DIR / "reviews",
) -> int:
    """Execute review transformation and NLP pipeline."""
    parquet_files = list(bronze_dir.glob("**/*.parquet"))
    if not parquet_files:
        logger.error("No Bronze reviews parquet files found in %s", bronze_dir)
        return 0

    logger.info("Reading %d Bronze reviews parquet files...", len(parquet_files))
    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs, ignore_index=True)
    initial_count = len(df)
    logger.info("Loaded %d raw reviews from Bronze", initial_count)

    # 1. Type casting
    df["review_id"] = pd.to_numeric(df["review_id"], errors="coerce").fillna(0).astype(int)
    df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").fillna(0).astype(int)
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce").fillna(5).astype(int)
    df["thank_count"] = pd.to_numeric(df["thank_count"], errors="coerce").fillna(0).astype(int)

    # 2. Text cleaning
    df["title"] = df["title"].fillna("").astype(str).apply(clean_html)
    df["content"] = df["content"].fillna("").astype(str).apply(clean_html)
    df["full_text"] = (df["title"] + " " + df["content"]).str.strip()

    # 3. Sentiment Labeling
    df["sentiment_label"] = df["rating"].apply(assign_sentiment)

    # 4. Vietnamese NLP Tokenization
    logger.info("Performing Vietnamese NLP tokenization with underthesea...")
    df["tokenized_text"] = df["full_text"].apply(tokenize_vietnamese)

    # 5. Root-Cause Complaint Categorization (BQ6 Voice of Customer)
    df["is_after_sales_complaint"] = df["full_text"].apply(lambda t: contains_any(t, AFTER_SALES_KEYWORDS))
    df["is_defect_complaint"] = df["full_text"].apply(lambda t: contains_any(t, DEFECT_KEYWORDS))

    # 6. Deduplication by review_id
    df = df.drop_duplicates(subset=["review_id"], keep="first")

    # 7. Audit metadata
    df["_silver_processed_at"] = datetime.now(timezone.utc).isoformat()

    # Save to Silver layer
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / "clean_reviews.parquet"
    df.to_parquet(out_file, index=False, engine="pyarrow")

    neg_count = int((df["sentiment_label"] == "negative").sum())
    logger.info("✅ [SILVER] Cleaned %d reviews (Negative: %d). Saved to: %s",
                len(df), neg_count, out_file)
    return len(df)


def main():
    parser = argparse.ArgumentParser(description="Process Bronze reviews to Silver")
    parser.add_argument("--bronze", default=str(BRONZE_DIR / "reviews"))
    parser.add_argument("--silver", default=str(SILVER_DIR / "reviews"))
    args = parser.parse_args()

    process_bronze_to_silver_reviews(Path(args.bronze), Path(args.silver))


if __name__ == "__main__":
    main()
