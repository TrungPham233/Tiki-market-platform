"""
Automated Data Quality & Validation Gate (Circuit Breaker).

Runs 12 integrity checks on the Silver layer before anything is promoted to Gold:
1. Completeness: tables are not empty.
2. Uniqueness: primary keys have zero duplicates.
3. Non-nullability: critical columns have zero NULLs.
4. Range validity: ratings inside their allowed range.
5. Anomaly threshold: share of price anomalies stays under a tolerated limit (warning only).

Raises DataQualityError if any critical assertion fails (fail-fast), which stops
the Airflow DAG / pipeline runner before the Gold tasks.

The Silver tables are Delta tables; `run_silver_quality_gate_delta` reads only
the columns the checks need. The check functions themselves work on pandas
DataFrames so they are cheap to unit test.
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger("data_quality")

PRODUCT_COLUMNS = ["product_id", "name", "price", "rating_average", "is_price_anomaly"]
REVIEW_COLUMNS = ["review_id", "product_id", "rating"]


class DataQualityError(Exception):
    """Raised when one or more critical data quality assertions fail."""
    pass


class QualityCheckResult:
    def __init__(self, check_name: str, table_name: str, status: str, details: str):
        self.check_name = check_name
        self.table_name = table_name
        self.status = status  # 'PASSED', 'WARNING', 'FAILED'
        self.details = details

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check": self.check_name,
            "table": self.table_name,
            "status": self.status,
            "details": self.details,
        }


def check_not_empty(df: pd.DataFrame, table_name: str, min_rows: int = 1) -> QualityCheckResult:
    """Verify DataFrame is not empty."""
    count = len(df)
    if count >= min_rows:
        return QualityCheckResult("TABLE_NOT_EMPTY", table_name, "PASSED", f"Row count: {count} >= {min_rows}")
    return QualityCheckResult("TABLE_NOT_EMPTY", table_name, "FAILED", f"Row count {count} is below minimum {min_rows}")


def check_not_null(df: pd.DataFrame, column: str, table_name: str) -> QualityCheckResult:
    """Verify critical column has zero null values."""
    if column not in df.columns:
        return QualityCheckResult(f"NOT_NULL_{column}", table_name, "FAILED", f"Column '{column}' does not exist")
    null_count = int(df[column].isna().sum())
    if null_count == 0:
        return QualityCheckResult(f"NOT_NULL_{column}", table_name, "PASSED", f"Zero nulls in '{column}'")
    return QualityCheckResult(f"NOT_NULL_{column}", table_name, "FAILED", f"{null_count} nulls detected in '{column}'")


def check_unique(df: pd.DataFrame, column: str, table_name: str) -> QualityCheckResult:
    """Verify primary key has zero duplicate values."""
    if column not in df.columns:
        return QualityCheckResult(f"UNIQUE_{column}", table_name, "FAILED", f"Column '{column}' does not exist")
    dup_count = int(df.duplicated(subset=[column]).sum())
    if dup_count == 0:
        return QualityCheckResult(f"UNIQUE_{column}", table_name, "PASSED", f"All values in '{column}' are unique")
    return QualityCheckResult(f"UNIQUE_{column}", table_name, "FAILED", f"{dup_count} duplicates found in '{column}'")


def check_numeric_range(
    df: pd.DataFrame, column: str, min_val: float, max_val: float, table_name: str
) -> QualityCheckResult:
    """Verify numerical column falls strictly within designated boundaries."""
    if column not in df.columns:
        return QualityCheckResult(f"RANGE_{column}", table_name, "FAILED", f"Column '{column}' missing")
    invalid = df[(df[column] < min_val) | (df[column] > max_val)]
    if len(invalid) == 0:
        return QualityCheckResult(
            f"RANGE_{column}", table_name, "PASSED", f"All values within [{min_val}, {max_val}]"
        )
    return QualityCheckResult(
        f"RANGE_{column}",
        table_name,
        "FAILED",
        f"{len(invalid)} records outside allowed range [{min_val}, {max_val}]",
    )


def check_anomaly_threshold(
    df: pd.DataFrame, flag_col: str, max_allowed_pct: float, table_name: str
) -> QualityCheckResult:
    """Ensure percentage of flagged anomaly records does not exceed tolerable SLA limit."""
    if flag_col not in df.columns:
        return QualityCheckResult(f"ANOMALY_{flag_col}", table_name, "WARNING", f"Flag column '{flag_col}' not present")
    total = len(df)
    if total == 0:
        return QualityCheckResult(f"ANOMALY_{flag_col}", table_name, "PASSED", "Empty dataset")
    anomalies = int(df[flag_col].sum())
    pct = (anomalies / total) * 100
    if pct <= max_allowed_pct:
        return QualityCheckResult(
            f"ANOMALY_{flag_col}",
            table_name,
            "PASSED",
            f"Anomaly rate: {pct:.2f}% <= allowed {max_allowed_pct:.1f}% ({anomalies}/{total})",
        )
    return QualityCheckResult(
        f"ANOMALY_{flag_col}",
        table_name,
        "WARNING",
        f"High anomaly rate: {pct:.2f}% exceeds threshold {max_allowed_pct:.1f}% ({anomalies}/{total})",
    )


def evaluate_silver_quality(
    df_p: Optional[pd.DataFrame],
    df_r: Optional[pd.DataFrame],
    report_output_file: Optional[Path] = None,
    source_label: str = "",
) -> Dict[str, Any]:
    """Run the 12 checks. A None DataFrame means the Silver table is missing."""
    results: List[QualityCheckResult] = []

    if df_p is not None:
        results.append(check_not_empty(df_p, "silver_products", min_rows=1))
        results.append(check_not_null(df_p, "product_id", "silver_products"))
        results.append(check_not_null(df_p, "name", "silver_products"))
        results.append(check_not_null(df_p, "price", "silver_products"))
        results.append(check_unique(df_p, "product_id", "silver_products"))
        results.append(check_numeric_range(df_p, "rating_average", 0.0, 5.0, "silver_products"))
        results.append(check_anomaly_threshold(df_p, "is_price_anomaly", 5.0, "silver_products"))
    else:
        results.append(QualityCheckResult("TABLE_EXISTS", "silver_products", "FAILED", "Silver products table missing"))

    if df_r is not None:
        results.append(check_not_empty(df_r, "silver_reviews", min_rows=1))
        results.append(check_not_null(df_r, "review_id", "silver_reviews"))
        results.append(check_not_null(df_r, "product_id", "silver_reviews"))
        results.append(check_unique(df_r, "review_id", "silver_reviews"))
        results.append(check_numeric_range(df_r, "rating", 1, 5, "silver_reviews"))
    else:
        results.append(QualityCheckResult("TABLE_EXISTS", "silver_reviews", "FAILED", "Silver reviews table missing"))

    failed_checks = [r for r in results if r.status == "FAILED"]
    warn_checks = [r for r in results if r.status == "WARNING"]
    passed_checks = [r for r in results if r.status == "PASSED"]

    summary = {
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "source": source_label,
        "total_checks": len(results),
        "passed": len(passed_checks),
        "warnings": len(warn_checks),
        "failures": len(failed_checks),
        "gate_status": "PASSED" if not failed_checks else "FAILED",
        "results": [r.to_dict() for r in results],
    }

    if report_output_file:
        report_output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(report_output_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

    logger.info("Quality Gate Evaluation: %d Passed, %d Warnings, %d Failed",
                len(passed_checks), len(warn_checks), len(failed_checks))

    if failed_checks:
        fail_msgs = "; ".join([f"{f.table_name}.{f.check_name}: {f.details}" for f in failed_checks])
        raise DataQualityError(f"Data Quality Gate Failed: {fail_msgs}")

    return summary


def run_silver_quality_gate(
    silver_products_file: Path,
    silver_reviews_file: Path,
    report_output_file: Optional[Path] = None,
) -> Dict[str, Any]:
    """Quality gate over Parquet extracts (used by unit tests and ad-hoc checks)."""
    df_p = pd.read_parquet(silver_products_file) if silver_products_file.exists() else None
    df_r = pd.read_parquet(silver_reviews_file) if silver_reviews_file.exists() else None
    return evaluate_silver_quality(df_p, df_r, report_output_file, source_label="parquet")


def run_silver_quality_gate_delta(
    spark,
    silver_path: str,
    report_output_file: Optional[Path] = None,
) -> Dict[str, Any]:
    """Quality gate over the Silver Delta tables (used by the pipeline and the DAG)."""
    from delta.tables import DeltaTable

    def load(table: str, cols: List[str]) -> Optional[pd.DataFrame]:
        path = f"{silver_path}/{table}"
        if not DeltaTable.isDeltaTable(spark, path):
            return None
        return spark.read.format("delta").load(path).select(*cols).toPandas()

    return evaluate_silver_quality(
        load("products", PRODUCT_COLUMNS),
        load("reviews", REVIEW_COLUMNS),
        report_output_file,
        source_label="delta",
    )
