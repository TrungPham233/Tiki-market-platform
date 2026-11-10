"""
Unit Tests for Phase 4: Data Quality Checks, Circuit Breaker & Quality Gate.
"""
from pathlib import Path
import pandas as pd
import pytest
from dags.data_quality import (
    DataQualityError,
    check_anomaly_threshold,
    check_not_empty,
    check_not_null,
    check_numeric_range,
    check_unique,
    run_silver_quality_gate,
)


def test_check_not_empty():
    df_valid = pd.DataFrame([{"id": 1}])
    df_empty = pd.DataFrame()
    assert check_not_empty(df_valid, "test").status == "PASSED"
    assert check_not_empty(df_empty, "test").status == "FAILED"


def test_check_not_null():
    df_clean = pd.DataFrame([{"id": 1, "price": 100}, {"id": 2, "price": 200}])
    df_corrupted = pd.DataFrame([{"id": 1, "price": None}, {"id": 2, "price": 200}])
    assert check_not_null(df_clean, "price", "test").status == "PASSED"
    assert check_not_null(df_corrupted, "price", "test").status == "FAILED"


def test_check_unique():
    df_unique = pd.DataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
    df_dups = pd.DataFrame([{"id": 1}, {"id": 2}, {"id": 1}])
    assert check_unique(df_unique, "id", "test").status == "PASSED"
    assert check_unique(df_dups, "id", "test").status == "FAILED"


def test_check_numeric_range():
    df_valid = pd.DataFrame([{"rating": 4.5}, {"rating": 5.0}, {"rating": 1.0}])
    df_invalid = pd.DataFrame([{"rating": 4.5}, {"rating": 9.9}])
    assert check_numeric_range(df_valid, "rating", 1.0, 5.0, "test").status == "PASSED"
    assert check_numeric_range(df_invalid, "rating", 1.0, 5.0, "test").status == "FAILED"


def test_check_anomaly_threshold():
    # 1 out of 20 = 5% anomaly
    records = [{"anomaly": False}] * 19 + [{"anomaly": True}]
    df = pd.DataFrame(records)
    # Allowed 10% -> Should PASS
    assert check_anomaly_threshold(df, "anomaly", 10.0, "test").status == "PASSED"
    # Allowed 2% -> Should WARN
    assert check_anomaly_threshold(df, "anomaly", 2.0, "test").status == "WARNING"


def test_silver_quality_gate_passes_clean_data(tmp_path):
    p_file = tmp_path / "clean_products.parquet"
    r_file = tmp_path / "clean_reviews.parquet"

    df_p = pd.DataFrame([
        {
            "product_id": 101,
            "name": "Nồi cơm điện",
            "price": 500000,
            "rating_average": 4.8,
            "is_price_anomaly": False,
        }
    ])
    df_r = pd.DataFrame([
        {
            "review_id": 201,
            "product_id": 101,
            "rating": 5,
        }
    ])
    df_p.to_parquet(p_file)
    df_r.to_parquet(r_file)

    summary = run_silver_quality_gate(p_file, r_file)
    assert summary["gate_status"] == "PASSED"
    assert summary["failures"] == 0


def test_silver_quality_gate_fails_corrupted_data(tmp_path):
    p_file = tmp_path / "clean_products.parquet"
    r_file = tmp_path / "clean_reviews.parquet"

    # Corrupt data: duplicate product_id and invalid rating
    df_p = pd.DataFrame([
        {"product_id": 101, "name": "Item A", "price": 100, "rating_average": 99.0, "is_price_anomaly": False},
        {"product_id": 101, "name": "Item B", "price": 200, "rating_average": 4.0, "is_price_anomaly": False},
    ])
    df_r = pd.DataFrame([{"review_id": 201, "product_id": 101, "rating": 5}])
    df_p.to_parquet(p_file)
    df_r.to_parquet(r_file)

    with pytest.raises(DataQualityError):
        run_silver_quality_gate(p_file, r_file)
