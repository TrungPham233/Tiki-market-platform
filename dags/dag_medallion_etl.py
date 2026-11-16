"""
Airflow DAG: Medallion Architecture ETL Pipeline with Automated Quality Gate.

Schedule: 04:00 AM UTC Daily (Triggered after daily crawl).
Flow:
  Bronze Ingest
       │
       ▼
  ┌───────────────────────┐
  │ Parallel Transforms   │
  │ - Silver Products     │
  │ - Silver Reviews(NLP) │
  └───────────┬───────────┘
              ▼
   [ DATA QUALITY GATE ]   <-- Fail-Fast: halts pipeline if critical rules fail
              │
              ▼
  Gold Dimensions (SCD2 MERGE)
              │
              ▼
  Gold Facts (point-in-time join to dim_product versions)
              │
              ▼
  Gold Business Aggregations (BQ1 to BQ6)
              │
              ▼
  Price-alert Structured Stream (availableNow) -> Serving export (Parquet for DWH/BI)

Every task is a PySpark job writing Delta Lake tables (MERGE-based, idempotent re-runs).
"""
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def execute_quality_gate(**context):
    """Run the 12 quality checks on the Silver Delta tables; raises to stop the DAG."""
    from dags.data_quality import run_silver_quality_gate_delta
    from spark.config import LAKEHOUSE_DIR, SILVER_PATH
    from spark.spark_session import get_spark_session

    spark = get_spark_session("airflow_quality_gate")
    try:
        summary = run_silver_quality_gate_delta(
            spark, SILVER_PATH, LAKEHOUSE_DIR / "silver_quality_report.json"
        )
    finally:
        spark.stop()
    print(f"Data Quality Gate PASSED: {summary['passed']}/{summary['total_checks']} checks.")


with DAG(
    dag_id="tiki_medallion_etl_pipeline",
    default_args=default_args,
    description="End-to-End Medallion Lakehouse ETL Pipeline with Quality Gate",
    schedule_interval="0 4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tiki", "lakehouse", "medallion", "etl", "quality_gate"],
) as dag:

    # Step 1: Bronze Ingestion
    bronze_ingest = BashOperator(
        task_id="bronze_ingestion",
        bash_command="python -m spark.jobs.ingest_to_bronze",
    )

    # Step 2: Silver Cleaning & Transformation (Parallel execution)
    silver_products = BashOperator(
        task_id="silver_transform_products",
        bash_command="python -m spark.jobs.bronze_to_silver_products",
    )

    silver_reviews = BashOperator(
        task_id="silver_transform_reviews_nlp",
        bash_command="python -m spark.jobs.bronze_to_silver_reviews",
    )

    # Step 3: Data Quality Gate (Validation barrier)
    quality_gate = PythonOperator(
        task_id="data_quality_gate",
        python_callable=execute_quality_gate,
        provide_context=True,
    )

    # Step 4: Gold Star Schema Generation (Parallel execution)
    gold_dimensions = BashOperator(
        task_id="gold_build_dimensions_scd2",
        bash_command="python -m spark.jobs.silver_to_gold_dimensions",
    )

    gold_facts = BashOperator(
        task_id="gold_build_facts",
        bash_command="python -m spark.jobs.silver_to_gold_facts",
    )

    # Step 5: Gold Business Aggregations (BQ1 to BQ6)
    gold_aggregations = BashOperator(
        task_id="gold_compute_business_aggregates",
        bash_command="python -m spark.jobs.silver_to_gold_aggregations",
    )

    # Step 6: Streaming Price Alerts Detection
    price_alerts = BashOperator(
        task_id="streaming_price_alerts_detection",
        bash_command="python -m spark.streaming.price_alert_stream --source files --trigger available-now",
    )

    # Step 7: Export Gold Delta tables to Parquet for the DWH loader / Power BI
    serving_export = BashOperator(
        task_id="serving_layer_export",
        bash_command="python -m spark.jobs.export_serving",
    )

    # Define DAG workflow dependencies
    bronze_ingest >> [silver_products, silver_reviews]
    [silver_products, silver_reviews] >> quality_gate
    quality_gate >> gold_dimensions >> gold_facts >> gold_aggregations
    gold_aggregations >> price_alerts >> serving_export

