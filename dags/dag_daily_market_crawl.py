"""
Airflow DAG: Daily E-Commerce Market Crawler & Kafka Ingestion.

Schedule: 02:00 AM UTC Daily.
Orchestrates:
1. crawl_product_ids: Scrapes active product IDs across price partitions.
2. crawl_product_details: Fetches deep specifications, prices, sellers, and brands.
3. crawl_reviews: Fetches customer feedback and ratings.
4. simulate_price_events: Generates real-time price fluctuation events.
5. publish_to_kafka: Publishes payloads into Kafka topics.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="tiki_daily_market_crawl",
    default_args=default_args,
    description="Daily ingestion pipeline: crawl Tiki products/reviews and stream to Kafka",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tiki", "crawler", "kafka", "ingestion"],
) as dag:

    # Task 1: Crawl product IDs
    crawl_ids = BashOperator(
        task_id="crawl_product_ids",
        bash_command="python -m data_collection.crawl_product_ids --category-id 1882",
    )

    # Task 2: Crawl product details
    crawl_details = BashOperator(
        task_id="crawl_product_details",
        bash_command="python -m data_collection.crawl_product_detail",
    )

    # Task 3: Crawl customer reviews
    crawl_reviews = BashOperator(
        task_id="crawl_reviews",
        bash_command="python -m data_collection.crawl_reviews --max-reviews-per-product 50",
    )

    # Task 4: Simulate real-time price fluctuations for streaming
    simulate_events = BashOperator(
        task_id="simulate_price_events",
        bash_command="python -m data_collection.simulate_price_events --count 100 --rate 10",
    )

    # Task 5: Publish crawled data to Kafka topics (dry-run fallback if cluster offline)
    publish_products = BashOperator(
        task_id="publish_products_to_kafka",
        bash_command="python -m kafka_pipeline.producers.product_producer",
    )

    publish_reviews = BashOperator(
        task_id="publish_reviews_to_kafka",
        bash_command="python -m kafka_pipeline.producers.review_producer",
    )

    # Dependency graph
    crawl_ids >> crawl_details >> crawl_reviews >> simulate_events
    simulate_events >> [publish_products, publish_reviews]
