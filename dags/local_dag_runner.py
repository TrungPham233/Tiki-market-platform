"""
Local runner for the 'tiki_medallion_etl_pipeline' DAG.

Runs the same task functions as the Airflow DAG, in the same order, inside one
local SparkSession - useful when Airflow/Docker is not running.
"""
import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from spark.run_medallion_pipeline import run_pipeline


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Local Airflow DAG Runner")
    parser.add_argument("--skip-stream", action="store_true")
    args = parser.parse_args()
    run_pipeline(skip_stream=args.skip_stream)
    sys.exit(0)
