"""
End-to-end latency benchmark for the price-alert stream.

1. Starts the Structured Streaming job in continuous mode (2-second micro-batches,
   file source) as a subprocess.
2. After Spark has started, drops a new file of 15 price events every 3 seconds
   (about 1/3 forced flash sales), using real crawled products.
3. When the stream stops, reads gold/price_alerts and reports, for the alerts of
   those events only: count vs expected, p50 / p95 / max of
   latency_sec = alert write time - event time.

Usage: python -m spark.streaming.measure_latency [--files 12] [--interval 2]
"""
import argparse
import json
import random
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from data_collection.config import RAW_DATA_DIR  # noqa: E402
from data_collection.simulate_price_events import generate_price_event, load_sample_products  # noqa: E402
from spark.config import GOLD_PATH, table_path  # noqa: E402
from spark.streaming.price_alert_stream import EVENTS_DIR  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", type=int, default=12)
    parser.add_argument("--events-per-file", type=int, default=15)
    parser.add_argument("--interval", type=int, default=2, help="micro-batch interval (s)")
    parser.add_argument("--startup-wait", type=int, default=35)
    args = parser.parse_args()

    run_seconds = args.startup_wait + args.files * 3 + 20
    stream = subprocess.Popen(
        [sys.executable, "-m", "spark.streaming.price_alert_stream", "--source", "files",
         "--trigger", "continuous", "--interval", str(args.interval), "--run-seconds", str(run_seconds)],
        cwd=ROOT,
    )
    time.sleep(args.startup_wait)

    products = load_sample_products(RAW_DATA_DIR / "products_raw.jsonl")
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    expected = []
    for _ in range(args.files):
        events = [generate_price_event(random.choice(products), force_flash_sale=(j % 3 == 0))
                  for j in range(args.events_per_file)]
        expected += [e["event_id"] for e in events if e["is_flash_sale"] or e["price_change_pct"] <= -30]
        name = f"bench_{int(time.time() * 1000)}.jsonl"
        tmp = EVENTS_DIR / f".{name}"
        tmp.write_text("\n".join(json.dumps(e, ensure_ascii=False) for e in events) + "\n", encoding="utf-8")
        tmp.replace(EVENTS_DIR / name)
        time.sleep(3)
    stream.wait()

    from pyspark.sql import functions as F
    from spark.spark_session import get_spark_session

    spark = get_spark_session("latency_report")
    try:
        alerts = spark.read.format("delta").load(table_path(GOLD_PATH, "price_alerts"))
        r = alerts.filter(F.col("event_id").isin(expected)).agg(
            F.count("*").alias("alerts"),
            F.round(F.expr("percentile_approx(latency_sec, 0.5)"), 2).alias("p50_sec"),
            F.round(F.expr("percentile_approx(latency_sec, 0.95)"), 2).alias("p95_sec"),
            F.round(F.max("latency_sec"), 2).alias("max_sec"),
        ).first().asDict()
        r["expected_alerts"] = len(expected)
        print("LATENCY RESULT:", json.dumps(r))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
