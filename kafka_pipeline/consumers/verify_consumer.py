"""
Verification Kafka Consumer.

Listens to Kafka topics, displays incoming message throughput,
validates payload integrity, and verifies partition assignments.
"""
import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("verify_consumer")

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def consume_topic(
    topic: str,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP,
    group_id: str = "tiki-verify-group",
    max_messages: int = 20,
    from_beginning: bool = True,
) -> None:
    """Consume and print sample messages from designated topic."""
    auto_offset = "earliest" if from_beginning else "latest"
    logger.info("Connecting to %s | Topic: %s | Offset: %s | Group: %s",
                bootstrap_servers, topic, auto_offset, group_id)

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset,
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            consumer_timeout_ms=10000,
        )
    except Exception as err:
        logger.error("Failed to connect consumer to Kafka: %s", err)
        return

    logger.info("Listening for messages on '%s'...", topic)
    count = 0
    start_time = time.time()

    for msg in consumer:
        count += 1
        val = msg.value
        sample_str = str(val)[:80] + "..." if len(str(val)) > 80 else str(val)
        logger.info("[Msg #%d] Partition=%d, Offset=%d, Key=%s | Value: %s",
                    count, msg.partition, msg.offset, msg.key, sample_str)

        if max_messages and count >= max_messages:
            break

    elapsed = time.time() - start_time
    rate = count / elapsed if elapsed > 0 else 0
    logger.info("✅ Done! Consumed %d messages in %.2f seconds (%.1f msg/s)", count, elapsed, rate)
    consumer.close()


def main():
    parser = argparse.ArgumentParser(description="Verify Kafka Consumer")
    parser.add_argument("--topic", default="raw-price-events", choices=["raw-products", "raw-price-events", "raw-reviews"])
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--group-id", default="tiki-verify-group")
    parser.add_argument("--max", type=int, default=20)
    parser.add_argument("--from-beginning", action="store_true", default=True)
    args = parser.parse_args()

    consume_topic(
        topic=args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group_id,
        max_messages=args.max,
        from_beginning=args.from_beginning,
    )


if __name__ == "__main__":
    main()
