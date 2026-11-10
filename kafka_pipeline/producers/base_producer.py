"""
Base Kafka Producer with Schema Validation and Resilient Delivery Callbacks.
"""
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import jsonschema
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger("base_producer")


class BaseProducer:
    """Base class for all Kafka event producers in the platform."""

    def __init__(
        self,
        topic: str,
        schema_path: Optional[Path] = None,
        bootstrap_servers: Optional[str] = None,
        dry_run: bool = False,
    ):
        self.topic = topic
        self.dry_run = dry_run
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.schema = self._load_schema(schema_path) if schema_path else None
        self.producer = None

        if not self.dry_run:
            self._connect()

    def _load_schema(self, path: Path) -> dict:
        """Load JSON schema from disk."""
        if not path.exists():
            raise FileNotFoundError(f"Schema file not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _connect(self) -> None:
        """Initialize KafkaProducer with delivery reliability settings."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
                acks="all",              # Wait for all in-sync replicas to acknowledge
                retries=3,               # Automatically retry failed sends
                max_in_flight_requests_per_connection=1,  # Guarantee ordering
                request_timeout_ms=15000,
            )
            logger.info("Connected to Kafka at %s (Topic: %s)", self.bootstrap_servers, self.topic)
        except Exception as err:
            logger.warning("Could not connect to Kafka (%s). Fallback to dry-run mode.", err)
            self.dry_run = True

    def validate_message(self, message: Dict[str, Any]) -> bool:
        """Validate payload against JSON schema."""
        if not self.schema:
            return True
        try:
            jsonschema.validate(instance=message, schema=self.schema)
            return True
        except jsonschema.ValidationError as err:
            logger.error("Schema validation failed: %s | Payload: %s", err.message, message)
            return False

    def send(self, key: Any, value: Dict[str, Any], callback: Optional[Callable] = None) -> bool:
        """Send message with schema validation and async delivery callback."""
        if not self.validate_message(value):
            return False

        if self.dry_run or not self.producer:
            logger.debug("[DRY RUN] Would send to %s (Key=%s): %s", self.topic, key, value)
            return True

        def default_callback(record_metadata):
            logger.debug("Message delivered to %s [partition %d] @ offset %d",
                         record_metadata.topic, record_metadata.partition, record_metadata.offset)

        def err_callback(exc):
            logger.error("Delivery failed for message (Key=%s): %s", key, exc)

        future = self.producer.send(
            self.topic,
            key=key,
            value=value,
        )
        future.add_callback(callback or default_callback)
        future.add_errback(err_callback)
        return True

    def flush(self) -> None:
        """Ensure all buffered messages are published."""
        if self.producer and not self.dry_run:
            self.producer.flush()

    def close(self) -> None:
        """Flush buffer and safely close Kafka producer."""
        if self.producer and not self.dry_run:
            self.flush()
            self.producer.close()
            logger.info("Closed producer for topic %s", self.topic)
