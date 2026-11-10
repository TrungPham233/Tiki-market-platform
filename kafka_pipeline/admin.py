"""
Kafka Admin Utility: Topic Creation, Listing, and Deletion.
"""
import argparse
import logging
import os
import sys
from pathlib import Path

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("kafka_admin")

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TARGET_TOPICS = [
    {"name": "raw-products", "partitions": 3, "replication_factor": 1},
    {"name": "raw-price-events", "partitions": 3, "replication_factor": 1},
    {"name": "raw-reviews", "partitions": 3, "replication_factor": 1},
]


def get_admin_client(bootstrap_servers: str = DEFAULT_BOOTSTRAP) -> KafkaAdminClient:
    """Connect to Kafka cluster as admin."""
    return KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="tiki-admin-client",
        request_timeout_ms=10000,
    )


def create_topics(bootstrap_servers: str = DEFAULT_BOOTSTRAP) -> None:
    """Ensure all required topics exist with designated partitions."""
    try:
        client = get_admin_client(bootstrap_servers)
    except Exception as err:
        logger.error("Failed to connect to Kafka at %s: %s", bootstrap_servers, err)
        return

    existing = client.list_topics()
    logger.info("Existing Kafka topics: %s", existing)

    new_topics = []
    for t in TARGET_TOPICS:
        if t["name"] not in existing:
            new_topics.append(
                NewTopic(
                    name=t["name"],
                    num_partitions=t["partitions"],
                    replication_factor=t["replication_factor"],
                )
            )

    if new_topics:
        try:
            client.create_topics(new_topics=new_topics, validate_only=False)
            logger.info("Successfully created topics: %s", [t.name for t in new_topics])
        except TopicAlreadyExistsError:
            pass
        except Exception as err:
            logger.error("Error creating topics: %s", err)
    else:
        logger.info("All target topics already exist.")

    client.close()


def list_topics(bootstrap_servers: str = DEFAULT_BOOTSTRAP) -> None:
    """Print all topic names and metadata."""
    client = get_admin_client(bootstrap_servers)
    topics = client.list_topics()
    logger.info("Topics in cluster (%s):", bootstrap_servers)
    for top in sorted(topics):
        logger.info("  - %s", top)
    client.close()


def main():
    parser = argparse.ArgumentParser(description="Kafka Topic Management")
    parser.add_argument("--action", choices=["create", "list"], default="create")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    args = parser.parse_args()

    if args.action == "create":
        create_topics(args.bootstrap)
    elif args.action == "list":
        list_topics(args.bootstrap)


if __name__ == "__main__":
    main()
