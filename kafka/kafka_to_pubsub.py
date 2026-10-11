"""
Kafka → Google Cloud Pub/Sub bridge.

Consumes messages from the Kafka raw-events topic and republishes
them to Pub/Sub, acting as the entry point into the GCP pipeline.
At-least-once delivery: Kafka offsets are committed only after Pub/Sub ACKs;
duplicates are removed downstream by event_id in the dbt staging model.

Usage:
    python kafka_to_pubsub.py
"""
from __future__ import annotations
import json, logging, os, signal, sys
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Consumer, KafkaError
from google.cloud import pubsub_v1
from schemas import KAFKA_TOPICS

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("kafka_to_pubsub")

BOOTSTRAP      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GROUP_ID       = os.getenv("KAFKA_GROUP_ID", "kafka-pubsub-bridge")
PROJECT_ID     = os.environ["GCP_PROJECT_ID"]
PUBSUB_TOPIC   = os.getenv("PUBSUB_TOPIC", "raw-events")
BATCH_SIZE     = int(os.getenv("BRIDGE_BATCH_SIZE", "100"))

_running = True


def _shutdown(sig, frame):
    global _running
    logger.info("Shutdown signal received.")
    _running = False


def run() -> None:
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    consumer = Consumer({
        "bootstrap.servers":  BOOTSTRAP,
        "group.id":           GROUP_ID,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300_000,
    })
    consumer.subscribe([KAFKA_TOPICS["raw_events"]])

    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)

    futures_batch: list = []
    msgs_batch:    list = []
    forwarded = 0

    logger.info("Bridge started: Kafka[%s] → Pub/Sub[%s]",
                KAFKA_TOPICS["raw_events"], topic_path)

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka error: %s", msg.error())
                continue

            future = publisher.publish(
                topic_path,
                data=msg.value(),
                kafka_offset=str(msg.offset()),
                kafka_partition=str(msg.partition()),
            )
            futures_batch.append(future)
            msgs_batch.append(msg)

            if len(futures_batch) >= BATCH_SIZE:
                _flush_batch(futures_batch, msgs_batch, consumer)
                forwarded += len(msgs_batch)
                logger.info("Forwarded %d messages total", forwarded)
                futures_batch, msgs_batch = [], []

    finally:
        if futures_batch:
            _flush_batch(futures_batch, msgs_batch, consumer)
        consumer.close()
        logger.info("Bridge stopped. Total forwarded: %d", forwarded)


def _flush_batch(futures, msgs, consumer) -> None:
    """Wait for all Pub/Sub publishes, then commit Kafka offsets."""
    errors = 0
    for fut, msg in zip(futures, msgs):
        try:
            fut.result(timeout=10)
        except Exception as e:
            logger.error("Pub/Sub publish failed for offset %d: %s", msg.offset(), e)
            errors += 1
    if errors == 0:
        consumer.commit(asynchronous=False)
    else:
        logger.warning("%d messages failed — offsets NOT committed.", errors)


if __name__ == "__main__":
    run()
