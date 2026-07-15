import argparse
import csv
import json
import logging
import os
import time
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

LOG_DIR = "logs"
POLL_TIMEOUT_SEC = 1.0
SIMULATED_PROCESSING_DELAY_SEC = 1.0


def setup_log_file(consumer_id: str) -> str:
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = f"{LOG_DIR}/consumer_{consumer_id}_logs.csv"

    with open(log_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["send_time", "finish_time", "message_size_bytes"])

    return log_file


def init_consumer(broker: str, group_id: str, topic: str) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([topic])
    return consumer


def handle_message(msg: Message, writer: Any, consumer_id: str) -> None:
    data = json.loads(msg.value().decode("utf-8"))

    time.sleep(SIMULATED_PROCESSING_DELAY_SEC)
    finish_time = time.time()

    writer.writerow([data["send_time"], finish_time, data["message_size_bytes"]])

    latency = finish_time - data["send_time"]
    logger.info(f"[{consumer_id}] Processed message (Latency: {latency:.2f}s)")


def consume_loop(consumer: Consumer, topic: str, consumer_id: str, log_file: str) -> None:
    """
    Main polling loop that continuously listens for and processes messages.
    """

    logger.info(f"Consumer {consumer_id} listening to topic '{topic}'...")

    try:
        with open(log_file, mode="a", newline="") as file:
            writer = csv.writer(file)

            while True:
                msg = consumer.poll(POLL_TIMEOUT_SEC)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        break

                handle_message(msg, writer, consumer_id)

                file.flush()

    except KeyboardInterrupt:
        logger.info(f"Closing consumer {consumer_id} gracefully...")
    finally:
        consumer.close()


def run_processor(topic: str, group_id: str, broker: str, consumer_id: str) -> None:
    log_file = setup_log_file(consumer_id)
    consumer = init_consumer(broker, group_id, topic)

    consume_loop(consumer, topic, consumer_id, log_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Data Processor (Consumer)")
    parser.add_argument("--topic", type=str, default="twitter-test", help="Kafka topic to consume")
    parser.add_argument("--group", type=str, default="twitter-consumer-group", help="Consumer group ID")
    parser.add_argument("--broker", type=str, default="redpanda-0:29092", help="Kafka broker address")
    parser.add_argument("--id", type=str, default=str(uuid.uuid4())[:8], help="Unique ID for this consumer")
    args = parser.parse_args()

    run_processor(args.topic, args.group, args.broker, args.id)
