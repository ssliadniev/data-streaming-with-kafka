import argparse
import json
import logging
import sys
import time
from typing import Any, Optional

import pandas as pd
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TWEET_COLUMN_INDEX = 5


def delivery_report(err: Optional[Any], msg: Any) -> None:
    """
    Callback triggered upon message delivery success or failure.
    """

    if err is not None:
        logger.error(f"Message delivery failed: {err}")


def load_dataset(file_path: str, max_rows: int) -> pd.DataFrame:
    try:
        return pd.read_csv(file_path, encoding="latin-1", header=None, nrows=max_rows)
    except FileNotFoundError:
        logger.error(f"Dataset not found at '{file_path}'. Please ensure it is in the data/ folder.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to load dataset: {e}")
        sys.exit(1)


def format_message(row: pd.Series) -> bytes:
    """
    Extracts tweet text from a dataset row and formats it into a JSON payload.
    """

    tweet_text = str(row[TWEET_COLUMN_INDEX])

    message_data = {
        "tweet": tweet_text,
        "send_time": time.time(),
        "message_size_bytes": len(tweet_text.encode("utf-8"))
    }

    return json.dumps(message_data).encode("utf-8")


def produce_messages(producer: Producer, topic: str, df: pd.DataFrame) -> None:
    """
    Iterates through the DataFrame and publishes messages to the Kafka topic.
    """

    logger.info(f"Sending {len(df)} messages to topic '{topic}'...")

    for _, row in df.iterrows():
        payload = format_message(row)

        producer.produce(
            topic,
            value=payload,
            callback=delivery_report
        )
        producer.poll(0)

    producer.flush()
    logger.info("All messages sent successfully!")


def run_generator(topic: str, dataset_path: str, max_msgs: int, broker: str) -> None:
    logger.info(f"Initializing Kafka producer connecting to {broker}...")
    producer = Producer({"bootstrap.servers": broker})

    logger.info(f"Loading dataset from {dataset_path}...")
    df = load_dataset(dataset_path, max_msgs)

    produce_messages(producer, topic, df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Data Generator (Producer)")
    parser.add_argument("--topic", type=str, default="twitter-test", help="Target Kafka topic")
    parser.add_argument("--dataset", type=str, default="data/twitter_dataset.csv", help="Path to CSV dataset")
    parser.add_argument("--max", type=int, default=100, help="Max messages to send")
    parser.add_argument("--broker", type=str, default="redpanda-0:29092", help="Kafka broker address")
    args = parser.parse_args()

    run_generator(args.topic, args.dataset, args.max, args.broker)
