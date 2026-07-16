import csv
import json
import logging
import os
import time
from typing import Any, Optional

from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class BrowserHistoryProducer:

    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})

    def _delivery_callback(self, err: Optional[Any], msg: Any) -> None:
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def stream_csv_data(self, file_path: str, delay_seconds: float = 0.0) -> None:
        """
        Reads the CSV dataset and streams rows to Kafka as JSON messages.

        Args:
            file_path: The path to the CSV file containing the dataset.
            delay_seconds: Artificial delay between messages to simulate real-time streaming.
        """

        logger.info(f"Starting to stream data from '{file_path}' to topic '{self.topic}'...")

        if not os.path.exists(file_path):
            logger.error(f"Dataset file not found at '{file_path}'. Please ensure it exists.")
            return

        try:
            with open(file_path, mode="r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    payload = json.dumps(row).encode("utf-8")

                    self.producer.produce(
                        self.topic,
                        value=payload,
                        callback=self._delivery_callback
                    )
                    self.producer.poll(0)

                    if delay_seconds > 0:
                        time.sleep(delay_seconds)

            logger.info("Flushing outstanding messages to Kafka...")
            self.producer.flush()
            logger.info("Finished streaming all dataset records successfully.")

        except Exception as error:
            logger.error(f"An unexpected error occurred while streaming data: {error}")


if __name__ == "__main__":
    BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
    TOPIC = os.getenv("KAFKA_TOPIC", "browser-history")
    FILE_PATH = os.getenv("CSV_FILE_PATH", "data/history.csv")

    STREAM_DELAY = float(os.getenv("STREAM_DELAY_SECONDS", "0.0"))

    startup_delay = 5
    logger.info(f"Waiting {startup_delay} seconds for Kafka broker to be ready...")
    time.sleep(startup_delay)

    producer_app = BrowserHistoryProducer(bootstrap_servers=BROKER, topic=TOPIC)
    producer_app.stream_csv_data(file_path=FILE_PATH, delay_seconds=STREAM_DELAY)
