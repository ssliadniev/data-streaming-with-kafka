import json
import logging
import os
import signal
import time
from collections import Counter
from typing import Any
from urllib.parse import urlparse

from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DomainStatsConsumer:

    def __init__(self, bootstrap_servers: str, topic: str, group_id: str, report_dir: str):
        self.topic = topic
        self.report_dir = report_dir
        self.domain_counts: Counter[str] = Counter()
        self.running = True

        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest"
        })
        self.consumer.subscribe([self.topic])

        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum: Any, frame: Any) -> None:
        logger.info("Shutdown signal received. Initiating clean shutdown...")
        self.running = False

    def _process_message(self, message_payload: str) -> None:
        try:
            data = json.loads(message_payload)
            url = data.get("url", "")

            if not url:
                return

            netloc = urlparse(url).netloc
            parts = netloc.split(".")

            if len(parts) > 1:
                root_domain = parts[-1].lower()
                self.domain_counts[root_domain] += 1

                self._display_statistics()
                self._save_statistics()

        except json.JSONDecodeError:
            logger.error("Failed to decode message payload as JSON.")
        except Exception as error:
            logger.error(f"Unexpected error processing message: {error}")

    def _display_statistics(self) -> None:
        top_5 = self.domain_counts.most_common(5)

        print("\n====== Top 5 Root Domains ======")
        for rank, (domain, count) in enumerate(top_5, 1):
            print(f"{rank}. {domain.upper()}: {count} visits")
        print("================================")

    def _save_statistics(self) -> None:
        top_5 = self.domain_counts.most_common(5)

        stats_dict = {
            "top_domains": [{"domain": domain, "visits": count} for domain, count in top_5]
        }

        try:
            os.makedirs(self.report_dir, exist_ok=True)
            report_path = os.path.join(self.report_dir, "top_domains.json")

            with open(report_path, "w", encoding="utf-8") as file:
                json.dump(stats_dict, file, indent=4)

        except IOError as error:
            logger.error(f"Failed to write statistics to {self.report_dir}: {error}")

    def consume_data(self) -> None:
        logger.info(f"Consumer started. Listening to topic '{self.topic}'...")

        try:
            while self.running:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    error_code = msg.error().code()

                    if error_code == KafkaError._PARTITION_EOF:
                        continue
                    elif error_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        logger.warning(f"Topic '{self.topic}' not ready yet. Waiting for producer...")
                        time.sleep(2.0)
                        continue
                    else:
                        logger.error(f"Critical consumer error: {msg.error()}")
                        break

                self._process_message(msg.value().decode("utf-8"))
        finally:
            self.consumer.close()
            logger.info("Consumer connection closed safely.")


if __name__ == "__main__":
    BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
    TOPIC = os.getenv("KAFKA_TOPIC", "browser-history")
    GROUP = os.getenv("CONSUMER_GROUP", "domain-stats-group")
    REPORT_DIR = os.getenv("REPORT_DIR", "/app/report")

    consumer_app = DomainStatsConsumer(
        bootstrap_servers=BROKER,
        topic=TOPIC,
        group_id=GROUP,
        report_dir=REPORT_DIR
    )
    consumer_app.consume_data()
