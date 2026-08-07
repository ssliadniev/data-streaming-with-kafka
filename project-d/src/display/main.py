import signal
import sys

from confluent_kafka import Consumer
from pydantic import ValidationError
from src import config
from src.logger import get_logger
from src.models import AnomalyAlert

logger = get_logger("AnomalyDisplay")


class AnomalyDisplay:
    """
    Consumes anomaly alerts and prints them cleanly to the console.
    """

    def __init__(self):
        self.running = True

        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        logger.info(f"Connecting to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS}...")
        try:
            self.consumer = Consumer({
                "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": config.DISPLAY_GROUP_ID,
                "auto.offset.reset": "earliest"
            })
        except Exception as error:
            logger.critical(f"Failed to initialize Kafka consumer: {error}")
            sys.exit(1)

    def shutdown(self, sig, frame) -> None:
        logger.info("Termination signal received. Shutting down display service...")
        self.running = False

    def run(self) -> None:
        self.consumer.subscribe([config.ANOMALY_TOPIC])

        logger.info("=" * 85)
        logger.info(f" 🚨 LISTENING FOR ANOMALIES ON '{config.ANOMALY_TOPIC}' 🚨 ")
        logger.info("=" * 85)

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    logger.error(f"Kafka consumer error: {msg.error()}")
                    continue

                try:
                    alert = AnomalyAlert.from_json(msg.value())
                except ValidationError as error:
                    logger.error(f"Malformed alert skipped (Validation Error): {error}")
                    continue
                except Exception as error:
                    logger.error(f"Unexpected error parsing alert: {error}")
                    continue

                room_formatted = alert.room.replace('_', ' ').upper()

                logger.info(
                    f"[!] ANOMALY | Room: {room_formatted:<15} | "
                    f"Time: {alert.datetime} | "
                    f"Level: {alert.level:>6.2f} (Threshold: {alert.threshold_exceeded:>6.2f})"
                )

        except KeyboardInterrupt:
            pass
        finally:
            logger.info("Closing Kafka consumer...")
            self.consumer.close()
            logger.info("Display service shutdown complete.")


if __name__ == "__main__":
    display_service = AnomalyDisplay()
    display_service.run()
