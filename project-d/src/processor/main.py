import signal
import sys
from collections import deque

import pandas as pd
from confluent_kafka import Consumer, Producer
from pydantic import ValidationError
from src import config
from src.logger import get_logger
from src.models import AnomalyAlert, SensorReading

logger = get_logger("AnomalyProcessor")


class AnomalyProcessor:
    """
    Consumes sensor data, detects anomalies using a rolling IQR and alerts.
    """

    def __init__(self):
        self.running = True

        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        logger.info(f"Connecting to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS}...")
        try:
            self.consumer = Consumer({
                "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": config.PROCESSOR_GROUP_ID,
                "auto.offset.reset": "earliest"
            })
            self.producer = Producer({
                "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS
            })
        except Exception as error:
            logger.critical(f"Kafka connection failed: {error}")
            sys.exit(1)

        self.room_windows = {
            "kitchen": deque(maxlen=config.WINDOW_SIZE),
            "laundry_room": deque(maxlen=config.WINDOW_SIZE),
            "water_heater_ac": deque(maxlen=config.WINDOW_SIZE)
        }

    def shutdown(self, sig, frame) -> None:
        logger.info("Termination signal received. Shutting down processor...")
        self.running = False

    @staticmethod
    def delivery_report(err, msg) -> None:
        if err is not None:
            logger.error(f"Failed to deliver anomaly message: {err}")
        else:
            logger.info(f"Anomaly alert sent to {msg.topic()}: {msg.value().decode('utf-8')}")

    def run(self) -> None:
        self.consumer.subscribe([config.RAW_TOPIC])
        logger.info(f"Subscribed to '{config.RAW_TOPIC}'. IQR Window: {config.WINDOW_SIZE}, Sigma: {config.SIGMA}")

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    reading = SensorReading.from_json(msg.value())
                except ValidationError as error:
                    logger.error(f"Malformed message skipped (Validation Error): {error}")
                    continue
                except Exception as error:
                    logger.error(f"Unexpected error parsing message: {error}")
                    continue

                window = self.room_windows[reading.room]

                if len(window) == config.WINDOW_SIZE:
                    series = pd.Series(window)
                    q1 = series.quantile(0.25)
                    q3 = series.quantile(0.75)
                    iqr = q3 - q1

                    upper_bound = q3 + (config.SIGMA * iqr)

                    if reading.consumption_level > upper_bound > 0.0:
                        alert = AnomalyAlert(
                            room=reading.room,
                            datetime=reading.datetime,
                            level=reading.consumption_level,
                            threshold_exceeded=upper_bound
                        )

                        self.producer.produce(
                            topic=config.ANOMALY_TOPIC,
                            value=alert.to_json(),
                            callback=self.delivery_report
                        )
                        self.producer.poll(0)

                window.append(reading.consumption_level)

        except KeyboardInterrupt:
            pass
        finally:
            logger.info("Closing consumer and flushing producer...")
            self.consumer.close()
            self.producer.flush()
            logger.info("Processor shutdown complete.")


if __name__ == "__main__":
    processor = AnomalyProcessor()
    processor.run()
