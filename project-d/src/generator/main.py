import csv
import signal
import sys
import time
from datetime import datetime

from confluent_kafka import Producer
from src import config
from src.logger import get_logger
from src.models import SensorReading

logger = get_logger("SensorGenerator")


class SensorGenerator:
    """
    Simulates an IoT sensor network by streaming dataset rows to Kafka.
    """

    def __init__(self):
        self.running = True
        self.rooms_mapping = {
            "Sub_metering_1": "kitchen",
            "Sub_metering_2": "laundry_room",
            "Sub_metering_3": "water_heater_ac"
        }

        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        logger.info(f"Initializing Kafka producer connected to {config.KAFKA_BOOTSTRAP_SERVERS}...")
        try:
            self.producer = Producer({
                "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
                "client.id": "sensor-generator"
            })
        except Exception as error:
            logger.critical(f"Failed to initialize Kafka producer: {error}")
            sys.exit(1)

    def shutdown(self, sig, frame) -> None:
        logger.info("Termination signal received. Initiating graceful shutdown...")
        self.running = False

    @staticmethod
    def delivery_report(err, msg) -> None:
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def run(self) -> None:
        logger.info(f"Starting to read from {config.DATA_FILE} and streaming to '{config.RAW_TOPIC}'...")
        messages_sent = 0

        try:
            with open(config.DATA_FILE, "r") as file:
                reader = csv.DictReader(file, delimiter=";")

                for row in reader:
                    if not self.running:
                        break

                    if "?" in row.values():
                        continue

                    try:
                        dt_str = f"{row['Date']} {row['Time']}"
                        dt_obj = datetime.strptime(dt_str, "%d/%m/%Y %H:%M:%S")
                        formatted_datetime = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError as ve:
                        logger.warning(f"Date parsing error for row: {ve}. Skipping.")
                        continue

                    for meter, room_name in self.rooms_mapping.items():
                        reading = SensorReading(
                            datetime=formatted_datetime,
                            room=room_name,
                            consumption_level=float(row[meter])
                        )

                        while True:
                            try:
                                self.producer.produce(
                                    topic=config.RAW_TOPIC,
                                    value=reading.to_json(),
                                    callback=self.delivery_report
                                )
                                break

                            except BufferError:
                                logger.debug("Producer queue full. Waiting for deliveries to clear space...")
                                self.producer.poll(0.5)

                        messages_sent += 1

                    self.producer.poll(0)

                    if messages_sent % 300 == 0:
                        logger.info(f"Successfully streamed {messages_sent} messages so far...")

        except FileNotFoundError:
            logger.critical(f"Data file not found at {config.DATA_FILE}. Check docker volume mounts.")
            sys.exit(1)
        except Exception as error:
            logger.error(f"Unexpected error during data generation: {error}")
        finally:
            logger.info("Flushing remaining messages to Kafka. Please wait...")
            self.producer.flush()
            logger.info("Generator shutdown complete.")


if __name__ == "__main__":
    generator = SensorGenerator()
    generator.run()
