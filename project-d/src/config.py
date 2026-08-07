import os

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RAW_TOPIC = os.environ.get("RAW_TOPIC", "sensor-raw-data")
ANOMALY_TOPIC = os.environ.get("ANOMALY_TOPIC", "sensor-anomalies")

DATA_FILE = os.environ.get("DATA_FILE", "/app/dataset/household_power_consumption.txt")

PROCESSOR_GROUP_ID = os.environ.get("PROCESSOR_GROUP_ID", "anomaly-detector-group")
WINDOW_SIZE = int(os.environ.get("WINDOW_SIZE", "100"))
SIGMA = float(os.environ.get("SIGMA", "1.5"))

DISPLAY_GROUP_ID = os.environ.get("DISPLAY_GROUP_ID", "display-group")
