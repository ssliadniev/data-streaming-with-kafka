# E2E data processing pipeline - processing sensor streams (IoT)

## Project overview
This project implements an E2E data processing pipeline for IoT sensor streams using Apache Kafka. 
The system simulates a network of household sensors, processes the streams in real-time to detect anomalous power 
consumption peaks using a dynamic statistical threshold and alerts downstream consumers.

**Topic:** E2E data processing pipeline - processing sensor streams (IoT)  
**Goal:** Learn about the implementation of E2E data processing pipelines using Kafka.

---

## Architecture & microservices
The pipeline consists of a containerized Apache Kafka (KRaft mode) cluster and three distinct python microservices:

1. **Generator (`src/generator/main.py`)**: Acts as the IoT sensor simulator. 
     It reads the dataset row by row, maps sub-meters to specific rooms (`kitchen`, `laundry_room`, `water_heater_ac`), 
     and produces structured JSON messages to the `sensor-raw-data` Kafka topic.
2. **Processor (`src/processor/main.py`)**: The streaming topology engine. It consumes raw sensor data, maintains a 
     rolling window of recent readings (default: 100) and applies the **Interquartile Range (IQR)** outlier detection 
     method. If a reading exceeds the dynamic upper bound ($Q_3 + (\Sigma \times IQR)$), an alert is sent to the 
     `sensor-anomalies` topic.
3. **Display (`src/display/main.py`)**: A consumer service that listens to the `sensor-anomalies` topic and formats 
     the detected peaks for real-time console monitoring.

---

## Prerequisites
* Docker
* Docker Compose

---

## How to run the implementation

**Step 1: Prepare the dataset**  
Ensure the dataset is located at exactly `data/household_power_consumption.txt`.

**Step 2: Environment configuration**  
Create your local environment file by copying the provided example template:
```bash
cp .env.example .env
```

(Optional) You can modify the .env file to tweak parameters like SIGMA (anomaly sensitivity) or WINDOW_SIZE (rolling average length).

**Step 3: Build and start the pipeline**  
Use Docker Compose to build the python images and spin up the Kafka broker alongside the three microservices:
```bash
docker compose up --build -d
```

**Step 4: View the anomaly alerts**  
To view the output of the pipeline and monitor for detected consumption peaks, attach to the logs of the display container:
```bash
docker compose logs -f display
```

---

## Configuration parameters (.env)
The pipeline's behavior can be dynamically adjusted without altering the source code:

* DELAY_SECONDS: The simulation speed of the generator.
* WINDOW_SIZE: The number of historical messages per room kept in memory to calculate the IQR baseline.
* SIGMA: The configurable anomaly threshold multiplier. Higher values reduce sensitivity; lower values increase alerts.
