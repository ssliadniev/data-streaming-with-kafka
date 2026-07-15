## 🏗️ System architecture

The project consists of three primary python microservices, all containerized via docker:

1. **Generator (`src/generator.py`)**: Acts as the Kafka producer. It reads a Twitter dataset (Sentiment140), packages each tweet with a `send_time` timestamp, and publishes it to the cluster.
2. **Processor (`src/processor.py`)**: Acts as the Kafka consumer. It subscribes to the topic, reads the message, simulates a heavy processing workload by sleeping for 1 second, calculates the `finish_time`, and flushes the metrics to a unique CSV log file.
3. **Aggregator (`src/aggregator.py`)**: Parses all consumer logs after an experiment, calculates the system's maximum latency and throughput (in Mbps), and appends the results to a summary file.

---

## 📁 Project structure

```text
homework-2/
├── data/                       # Contains the downloaded dataset
├── logs/                       # Consumer logs, summary CSV, and generated graphs
├── src/
│   ├── setup_data.py           # Downloads Sentiment140 via Kagglehub
│   ├── generator.py            # Producer microservice
│   ├── processor.py            # Consumer microservice
│   ├── aggregator.py           # Calculates latency and throughput metrics
│   └── plot_results.py         # Generates matplotlib performance graphs
├── docker-compose.yml          # Redpanda cluster and python app definitions
├── Dockerfile                  # Python runtime environment
├── Makefile                    # Automation for setup, execution, and plotting
├── requirements.txt            # Python dependencies
└── README.md
```

---

## ⚙️ Prerequisites
To run this project, you must have the following installed on your machine:

Docker and Docker Compose

Make (Native on Linux/macOS; use WSL or Git Bash on Windows)

---

## 🚀 Quick start & Execution guide
The entire project is automated using Make. Follow these steps to reproduce the experiments.

### Step 1: Initial setup
Spin up the Redpanda cluster, build the python docker images, and download the dataset automatically.

```bash
make setup
```

### Step 2: Run the experiments
The project tests 8 specific cluster configurations as required by the assignment. Run the following commands sequentially. **Wait for each experiment to finish completely before starting the next.**

| Experiment | Command | Producers | Partitions | Consumers |
| :--- | :--- | :--- | :--- | :--- |
| **h** | `make exp-h` | 1 | 1 | 1 |
| **i** | `make exp-i` | 1 | 1 | 2 |
| **j** | `make exp-j` | 1 | 2 | 2 |
| **k** | `make exp-k` | 1 | 5 | 5 |
| **l** | `make exp-l` | 1 | 10 | 1 |
| **m** | `make exp-m` | 1 | 10 | 5 |
| **n** | `make exp-n` | 1 | 10 | 10 |
| **o** | `make exp-o` | 2 | 10 | 10 |

### Step 3: Generate the performance report
Once all experiments are complete, generate the visual performance graphs:
```bash
make plot
```

### Step 4: Cleanup
When you are completely finished with your homework, tear down the environment to free up system resources:
```bash
make clean
```
