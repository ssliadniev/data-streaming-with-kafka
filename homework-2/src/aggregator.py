import argparse
import glob
import logging
import os

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

LOG_FILES_PATTERN = "logs/consumer_*_logs.csv"
SUMMARY_FILE_PATH = "report/experiment_summary.csv"


def load_consumer_data(file_pattern: str) -> Optional[pd.DataFrame]:
    """
    Loads and concatenates all consumer CSV logs into a single DataFrame.
    """

    log_files = glob.glob(file_pattern)

    if not log_files:
        logger.warning("No consumer log files found.")
        return None

    df_list = [pd.read_csv(file) for file in log_files]
    df = pd.concat(df_list, ignore_index=True)

    if df.empty:
        logger.warning("Log files are empty.")
        return None

    return df


def calculate_metrics(df: pd.DataFrame) -> Tuple[float, float]:
    """
    Calculates max latency and throughput in Mbps from the raw data.
    """

    df["latency"] = df["finish_time"] - df["send_time"]
    max_latency = df["latency"].max()

    total_bytes = df["message_size_bytes"].sum()
    total_bits = total_bytes * 8

    min_send_time = df["send_time"].min()
    max_finish_time = df["finish_time"].max()
    total_time_seconds = max_finish_time - min_send_time

    if total_time_seconds > 0:
        throughput_mbps = (total_bits / 1_000_000) / total_time_seconds
    else:
        throughput_mbps = 0.0

    return max_latency, throughput_mbps


def print_performance_report(exp_name: str, max_latency: float, throughput_mbps: float) -> None:
    logger.info(f"=== {exp_name} PERFORMANCE REPORT ===")
    logger.info(f"Max latency: {max_latency:.2f} seconds")
    logger.info(f"System throughput: {throughput_mbps:.6f} Mbps")
    logger.info("=================================")


def save_summary_result(exp_name: str, max_latency: float, throughput_mbps: float, file_path: str) -> None:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    file_exists = os.path.isfile(file_path)

    with open(file_path, mode="a") as file:
        if not file_exists:
            file.write("Config,Throughput,Max_Latency\n")

        file.write(f"{exp_name},{throughput_mbps},{max_latency}\n")


def generate_report(exp_name: str) -> None:
    df = load_consumer_data(LOG_FILES_PATTERN)

    if df is None:
        return

    max_latency, throughput_mbps = calculate_metrics(df)

    print_performance_report(exp_name, max_latency, throughput_mbps)
    save_summary_result(exp_name, max_latency, throughput_mbps, SUMMARY_FILE_PATH)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aggregates Kafka consumer logs and calculates performance metrics.")
    parser.add_argument("--name", type=str, default="Custom Run", help="Name of the experiment")
    args = parser.parse_args()

    generate_report(args.name)
