import glob
import logging
import os
import shutil
from typing import Optional

import kagglehub

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

KAGGLE_DATASET_ID = "kazanova/sentiment140"
DESTINATION_DIR = "data"
DESTINATION_FILE = "twitter_dataset.csv"


def download_dataset(dataset_id: str) -> str:
    """
    Downloads a dataset via kagglehub and returns the local cache path.
    """

    logger.info(f"Downloading dataset '{dataset_id}' via kagglehub...")

    cache_path = kagglehub.dataset_download(dataset_id)
    logger.info(f"Dataset successfully cached at: {cache_path}")

    return cache_path


def find_csv_in_cache(cache_path: str) -> Optional[str]:
    csv_files = glob.glob(os.path.join(cache_path, "*.csv"))

    if not csv_files:
        return None

    return csv_files[0]


def copy_to_project(source_file: str, dest_dir: str, dest_filename: str) -> None:
    os.makedirs(dest_dir, exist_ok=True)
    destination_path = os.path.join(dest_dir, dest_filename)

    shutil.copy(source_file, destination_path)
    logger.info(f"Success! Dataset copied to your project at: {destination_path}")


def setup_dataset() -> None:
    try:
        cache_path = download_dataset(KAGGLE_DATASET_ID)
        source_csv = find_csv_in_cache(cache_path)

        if source_csv:
            copy_to_project(source_csv, DESTINATION_DIR, DESTINATION_FILE)
        else:
            logger.error("Could not find a CSV file in the downloaded dataset.")

    except Exception as error:
        logger.error(f"An error occurred during dataset setup: {error}")


if __name__ == "__main__":
    setup_dataset()
