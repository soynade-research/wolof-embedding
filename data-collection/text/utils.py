import requests
from bs4 import BeautifulSoup
from typing import Optional, List
import time
from datasets import Dataset
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_html(
    url: str, session: requests.Session, delay: int, max_retries: int, timeout: int
) -> Optional[BeautifulSoup]:
    for attempt in range(max_retries):
        try:
            time.sleep(delay)
            response = session.get(url, timeout=timeout)
            response.raise_for_status()

            if "text/html" not in response.headers.get("Content-Type", ""):
                logger.warning(f"Non-HTML content at {url}")
                return None
            return BeautifulSoup(response.text, "html.parser")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt+1} failed for {url}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(delay * 2**attempt)
            else:
                logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                return None


def create_and_push_dataset(
        data: list, dataset_name: str, organization: Optional[str] = None
    ) -> None:
        try:
            dataset = Dataset.from_list(data)
            repo_name = (
                f"{organization}/{dataset_name}" if organization else dataset_name
            )
            dataset.push_to_hub(repo_name, token=os.environ['HF_TOKEN'])
            logger.info(f"Dataset pushed to {repo_name}")
        except Exception as e:
            logger.error(f"Dataset creation failed: {str(e)}")
            raise