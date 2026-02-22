import time
import logging
import requests
from config import DISCORD_WEBHOOK

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


def send_alert(message: str):
    """Send a message to Discord via webhook with retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                DISCORD_WEBHOOK,
                json={"content": message},
                timeout=10,
            )
            response.raise_for_status()
            logger.debug(f"Alert sent: {message[:80]}...")
            return
        except requests.exceptions.RequestException as e:
            logger.warning(f"Alert send attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    logger.error(f"Failed to send alert after {MAX_RETRIES} attempts: {message[:80]}")