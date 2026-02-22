import time
import logging
import requests
from config import API_URL

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 10  # seconds between retries on failure


def fetch_all_orders() -> list:
    """Fetch all orders via cursor-based pagination with retry logic."""
    all_orders = []
    cursor = ""
    page = 0

    while True:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.get(
                    API_URL,
                    params={"cursor": cursor} if cursor else {},
                    timeout=15,
                )
                response.raise_for_status()
                data = response.json()
                break  # success — exit retry loop
            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"Page {page} fetch attempt {attempt}/{MAX_RETRIES} failed: {e}"
                )
                if attempt == MAX_RETRIES:
                    logger.error("Max retries reached. Returning partial data.")
                    return all_orders
                time.sleep(RETRY_DELAY)

        orders = data.get("orders", [])
        all_orders.extend(orders)
        page += 1
        logger.debug(f"Fetched page {page} ({len(orders)} orders)")

        cursor = data.get("nextCursor")
        if not cursor:
            break

    logger.info(f"Fetched {len(all_orders)} total orders across {page} page(s)")
    return all_orders