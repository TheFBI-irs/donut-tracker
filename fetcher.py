"""
fetcher.py — Fetches auction listings and transactions from api.donutsmp.net

API endpoints:
  GET /v1/auction/list/{page}         — current sell listings (44 per page)
  GET /v1/auction/transactions/{page} — completed sales with timestamps

Rate limit: 250 req/min per API key
We use a small thread pool to fetch pages concurrently, staying under the limit.
"""

import os
import time
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

BASE_URL          = "https://api.donutsmp.net/v1"
MAX_WORKERS       = 2      # conservative — keeps us under 250/min rate limit
MAX_RETRIES       = 3
RETRY_DELAY       = 5
PAGE_SIZE         = 44
TRANSACTION_PAGES = 10     # how many transaction pages to fetch per cycle


def _get_headers() -> dict:
    key = os.getenv("DONUT_API_KEY")
    if not key:
        raise RuntimeError("DONUT_API_KEY environment variable not set.")
    return {"Authorization": f"Bearer {key}"}


def _fetch_page(endpoint: str, page: int) -> list:
    """Fetch a single page with retry logic. Returns list of results."""
    url = f"{BASE_URL}/{endpoint}/{page}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=_get_headers(), timeout=15)
            if r.status_code in (500, 404):
                return []  # past end of pages — don't retry
            if r.status_code == 429:
                logger.warning(f"Rate limited on {endpoint} page {page}, backing off 30s...")
                time.sleep(30)
                continue  # retry after backoff
            r.raise_for_status()
            data = r.json()
            if not data.get("result"):
                return []
            return data.get("result", [])
        except requests.exceptions.RequestException as e:
            logger.warning(f"{endpoint} page {page} attempt {attempt}/{MAX_RETRIES}: {e}")
            if attempt == MAX_RETRIES:
                return []
            time.sleep(RETRY_DELAY)
    return []


def _find_last_page(endpoint: str) -> int:
    """Binary search for the last valid page. Uses single-threaded requests."""
    lo, hi = 1, 5000
    while lo < hi:
        mid = (lo + hi + 1) // 2
        result = _fetch_page(endpoint, mid)
        if result:
            lo = mid
        else:
            hi = mid - 1
    return lo


def fetch_all_listings() -> list:
    """
    Fetch all current auction house listings using concurrent requests.
    Returns flat list of all listing dicts.
    """
    logger.info("Finding last listings page...")
    last_page = _find_last_page("auction/list")
    logger.info(f"Fetching {last_page} pages (~{last_page * PAGE_SIZE} listings)...")

    all_listings = []
    failed_pages = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_page, "auction/list", page): page
            for page in range(1, last_page + 1)
        }
        for future in as_completed(futures):
            page    = futures[future]
            results = future.result()
            if results:
                all_listings.extend(results)
            else:
                failed_pages.append(page)

    if failed_pages:
        logger.warning(f"Failed pages ({len(failed_pages)}): {sorted(failed_pages)[:10]}")

    logger.info(f"Fetched {len(all_listings)} total listings across {last_page} pages")
    return all_listings


def fetch_recent_transactions(pages: int = TRANSACTION_PAGES) -> list:
    """
    Fetch the most recent completed transactions.
    Returns flat list of transaction dicts sorted by time descending.
    """
    all_transactions = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_page, "auction/transactions", page): page
            for page in range(1, pages + 1)
        }
        for future in as_completed(futures):
            results = future.result()
            if results:
                all_transactions.extend(results)

    all_transactions.sort(key=lambda x: x.get("unixMillisDateSold", 0), reverse=True)
    logger.info(f"Fetched {len(all_transactions)} recent transactions")
    return all_transactions


def fetch_sample():
    """Print first 3 raw listings to inspect field semantics."""
    import json
    listings = _fetch_page("auction/list", 1)
    for listing in listings[:3]:
        print(json.dumps(listing, indent=2))