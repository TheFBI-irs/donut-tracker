"""
app.py — Main loop + Discord bot thread.

Startup sequence:
  1. init_db()                  — PostgreSQL tables (scans, snapshots, events)
  2. init_features_table()      — features table
  3. load_history_from_db()     — calibration from existing data
  4. start_bot_thread()         — interactive Discord bot
  5. scan loop                  — runs every 30 minutes
"""

import time
import logging
from datetime import datetime, timezone

from config import CHECK_INTERVAL, load_watchlist
from fetcher import fetch_all_listings, fetch_recent_transactions
from tracker import analyze_market, load_history_from_db
from macro import analyze_macro
from historian import init_db, record_scan
from features import init_features_table, extract_and_store, get_labeled_count, get_training_data_summary
from positions import report_positions
from digest import generate_digest
from alerts import send_alert
from bot import start_bot_thread

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("donut_bot.log"),
    ],
)
logger = logging.getLogger(__name__)

_last_digest_week: int | None = None
TRAINING_READY_THRESHOLD = 50000  # log a milestone when we hit this


def should_send_digest() -> bool:
    global _last_digest_week
    now  = datetime.now(timezone.utc)
    week = now.isocalendar()[1]
    if now.weekday() == 6 and week != _last_digest_week:
        _last_digest_week = week
        return True
    return False


def run_cycle():
    logger.info("Starting market scan cycle...")

    watch_items, fair_values = load_watchlist()
    if not watch_items:
        logger.warning("Watch list is empty — check watchlist.json")

    listings = fetch_all_listings()
    transactions = fetch_recent_transactions()
    if not listings:
        logger.warning("No orders returned — skipping cycle.")
        return

    # Layer 1: raw data to PostgreSQL
    scan_id = record_scan(listings, transactions)

    # Layer 2: ML feature extraction
    try:
        extract_and_store(scan_id)
        labeled = get_labeled_count()
        logger.info(f"Training set: {labeled:,} labeled examples")
        if labeled > 0 and labeled % 10000 < 50:
            summary = get_training_data_summary()
            logger.info(f"Training data summary: {summary}")
    except Exception as e:
        logger.warning(f"Feature extraction failed (non-critical): {e}")

    # Layer 3: watch-list alerts
    watch_alerts = analyze_market(listings, watch_items, fair_values)

    # Layer 4: positions P&L
    from tracker import price_history
    current_prices = {}
    for item in watch_items:
        records = price_history.get(item, [])
        if records and isinstance(records, list) and records[-1].get("bbp"):
            current_prices[item] = records[-1]["bbp"]

    position_alerts = report_positions(current_prices)

    # Layer 5: macro signals
    macro_alerts = analyze_macro(watch_items)

    all_alerts = watch_alerts + position_alerts + macro_alerts

    # Layer 6: weekly digest (Sundays)
    if should_send_digest():
        logger.info("Generating weekly digest...")
        all_alerts.extend(generate_digest(watch_items))

    for alert in all_alerts:
        send_alert(alert)

    logger.info(f"Cycle complete — {len(all_alerts)} alert(s) sent.")


def main():
    logger.info("Donut Market Tracker Started")

    init_db()
    init_features_table()

    watch_items, _ = load_watchlist()
    load_history_from_db(watch_items)

    start_bot_thread()

    while True:
        try:
            run_cycle()
        except Exception as e:
            logger.exception(f"Unexpected error in cycle: {e}")

        logger.info(f"Sleeping {CHECK_INTERVAL // 60} minutes...")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()