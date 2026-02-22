"""
app.py — Main loop + Discord bot thread.

Startup sequence:
  1. init_db()                  — create PostgreSQL tables if needed
  2. load_history_from_db()     — populate calibration from existing data
  3. start_bot_thread()         — interactive Discord bot
  4. scan loop                  — runs every 30 minutes forever
"""

import time
import logging
from datetime import datetime, timezone

from config import CHECK_INTERVAL, load_watchlist
from fetcher import fetch_all_orders
from tracker import analyze_market, load_history_from_db
from macro import analyze_macro
from historian import init_db, record_scan
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

    orders = fetch_all_orders()
    if not orders:
        logger.warning("No orders returned — skipping cycle.")
        return

    record_scan(orders)

    watch_alerts = analyze_market(orders, watch_items, fair_values)

    from tracker import price_history
    current_prices = {}
    for item in watch_items:
        records = price_history.get(item, [])
        if records and isinstance(records, list) and records[-1].get("bbp"):
            current_prices[item] = records[-1]["bbp"]

    position_alerts = report_positions(current_prices)
    macro_alerts    = analyze_macro(watch_items)
    all_alerts      = watch_alerts + position_alerts + macro_alerts

    if should_send_digest():
        logger.info("Generating weekly digest...")
        all_alerts.extend(generate_digest(watch_items))

    for alert in all_alerts:
        send_alert(alert)

    logger.info(f"Cycle complete — {len(all_alerts)} alert(s) sent.")


def main():
    logger.info("Donut Market Tracker Started")

    # Step 1: ensure tables exist
    init_db()

    # Step 2: load calibration history from PostgreSQL
    # This means calibration survives redeployments — no more starting over
    watch_items, _ = load_watchlist()
    load_history_from_db(watch_items)

    # Step 3: start interactive Discord bot in background thread
    start_bot_thread()

    # Step 4: main scan loop
    while True:
        try:
            run_cycle()
        except Exception as e:
            logger.exception(f"Unexpected error in cycle: {e}")

        logger.info(f"Sleeping {CHECK_INTERVAL // 60} minutes...")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()