"""
app.py — Main loop.

Each cycle:
  1. Fetch all orders from API
  2. historian.record_scan()  → writes every item to SQLite (macro data layer)
  3. tracker.analyze_market() → watch-list alerts with self-calibrating demand
  4. macro.analyze_macro()    → economy-wide signals (unlocks after 20 scans)
  5. Send all alerts to Discord
"""

import time
import logging
from config import WATCH_ITEMS
from fetcher import fetch_all_orders
from tracker import analyze_market
from macro import analyze_macro
from historian import init_db, record_scan
from alerts import send_alert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("donut_bot.log"),
    ],
)
logger = logging.getLogger(__name__)

CHECK_INTERVAL = 1800  # 30 minutes


def run_cycle():
    logger.info("Starting market scan cycle...")

    orders = fetch_all_orders()
    if not orders:
        logger.warning("No orders returned — skipping cycle.")
        return

    # Layer 1: record everything to SQLite
    record_scan(orders)

    # Layer 2: watch-list alerts
    watch_alerts = analyze_market(orders)

    # Layer 3: macro economy signals
    macro_alerts = analyze_macro(WATCH_ITEMS)

    all_alerts = watch_alerts + macro_alerts

    for alert in all_alerts:
        send_alert(alert)

    logger.info(f"Cycle complete — {len(all_alerts)} alert(s) sent.")


def main():
    logger.info("Donut Market Tracker Started")
    init_db()   # create tables if first run, no-op otherwise

    while True:
        try:
            run_cycle()
        except Exception as e:
            logger.exception(f"Unexpected error in cycle: {e}")

        logger.info(f"Sleeping {CHECK_INTERVAL // 60} minutes...")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()