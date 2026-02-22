"""
app.py — Main loop.

Each cycle:
  1. Load watch list + fair values from watchlist.json (dynamic, no redeploy)
  2. Fetch all orders from API
  3. historian.record_scan()  → writes every item to SQLite
  4. tracker.analyze_market() → watch-list alerts, self-calibrating demand
  5. positions.report()       → P&L on your open positions
  6. macro.analyze_macro()    → economy-wide signals
  7. digest (Sundays only)    → weekly summary from historian DB
  8. Send all alerts to Discord
"""

import time
import logging
from datetime import datetime, timezone

from config import DISCORD_WEBHOOK, CHECK_INTERVAL, load_watchlist
from fetcher import fetch_all_orders
from tracker import analyze_market
from macro import analyze_macro
from historian import init_db, record_scan
from positions import report_positions
from digest import generate_digest
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

_last_digest_week: int | None = None


def should_send_digest() -> bool:
    """Send digest once per week on Sunday."""
    global _last_digest_week
    now  = datetime.now(timezone.utc)
    week = now.isocalendar()[1]
    if now.weekday() == 6 and week != _last_digest_week:  # 6 = Sunday
        _last_digest_week = week
        return True
    return False


def run_cycle():
    logger.info("Starting market scan cycle...")

    # Reload watch list every cycle — picks up edits without redeployment
    watch_items, fair_values = load_watchlist()
    if not watch_items:
        logger.warning("Watch list is empty — check watchlist.json")

    orders = fetch_all_orders()
    if not orders:
        logger.warning("No orders returned — skipping cycle.")
        return

    # Layer 1: record everything to SQLite
    record_scan(orders)

    # Layer 2: watch-list signals
    watch_alerts = analyze_market(orders, watch_items, fair_values)

    # Layer 3: current BBP map for position P&L
    current_prices = {}
    for alert in watch_alerts:
        pass  # BBP is already computed inside tracker; extract from history
    # Pull BBP directly from the most recent history records
    from tracker import price_history
    for item in watch_items:
        records = price_history.get(item, [])
        if records and isinstance(records, list) and records[-1].get("bbp"):
            current_prices[item] = records[-1]["bbp"]

    position_alerts = report_positions(current_prices)

    # Layer 4: macro signals
    macro_alerts = analyze_macro(watch_items)

    all_alerts = watch_alerts + position_alerts + macro_alerts

    # Layer 5: weekly digest (Sundays)
    if should_send_digest():
        logger.info("Generating weekly digest...")
        digest_messages = generate_digest(watch_items)
        all_alerts.extend(digest_messages)

    for alert in all_alerts:
        send_alert(alert)

    logger.info(f"Cycle complete — {len(all_alerts)} alert(s) sent.")


def main():
    logger.info("Donut Market Tracker Started")
    init_db()

    while True:
        try:
            run_cycle()
        except Exception as e:
            logger.exception(f"Unexpected error in cycle: {e}")

        logger.info(f"Sleeping {CHECK_INTERVAL // 60} minutes...")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()