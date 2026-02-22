import time
import logging
from fetcher import fetch_all_orders
from tracker import analyze_market
from alerts import send_alert

# --- Logging setup ---
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
        logger.warning("No orders returned — skipping analysis.")
        return

    results = analyze_market(orders)

    for alert in results:
        send_alert(alert)

    logger.info(f"Cycle complete. {len(results)} alert(s) sent.")


def main():
    logger.info("Donut Market Tracker Started")

    while True:
        try:
            run_cycle()
        except Exception as e:
            logger.exception(f"Unexpected error in cycle: {e}")

        logger.info(f"Sleeping {CHECK_INTERVAL // 60} minutes until next scan...")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()