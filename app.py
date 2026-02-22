# app.py

import time
from fetcher import fetch_all_orders
from tracker import analyze_market
from alerts import send_alert

FAIR_VALUES = {
    "elytra": 280_000_000,
    "netherite_block": 5_000_000,
    "dragon_head": 90_000_000,
    "enchanted_golden_apple": 3_500_000,
}

WATCH_ITEMS = [
    "elytra",
    "netherite_ingot",
    "netherite_block",
    "dragon_head",
    "enchanted_golden_apple"
]


def main():
    print("Donut Market Tracker Started")

    while True:
        print("\nFetching market data...")

        orders = fetch_all_orders()

        results = analyze_market(orders, WATCH_ITEMS)

        for alert in results:
            send_alert(alert)
        
        fair = FAIR_VALUES.get(item)

        if fair and vwpi < fair * 0.85:
            send_alert(f"🟢 BUY SIGNAL: {item} undervalued")
        elif fair and vwpi > fair * 1.25:
            send_alert(f"🔴 SELL SIGNAL: {item} overvalued")

        print(f"Sleeping for {1800/60} minutes...")
        time.sleep(1800)


if __name__ == "__main__":
    main()

