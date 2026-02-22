# app.py

import time
from fetcher import fetch_all_orders
from tracker import analyze_market
from alerts import send_alert

def main():
    print("Donut Market Tracker Started")

    while True:
        print("\nFetching market data...")
        orders = fetch_all_orders()
        results = analyze_market(orders)

        for alert in results:
            send_alert(alert)

        print(f"Sleeping for {1800/60} minutes...")
        time.sleep(1800)

if __name__ == "__main__":
    main()