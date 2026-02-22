import requests
from config import API_URL

def fetch_all_orders():
    all_orders = []
    cursor = ""

    while True:
        try:
            response = requests.get(
                API_URL,
                params={"cursor": cursor},
                timeout=15
            )
            response.raise_for_status()
        except requests.RequestException as e:
            print("API request failed:", e)
            break

        data = response.json()

        orders = data.get("orders", [])
        all_orders.extend(orders)

        cursor = data.get("nextCursor")

        if not cursor:
            break

    print(f"Fetched {len(all_orders)} total orders")
    return all_orders