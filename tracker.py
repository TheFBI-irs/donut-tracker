from collections import Counter
from storage import load_prices, save_prices
from volatility import crash_risk


def filter_item(orders, item_name):
    return [
        o for o in orders
        if o["item"]["itemId"] == item_name
    ]


# ---------------- PRICE TRACKING ----------------

def extract_market_price(orders):
    if not orders:
        return None

    prices = [o["itemPrice"] for o in orders]
    return min(prices)  # lowest listing = market price


def update_price_history(item, price):
    data = load_prices()

    if item not in data:
        data[item] = []

    data[item].append(price)

    data[item] = data[item][-500:]  # keep history bounded

    save_prices(data)

    return data[item]


# ---------------- SIGNALS ----------------

def detect_listing_flood(orders):
    return len(orders) >= 12


def detect_whale_activity(orders):
    counter = Counter()

    for o in orders:
        if o["amountOrdered"] >= 10:
            counter[o["userName"]] += o["amountOrdered"]

    whales = [u for u, amt in counter.items() if amt >= 20]
    return whales


# ---------------- MASTER ANALYSIS ----------------

def analyze_market(all_orders, item):
    item_orders = filter_item(all_orders, item)

    signals = []

    price = extract_market_price(item_orders)

    if price:
        history = update_price_history(item, price)

        if crash_risk(history):
            signals.append("⚠️ HIGH CRASH RISK (volatility expansion)")

    if detect_listing_flood(item_orders):
        signals.append("📉 Listing flood detected")

    whales = detect_whale_activity(item_orders)
    if whales:
        signals.append(
            "🐋 Whale accumulation: " + ", ".join(whales)
        )

    return signals