# tracker.py

from config import WATCH_ITEMS

FAIR_VALUES = {
    "elytra": 280_000_000,
    "netherite_block": 5_000_000,
    "dragon_head": 90_000_000,
    "enchanted_golden_apple": 3_500_000,
}

price_history = {}


def detect_volatility(item, price):
    history = price_history.setdefault(item, [])
    history.append(price)

    if len(history) > 10:
        history.pop(0)

    if len(history) < 5:
        return None

    avg = sum(history) / len(history)
    variance = sum((p - avg) ** 2 for p in history) / len(history)
    volatility = (variance ** 0.5) / avg

    if volatility > 0.12:
        return f"⚠️ HIGH VOLATILITY detected for {item}"

    return None


def analyze_market(orders):
    alerts = []
    market_data = {}

    # Aggregate orders
    for order in orders:
        item = order["item"]["itemId"]

        if item not in WATCH_ITEMS:
            continue

        price = order["itemPrice"]
        remaining = order["amountOrdered"] - order["amountDelivered"]

        if remaining <= 0:
            continue

        if item not in market_data:
            market_data[item] = {
                "total_value": 0,
                "total_volume": 0,
            }

        market_data[item]["total_value"] += price * remaining
        market_data[item]["total_volume"] += remaining

    # Compute VWPI + signals
    for item, data in market_data.items():
        vwpi = data["total_value"] / data["total_volume"]

        alerts.append(
            f"📊 {item} VWPI price: {int(vwpi):,}"
        )

        # volatility alert
        vol_alert = detect_volatility(item, vwpi)
        if vol_alert:
            alerts.append(vol_alert)

        # buy/sell signals
        fair = FAIR_VALUES.get(item)

        if fair:
            if vwpi < fair * 0.85:
                alerts.append(f"🟢 BUY SIGNAL: {item} undervalued")

            elif vwpi > fair * 1.25:
                alerts.append(f"🔴 SELL SIGNAL: {item} overvalued")

    return alerts