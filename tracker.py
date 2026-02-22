import json
import os
import logging
from datetime import datetime
from config import WATCH_ITEMS

logger = logging.getLogger(__name__)

FAIR_VALUES = {
    "elytra": 280_000_000,
    "netherite_block": 5_000_000,
    "dragon_head": 90_000_000,
    "enchanted_golden_apple": 3_500_000,
}

HISTORY_FILE = "price_history.json"

# --- Persistent history ---

def load_history() -> dict:
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r") as f:
                data = json.load(f)
                logger.info(f"Loaded price history from {HISTORY_FILE}")
                return data
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Could not load history file: {e}. Starting fresh.")
    return {}

def save_history(history: dict):
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=2)
    except OSError as e:
        logger.error(f"Failed to save price history: {e}")

# In-memory history loaded once at module init
# Each key: item name
# Each value: list of {"ts": ISO timestamp, "vwpi": float, "supply": int}
price_history: dict = load_history()

MAX_HISTORY = 20  # keep last 20 samples per item

# --- Volatility ---

def detect_volatility(item: str, vwpi: float) -> str | None:
    records = price_history.get(item, [])
    prices = [r["vwpi"] for r in records]

    if len(prices) < 5:
        return None

    avg = sum(prices) / len(prices)
    variance = sum((p - avg) ** 2 for p in prices) / len(prices)
    volatility = (variance ** 0.5) / avg

    if volatility > 0.12:
        return f"⚠️ HIGH VOLATILITY: **{item}** (σ/μ = {volatility:.1%})"
    return None

# --- Trend detection ---

def detect_trend(item: str) -> str | None:
    records = price_history.get(item, [])
    prices = [r["vwpi"] for r in records]

    if len(prices) < 10:
        return None

    short_ma = sum(prices[-3:]) / 3
    long_ma = sum(prices[-10:]) / 10

    delta_pct = (short_ma - long_ma) / long_ma

    # Only signal if the divergence is meaningful (>3%)
    if abs(delta_pct) < 0.03:
        return None

    if short_ma < long_ma:
        return (
            f"📉 BEARISH TREND: **{item}** "
            f"(3-MA {int(short_ma):,} < 10-MA {int(long_ma):,}, Δ {delta_pct:.1%})"
        )
    else:
        return (
            f"📈 BULLISH TREND: **{item}** "
            f"(3-MA {int(short_ma):,} > 10-MA {int(long_ma):,}, Δ +{delta_pct:.1%})"
        )

# --- Crash / supply shock detector ---

def detect_crash_risk(item: str, current_supply: int, current_vwpi: float) -> str | None:
    records = price_history.get(item, [])

    if len(records) < 3:
        return None

    recent_supplies = [r["supply"] for r in records[-3:]]
    avg_recent_supply = sum(recent_supplies) / len(recent_supplies)

    if avg_recent_supply == 0:
        return None

    supply_ratio = current_supply / avg_recent_supply

    # Supply spike: >50% above recent average
    supply_spiked = supply_ratio > 1.5

    # Price falling: current VWPI below 3-sample average
    recent_prices = [r["vwpi"] for r in records[-3:]]
    avg_recent_price = sum(recent_prices) / len(recent_prices)
    price_falling = current_vwpi < avg_recent_price * 0.97

    if supply_spiked and price_falling:
        return (
            f"🚨 MARKET RISK: **{item}** supply surge detected — possible crash! "
            f"(Supply ×{supply_ratio:.1f} vs recent avg, VWPI down {(1 - current_vwpi/avg_recent_price):.1%})"
        )
    elif supply_spiked:
        return (
            f"⚠️ SUPPLY SPIKE: **{item}** listings up ×{supply_ratio:.1f} vs recent avg — monitor closely"
        )

    return None

# --- Core analysis ---

def analyze_market(orders: list) -> list[str]:
    alerts = []
    market_data: dict = {}

    for order in orders:
        item = order["item"]["itemId"]

        if item not in WATCH_ITEMS:
            continue

        price = order["itemPrice"]
        remaining = order["amountOrdered"] - order["amountDelivered"]

        if remaining <= 0:
            continue

        if item not in market_data:
            market_data[item] = {"total_value": 0, "total_volume": 0}

        market_data[item]["total_value"] += price * remaining
        market_data[item]["total_volume"] += remaining

    now = datetime.utcnow().isoformat()

    for item, data in market_data.items():
        if data["total_volume"] == 0:
            continue

        vwpi = data["total_value"] / data["total_volume"]
        supply = data["total_volume"]

        # --- Update persistent history ---
        records = price_history.setdefault(item, [])
        records.append({"ts": now, "vwpi": vwpi, "supply": supply})
        if len(records) > MAX_HISTORY:
            records.pop(0)

        # --- Always emit a price summary ---
        alerts.append(
            f"📊 **{item}** | VWPI: {int(vwpi):,} | Supply: {supply:,} units"
        )

        # --- Signals ---
        crash_alert = detect_crash_risk(item, supply, vwpi)
        if crash_alert:
            alerts.append(crash_alert)

        trend_alert = detect_trend(item)
        if trend_alert:
            alerts.append(trend_alert)

        vol_alert = detect_volatility(item, vwpi)
        if vol_alert:
            alerts.append(vol_alert)

        fair = FAIR_VALUES.get(item)
        if fair:
            if vwpi < fair * 0.85:
                discount = (1 - vwpi / fair)
                alerts.append(
                    f"🟢 BUY SIGNAL: **{item}** undervalued by {discount:.1%} vs fair value {fair:,}"
                )
            elif vwpi > fair * 1.25:
                premium = (vwpi / fair - 1)
                alerts.append(
                    f"🔴 SELL SIGNAL: **{item}** overvalued by +{premium:.1%} vs fair value {fair:,}"
                )

    # Persist updated history after all processing
    save_history(price_history)

    if not alerts:
        alerts.append("✅ Market scan complete — no signals detected.")

    return alerts