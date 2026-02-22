"""
tracker.py — Market intelligence for Donut SMP

KEY INSIGHT:
  /orders returns BUY ORDERS (bids) only — not sell listings.
  Players post "I will pay X for item Y." When someone delivers, they get paid.

  VWPI (volume-weighted average) is useless here because troll orders
  (e.g., 1 coin for an elytra) drag the average to near zero.

  CORRECT MODEL:
    - Take the top N highest bids per item ("best bids").
    - Average them → Best Bid Price (BBP).
    - Track BBP over time.
    - High bids = real demand that will actually get filled.
    - Low bids = noise/trolls, ignored entirely.
"""

import json
import os
import logging
from datetime import datetime
from config import WATCH_ITEMS

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TOP_N_BIDS = 5          # Number of highest bids to average for price signal
MIN_BID_FLOOR = 1000    # Ignore bids below this — troll/dust filter
HISTORY_FILE = "price_history.json"
MAX_HISTORY = 48        # ~24 hours of 30-min samples

FAIR_VALUES = {
    "elytra": 280_000_000,
    "netherite_ingot": 2_500_000,
    "netherite_block": 5_000_000,
    "dragon_head": 90_000_000,
    "enchanted_golden_apple": 3_500_000,
}

# ---------------------------------------------------------------------------
# Persistent history
# Each item stores a list of:
#   { "ts": ISO string, "bbp": float, "top_bids": [int, ...], "demand": int }
# ---------------------------------------------------------------------------

def load_history() -> dict:
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r") as f:
                data = json.load(f)
                logger.info(f"Loaded price history ({HISTORY_FILE})")
                return data
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Could not load history: {e}. Starting fresh.")
    return {}

def save_history(history: dict):
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=2)
    except OSError as e:
        logger.error(f"Failed to save price history: {e}")

price_history: dict = load_history()

# ---------------------------------------------------------------------------
# Core price signal: Best Bid Price (BBP)
# ---------------------------------------------------------------------------

def compute_bbp(bids: list) -> float | None:
    """
    Average the top N bids above the troll floor.
    Returns None if there aren't enough real bids.
    """
    real_bids = sorted(
        [b for b in bids if b >= MIN_BID_FLOOR],
        reverse=True
    )
    if not real_bids:
        return None
    top = real_bids[:TOP_N_BIDS]
    return sum(top) / len(top)

# ---------------------------------------------------------------------------
# Signal detectors
# ---------------------------------------------------------------------------

def detect_trend(item: str, current_bbp: float) -> str | None:
    records = price_history.get(item, [])
    prices = [r["bbp"] for r in records if r.get("bbp")]

    if len(prices) < 10:
        return None

    short_ma = sum(prices[-3:]) / 3
    long_ma = sum(prices[-10:]) / 10
    delta_pct = (short_ma - long_ma) / long_ma

    if abs(delta_pct) < 0.03:
        return None

    if short_ma < long_ma:
        return (
            f"📉 BEARISH TREND: **{item}** "
            f"(3-sample MA {int(short_ma):,} < 10-sample MA {int(long_ma):,}, Δ {delta_pct:.1%})"
        )
    else:
        return (
            f"📈 BULLISH TREND: **{item}** "
            f"(3-sample MA {int(short_ma):,} > 10-sample MA {int(long_ma):,}, Δ +{delta_pct:.1%})"
        )


def detect_volatility(item: str) -> str | None:
    """
    Volatility = are top bids moving a lot between scans?
    Uses % change in BBP between the last two samples.
    """
    records = price_history.get(item, [])
    prices = [r["bbp"] for r in records if r.get("bbp")]

    if len(prices) < 2:
        return None

    prev = prices[-2]
    curr = prices[-1]
    if prev == 0:
        return None

    change_pct = (curr - prev) / prev

    if abs(change_pct) >= 0.05:
        direction = "▲" if change_pct > 0 else "▼"
        return (
            f"⚠️ VOLATILITY: **{item}** top bids moved "
            f"{direction}{abs(change_pct):.1%} since last scan"
        )
    return None


def detect_crash_risk(item: str, current_demand: int, current_bbp: float) -> str | None:
    """
    Crash signal: top bids dropping AND demand volume rising
    (people posting more low bids, high bids disappearing = panic/supply dump incoming).
    """
    records = price_history.get(item, [])
    if len(records) < 3:
        return None

    recent_demands = [r["demand"] for r in records[-3:] if r.get("demand") is not None]
    recent_prices = [r["bbp"] for r in records[-3:] if r.get("bbp")]

    if not recent_demands or not recent_prices:
        return None

    avg_demand = sum(recent_demands) / len(recent_demands)
    avg_price = sum(recent_prices) / len(recent_prices)

    demand_spiked = avg_demand > 0 and (current_demand / avg_demand) > 1.5
    price_falling = current_bbp < avg_price * 0.95

    if demand_spiked and price_falling:
        demand_ratio = current_demand / avg_demand
        price_drop = (1 - current_bbp / avg_price)
        return (
            f"🚨 MARKET RISK: **{item}** — demand orders up ×{demand_ratio:.1f} "
            f"while top bids fell {price_drop:.1%}. Possible supply dump / crash incoming."
        )
    elif demand_spiked:
        return (
            f"⚠️ DEMAND SPIKE: **{item}** — order volume up ×{current_demand/avg_demand:.1f}. Monitor closely."
        )
    return None

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def analyze_market(orders: list) -> list:
    alerts = []

    # Group all bids per item
    item_bids = {}
    item_demand = {}  # total unfilled units demanded

    for order in orders:
        item = order["item"]["itemId"]
        if item not in WATCH_ITEMS:
            continue

        price = order["itemPrice"]
        remaining = order["amountOrdered"] - order["amountDelivered"]
        if remaining <= 0:
            continue

        item_bids.setdefault(item, []).append(price)
        item_demand[item] = item_demand.get(item, 0) + remaining

    now = datetime.utcnow().isoformat()

    for item in WATCH_ITEMS:
        bids = item_bids.get(item, [])
        demand = item_demand.get(item, 0)

        bbp = compute_bbp(bids)

        if bbp is None:
            alerts.append(f"❓ **{item}** — no meaningful bids found this scan")
            continue

        # Top bids for display
        real_bids = sorted([b for b in bids if b >= MIN_BID_FLOOR], reverse=True)
        top_display = [f"{b:,}" for b in real_bids[:3]]

        alerts.append(
            f"📊 **{item}** | Best Bid: {int(bbp):,} | "
            f"Top 3: [{', '.join(top_display)}] | "
            f"Demand: {demand:,} units"
        )

        # Update persistent history BEFORE running detectors so
        # detect_volatility and detect_trend see the current sample
        records = price_history.setdefault(item, [])
        records.append({
            "ts": now,
            "bbp": bbp,
            "top_bids": real_bids[:TOP_N_BIDS],
            "demand": demand,
        })
        if len(records) > MAX_HISTORY:
            records.pop(0)

        # Signals ordered: crash > volatility > trend > fair value
        crash = detect_crash_risk(item, demand, bbp)
        if crash:
            alerts.append(crash)

        vol = detect_volatility(item)
        if vol:
            alerts.append(vol)

        trend = detect_trend(item, bbp)
        if trend:
            alerts.append(trend)

        fair = FAIR_VALUES.get(item)
        if fair:
            ratio = bbp / fair
            if ratio < 0.85:
                alerts.append(
                    f"🟢 STRONG DEMAND: **{item}** top bids at {ratio:.0%} of fair value "
                    f"({int(bbp):,} vs {fair:,}) — buyers paying below fair, good time to source and fill"
                )
            elif ratio > 1.15:
                alerts.append(
                    f"🔴 PREMIUM DEMAND: **{item}** top bids at {ratio:.0%} of fair value "
                    f"({int(bbp):,} vs {fair:,}) — buyers paying above fair, good time to sell into orders"
                )

    save_history(price_history)

    if not alerts:
        alerts.append("✅ Market scan complete — no signals detected.")

    return alerts