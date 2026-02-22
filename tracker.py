"""
tracker.py — Watch-list alerts with self-calibrating demand pressure.

Demand pressure is relative to each item's own historical baseline,
not fixed global thresholds. The system learns what "normal" looks like
per item and signals deviations from that — no assumptions required.
"""

import json
import os
import logging
from datetime import datetime
from config import WATCH_ITEMS

logger = logging.getLogger(__name__)

TOP_N_BIDS       = 10
MIN_BID_FLOOR    = 1000
HISTORY_FILE     = "price_history.json"
MAX_HISTORY      = 48       # ~24 hours at 30-min intervals
CALIBRATION_MIN  = 10       # scans needed before demand labels mean anything
VOLATILITY_THRESHOLD = 0.05

FAIR_VALUES = {
    "elytra":                  280_000_000,
    "netherite_ingot":           4_300_000,
    "netherite_block":          38_000_000,
    "dragon_head":              26_000_000,
    "enchanted_golden_apple":      620_000,
}

# ---------------------------------------------------------------------------
# Persistent history (JSON, watch-list items only)
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
# Core metrics
# ---------------------------------------------------------------------------

def compute_top_bids(bids: list) -> list:
    return sorted([b for b in bids if b >= MIN_BID_FLOOR], reverse=True)[:TOP_N_BIDS]

def compute_bbp(top_bids: list) -> float | None:
    if not top_bids:
        return None
    return sum(top_bids) / len(top_bids)

def compute_gap_pct(top_bids: list) -> float | None:
    if len(top_bids) < 2:
        return None
    gaps = [top_bids[i] - top_bids[i + 1] for i in range(len(top_bids) - 1)]
    return (sum(gaps) / len(gaps)) / top_bids[0]

# ---------------------------------------------------------------------------
# Self-calibrating demand label
# Compares current gap% to item's own historical baseline.
# No fixed thresholds — the item defines its own "normal."
# ---------------------------------------------------------------------------

def describe_demand(item: str, gap_pct: float) -> str:
    records = price_history.get(item, [])
    historical_gaps = [r["gap_pct"] for r in records if r.get("gap_pct") is not None]

    n = len(historical_gaps)
    if n < CALIBRATION_MIN:
        return f"📊 CALIBRATING ({n}/{CALIBRATION_MIN} scans, gap {gap_pct*100:.2f}%)"

    avg  = sum(historical_gaps) / n
    variance = sum((g - avg) ** 2 for g in historical_gaps) / n
    std  = variance ** 0.5

    # z-score: how many standard deviations from item's own mean?
    z = (gap_pct - avg) / std if std > 0 else 0

    if z < -1.5:
        return f"🔥 HIGH (gap {gap_pct*100:.2f}% vs baseline {avg*100:.2f}%)"
    elif z > 1.5:
        return f"🧊 LOW  (gap {gap_pct*100:.2f}% vs baseline {avg*100:.2f}%)"
    else:
        return f"🟡 MEDIUM (gap {gap_pct*100:.2f}% vs baseline {avg*100:.2f}%)"

# ---------------------------------------------------------------------------
# Signal detectors
# ---------------------------------------------------------------------------

def detect_demand_shift(item: str) -> str | None:
    records = price_history.get(item, [])
    gaps = [r["gap_pct"] for r in records if r.get("gap_pct") is not None]
    if len(gaps) < 4:
        return None

    prev_avg = sum(gaps[-4:-1]) / 3
    curr = gaps[-1]
    if prev_avg == 0:
        return None

    change = (curr - prev_avg) / prev_avg
    if change > 0.50:
        return f"📉 DEMAND WEAKENING: **{item}** — bid gaps widened {change:.0%} vs recent avg"
    elif change < -0.50:
        return f"📈 DEMAND BUILDING: **{item}** — bid gaps tightened {abs(change):.0%} vs recent avg"
    return None


def detect_volatility(item: str) -> str | None:
    records = price_history.get(item, [])
    prices = [r["bbp"] for r in records if r.get("bbp")]
    if len(prices) < 2:
        return None
    prev, curr = prices[-2], prices[-1]
    if prev == 0:
        return None
    change_pct = (curr - prev) / prev
    if abs(change_pct) >= VOLATILITY_THRESHOLD:
        direction = "▲" if change_pct > 0 else "▼"
        return (
            f"⚠️ PRICE MOVE: **{item}** {direction}{abs(change_pct):.1%} "
            f"({int(prev):,} → {int(curr):,})"
        )
    return None


def detect_trend(item: str) -> str | None:
    records = price_history.get(item, [])
    prices = [r["bbp"] for r in records if r.get("bbp")]
    if len(prices) < 10:
        return None
    short_ma = sum(prices[-3:]) / 3
    long_ma  = sum(prices[-10:]) / 10
    delta    = (short_ma - long_ma) / long_ma
    if abs(delta) < 0.03:
        return None
    if short_ma < long_ma:
        return (
            f"📉 BEARISH: **{item}** "
            f"3-scan MA {int(short_ma):,} < 10-scan MA {int(long_ma):,} (Δ {delta:.1%})"
        )
    return (
        f"📈 BULLISH: **{item}** "
        f"3-scan MA {int(short_ma):,} > 10-scan MA {int(long_ma):,} (Δ +{delta:.1%})"
    )


def detect_crash_risk(item: str, bbp: float, gap_pct: float) -> str | None:
    records = price_history.get(item, [])
    if len(records) < 3:
        return None
    recent_prices = [r["bbp"]     for r in records[-3:] if r.get("bbp")]
    recent_gaps   = [r["gap_pct"] for r in records[-3:] if r.get("gap_pct") is not None]
    if not recent_prices or not recent_gaps:
        return None
    avg_price = sum(recent_prices) / len(recent_prices)
    avg_gap   = sum(recent_gaps)   / len(recent_gaps)
    if bbp < avg_price * 0.95 and gap_pct > avg_gap * 1.5:
        return (
            f"🚨 CRASH RISK: **{item}** — BBP down {(1 - bbp/avg_price):.1%} AND "
            f"bid gaps widened {(gap_pct/avg_gap - 1):.0%}. Buyers losing conviction."
        )
    return None

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def analyze_market(orders: list) -> list:
    alerts = []
    item_bids = {}

    for order in orders:
        item = order["item"]["itemId"]
        if item not in WATCH_ITEMS:
            continue
        remaining = order["amountOrdered"] - order["amountDelivered"]
        if remaining <= 0:
            continue
        item_bids.setdefault(item, []).append(order["itemPrice"])

    now = datetime.utcnow().isoformat()

    for item in WATCH_ITEMS:
        bids      = item_bids.get(item, [])
        top_bids  = compute_top_bids(bids)
        bbp       = compute_bbp(top_bids)

        if bbp is None:
            alerts.append(f"❓ **{item}** — no meaningful bids this scan")
            continue

        gap_pct      = compute_gap_pct(top_bids)
        top_display  = [f"{b:,}" for b in top_bids[:3]]
        demand_label = describe_demand(item, gap_pct) if gap_pct is not None else "❓"

        alerts.append(
            f"📊 **{item}** | BBP: {int(bbp):,} | "
            f"Top 3: [{', '.join(top_display)}] | "
            f"Demand: {demand_label}"
        )

        # Persist BEFORE detectors so current sample is visible to them
        records = price_history.setdefault(item, [])
        records.append({"ts": now, "bbp": bbp, "gap_pct": gap_pct, "top_bids": top_bids})
        if len(records) > MAX_HISTORY:
            records.pop(0)

        for signal in [
            detect_crash_risk(item, bbp, gap_pct or 0),
            detect_volatility(item),
            detect_demand_shift(item),
            detect_trend(item),
        ]:
            if signal:
                alerts.append(signal)

        fair = FAIR_VALUES.get(item)
        if fair:
            ratio = bbp / fair
            if ratio < 0.85:
                alerts.append(
                    f"🟢 BBP BELOW FAIR: **{item}** at {ratio:.0%} of fair "
                    f"({int(bbp):,} vs {fair:,})"
                )
            elif ratio > 1.15:
                alerts.append(
                    f"🔴 BBP ABOVE FAIR: **{item}** at {ratio:.0%} of fair "
                    f"({int(bbp):,} vs {fair:,})"
                )

    save_history(price_history)

    if not alerts:
        alerts.append("✅ Market scan complete — no signals detected.")

    return alerts