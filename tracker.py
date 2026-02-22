"""
tracker.py — Market intelligence for Donut SMP

DEMAND MODEL:
  Unit volume is meaningless — players store money in troll orders like
  "buy 10,000,000 elytras at 1 coin each."

  Real demand signal = BID DENSITY: how tightly are serious buyers
  clustered? In a competitive market, players outbid each other by small
  margins. In a dead market, gaps between bids are huge.

  METRIC: Average Gap % = mean gap between top N bids / top bid price
    - Small gap%  → buyers competing hard → high demand
    - Large gap%  → nobody is fighting for supply → low demand
    - Gap% falling over time → demand building
    - Gap% rising over time  → demand evaporating

PRICE MODEL:
  Best Bid Price (BBP) = average of top N bids above troll floor.
  Tracks what serious buyers are actually willing to pay right now.
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

TOP_N_BIDS = 10         # Bids used for gap analysis and BBP
MIN_BID_FLOOR = 1000    # Hard troll filter — ignore anything below this
HISTORY_FILE = "price_history.json"
MAX_HISTORY = 48        # ~24 hours at 30-min intervals

# Demand pressure thresholds (gap as % of top bid)
GAP_HIGH_DEMAND   = 0.005   # <0.5% gap  → very competitive
GAP_MEDIUM_DEMAND = 0.02    # <2% gap    → moderate competition
# >2% gap = low demand / thin market

# Volatility threshold: BBP move between two consecutive scans
VOLATILITY_THRESHOLD = 0.05  # 5% change scan-to-scan

FAIR_VALUES = {
    "elytra":                  280_000_000,
    "netherite_ingot":           4_300_000,
    "netherite_block":          38_000_000,
    "dragon_head":              26_000_000,
    "enchanted_golden_apple":      620_000,
}

# ---------------------------------------------------------------------------
# Persistent history
# Record per scan:
#   { "ts": str, "bbp": float, "gap_pct": float, "top_bids": [int, ...] }
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
    """Return top N real bids sorted descending, above troll floor."""
    return sorted([b for b in bids if b >= MIN_BID_FLOOR], reverse=True)[:TOP_N_BIDS]

def compute_bbp(top_bids: list) -> float | None:
    """Average of top N bids. None if no real bids exist."""
    if not top_bids:
        return None
    return sum(top_bids) / len(top_bids)

def compute_gap_pct(top_bids: list) -> float | None:
    """
    Average gap between consecutive top bids, as % of the highest bid.
    Requires at least 2 bids.

    Small value = buyers are tightly competing (high demand).
    Large value = wide spacing between bids (low demand / thin market).
    """
    if len(top_bids) < 2:
        return None
    gaps = [top_bids[i] - top_bids[i + 1] for i in range(len(top_bids) - 1)]
    avg_gap = sum(gaps) / len(gaps)
    return avg_gap / top_bids[0]

def describe_demand(gap_pct: float) -> str:
    if gap_pct < GAP_HIGH_DEMAND:
        return "🔥 HIGH"
    elif gap_pct < GAP_MEDIUM_DEMAND:
        return "🟡 MEDIUM"
    else:
        return "🧊 LOW"

# ---------------------------------------------------------------------------
# Signal detectors
# ---------------------------------------------------------------------------

def detect_demand_shift(item: str) -> str | None:
    """
    Detects meaningful change in bid density over last 3 scans.
    Gap % rising = demand weakening. Gap % falling = demand building.
    """
    records = price_history.get(item, [])
    gaps = [r["gap_pct"] for r in records if r.get("gap_pct") is not None]

    if len(gaps) < 4:
        return None

    prev_avg = sum(gaps[-4:-1]) / 3   # average of 3 scans before current
    curr = gaps[-1]

    if prev_avg == 0:
        return None

    change = (curr - prev_avg) / prev_avg

    if change > 0.5:   # gaps widened 50%+ → buyers backing off
        return (
            f"📉 DEMAND WEAKENING: **{item}** — bid gaps widened "
            f"{change:.0%} vs recent avg (buyers spreading out)"
        )
    elif change < -0.5:   # gaps tightened 50%+ → buyers competing harder
        return (
            f"📈 DEMAND BUILDING: **{item}** — bid gaps tightened "
            f"{abs(change):.0%} vs recent avg (buyers competing harder)"
        )
    return None


def detect_volatility(item: str) -> str | None:
    """BBP moved >5% between last two scans."""
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
            f"⚠️ PRICE MOVE: **{item}** BBP {direction}{abs(change_pct):.1%} "
            f"since last scan ({int(prev):,} → {int(curr):,})"
        )
    return None


def detect_trend(item: str) -> str | None:
    """Short MA vs long MA on BBP. Only signals when divergence > 3%."""
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
            f"(3-scan MA {int(short_ma):,} < 10-scan MA {int(long_ma):,}, Δ {delta_pct:.1%})"
        )
    return (
        f"📈 BULLISH TREND: **{item}** "
        f"(3-scan MA {int(short_ma):,} > 10-scan MA {int(long_ma):,}, Δ +{delta_pct:.1%})"
    )


def detect_crash_risk(item: str, current_bbp: float, current_gap_pct: float) -> str | None:
    """
    Crash = BBP falling AND bid gaps widening simultaneously.
    Buyers losing conviction: top bids drop AND competition evaporates.
    """
    records = price_history.get(item, [])
    if len(records) < 3:
        return None

    recent_prices = [r["bbp"] for r in records[-3:] if r.get("bbp")]
    recent_gaps   = [r["gap_pct"] for r in records[-3:] if r.get("gap_pct") is not None]

    if not recent_prices or not recent_gaps:
        return None

    avg_price = sum(recent_prices) / len(recent_prices)
    avg_gap   = sum(recent_gaps)   / len(recent_gaps)

    price_falling   = current_bbp     < avg_price * 0.95
    demand_thinning = current_gap_pct > avg_gap   * 1.5

    if price_falling and demand_thinning:
        price_drop = (1 - current_bbp / avg_price)
        gap_change = (current_gap_pct / avg_gap - 1)
        return (
            f"🚨 CRASH RISK: **{item}** — BBP down {price_drop:.1%} AND bid gaps "
            f"widened {gap_change:.0%}. Buyers losing conviction fast."
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
        bids = item_bids.get(item, [])
        top_bids = compute_top_bids(bids)
        bbp = compute_bbp(top_bids)

        if bbp is None:
            alerts.append(f"❓ **{item}** — no meaningful bids this scan")
            continue

        gap_pct = compute_gap_pct(top_bids)
        demand_label = describe_demand(gap_pct) if gap_pct is not None else "❓ UNKNOWN"
        top_display = [f"{b:,}" for b in top_bids[:3]]

        alerts.append(
            f"📊 **{item}** | BBP: {int(bbp):,} | "
            f"Top 3: [{', '.join(top_display)}] | "
            f"Demand pressure: {demand_label} "
            f"(gap {gap_pct*100:.2f}% of top bid)"
        )

        # Persist BEFORE running detectors so they see the current sample
        records = price_history.setdefault(item, [])
        records.append({
            "ts": now,
            "bbp": bbp,
            "gap_pct": gap_pct,
            "top_bids": top_bids,
        })
        if len(records) > MAX_HISTORY:
            records.pop(0)

        # Signals: crash > volatility > demand shift > trend > fair value
        crash = detect_crash_risk(item, bbp, gap_pct if gap_pct else 0)
        if crash:
            alerts.append(crash)

        vol = detect_volatility(item)
        if vol:
            alerts.append(vol)

        demand_shift = detect_demand_shift(item)
        if demand_shift:
            alerts.append(demand_shift)

        trend = detect_trend(item)
        if trend:
            alerts.append(trend)

        fair = FAIR_VALUES.get(item)
        if fair:
            ratio = bbp / fair
            if ratio < 0.85:
                alerts.append(
                    f"🟢 BBP BELOW FAIR: **{item}** at {ratio:.0%} of fair value "
                    f"({int(bbp):,} vs {fair:,}) — buyers paying below fair"
                )
            elif ratio > 1.15:
                alerts.append(
                    f"🔴 BBP ABOVE FAIR: **{item}** at {ratio:.0%} of fair value "
                    f"({int(bbp):,} vs {fair:,}) — buyers paying above fair"
                )

    save_history(price_history)

    if not alerts:
        alerts.append("✅ Market scan complete — no signals detected.")

    return alerts