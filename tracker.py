"""
tracker.py — Watch-list alerts with fully self-calibrating signals.

Calibration history is loaded directly from PostgreSQL on startup,
so it survives Railway redeployments. price_history.json is no longer
used — the database IS the source of truth.

On startup, the last CALIBRATION_WINDOW snapshots per watch-list item
are loaded from the snapshots table into memory. From there the scan
loop appends new records as before. No data is ever lost on redeploy.
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)

TOP_N_BIDS           = 10
MIN_BID_FLOOR        = 1000
DEPTH_FLOOR          = 0.90
MAX_HISTORY          = 96
CALIBRATION_WINDOW   = 48
CALIBRATION_MIN      = 10
VOLATILITY_THRESHOLD = 0.05

# In-memory rolling window — populated from PostgreSQL on startup
# Structure: { item_id: [ {ts, bbp, gap_pct, depth, top_bids}, ... ] }
price_history: dict = {}

# ---------------------------------------------------------------------------
# Load calibration history from PostgreSQL
# ---------------------------------------------------------------------------

def load_history_from_db(watch_items: list):
    """
    Called once at startup. Populates price_history from the last
    CALIBRATION_WINDOW snapshots per item in PostgreSQL.
    This means calibration survives redeployments completely.
    """
    global price_history
    from historian import execute_query

    logger.info("Loading calibration history from PostgreSQL...")
    loaded = 0

    for item in watch_items:
        rows = execute_query("""
            SELECT s.ts, sn.bbp, sn.gap_pct, sn.depth, sn.top_bid,
                   sn.second_bid, sn.third_bid
            FROM snapshots sn
            JOIN scans s ON s.id = sn.scan_id
            WHERE sn.item_id = %s
              AND sn.bbp IS NOT NULL
            ORDER BY sn.scan_id DESC
            LIMIT %s
        """, (item, CALIBRATION_WINDOW))

        # Reverse so oldest is first (same order as append logic)
        records = []
        for r in reversed(rows):
            top_bids = [b for b in [r["top_bid"], r["second_bid"], r["third_bid"]] if b]
            records.append({
                "ts":       r["ts"],
                "bbp":      r["bbp"],
                "gap_pct":  r["gap_pct"],
                "depth":    r["depth"],
                "top_bids": top_bids,
            })

        price_history[item] = records
        loaded += len(records)
        logger.info(f"  {item}: {len(records)} scans loaded")

    logger.info(f"Calibration ready — {loaded} total records loaded from DB.")


def reset_calibration(reason: str):
    """
    Called by macro.py on regime change.
    Wipes rolling windows so the system recalibrates from new baseline.
    """
    global price_history
    logger.warning(f"Calibration reset triggered: {reason}")
    for item in list(price_history.keys()):
        if isinstance(price_history[item], list):
            price_history[item] = []
    price_history["_regime_reset"] = {
        "ts":     datetime.utcnow().isoformat(),
        "reason": reason
    }

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

def compute_depth(all_real_bids: list, bbp: float) -> int:
    if not bbp:
        return 0
    return sum(1 for b in all_real_bids if b >= bbp * DEPTH_FLOOR)

# ---------------------------------------------------------------------------
# Z-score helper
# ---------------------------------------------------------------------------

def _zscore(value: float, window: list) -> float:
    n   = len(window)
    avg = sum(window) / n
    var = sum((x - avg) ** 2 for x in window) / n
    std = var ** 0.5
    return (value - avg) / std if std > 0 else 0

# ---------------------------------------------------------------------------
# Self-calibrating demand label (gap%)
# ---------------------------------------------------------------------------

def describe_demand(item: str, gap_pct: float) -> str:
    records = price_history.get(item, [])
    window  = [r["gap_pct"] for r in records[-CALIBRATION_WINDOW:]
               if r.get("gap_pct") is not None]
    n = len(window)
    if n < CALIBRATION_MIN:
        return f"📊 CALIBRATING ({n}/{CALIBRATION_MIN}, gap {gap_pct*100:.2f}%)"

    avg = sum(window) / n
    z   = _zscore(gap_pct, window)

    if z < -1.5:
        return f"🔥 HIGH (gap {gap_pct*100:.2f}% vs baseline {avg*100:.2f}%)"
    elif z > 1.5:
        return f"🧊 LOW  (gap {gap_pct*100:.2f}% vs baseline {avg*100:.2f}%)"
    else:
        return f"🟡 MEDIUM (gap {gap_pct*100:.2f}% vs baseline {avg*100:.2f}%)"

# ---------------------------------------------------------------------------
# Self-calibrating depth label
# ---------------------------------------------------------------------------

def describe_depth(item: str, depth: int) -> str:
    records = price_history.get(item, [])
    window  = [r["depth"] for r in records[-CALIBRATION_WINDOW:]
               if r.get("depth") is not None]
    n = len(window)
    if n < CALIBRATION_MIN:
        return f"📊 CALIBRATING ({depth})"

    avg = sum(window) / n
    z   = _zscore(depth, window)

    if z > 1.5:
        return f"💧 DEEP ({depth} vs baseline {avg:.0f})"
    elif z < -1.5:
        return f"⚠️ THIN ({depth} vs baseline {avg:.0f})"
    else:
        return f"({depth} vs baseline {avg:.0f})"

# ---------------------------------------------------------------------------
# Signal detectors
# ---------------------------------------------------------------------------

def detect_demand_shift(item: str) -> str | None:
    records = price_history.get(item, [])
    gaps    = [r["gap_pct"] for r in records[-CALIBRATION_WINDOW:]
               if r.get("gap_pct") is not None]
    if len(gaps) < 4:
        return None
    prev_avg = sum(gaps[-4:-1]) / 3
    curr     = gaps[-1]
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
    prices  = [r["bbp"] for r in records[-CALIBRATION_WINDOW:] if r.get("bbp")]
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
    prices  = [r["bbp"] for r in records[-CALIBRATION_WINDOW:] if r.get("bbp")]
    if len(prices) < 10:
        return None
    short_ma = sum(prices[-3:])  / 3
    long_ma  = sum(prices[-10:]) / 10
    delta    = (short_ma - long_ma) / long_ma
    if abs(delta) < 0.03:
        return None
    if short_ma < long_ma:
        return (
            f"📉 BEARISH: **{item}** "
            f"3-scan {int(short_ma):,} < 10-scan {int(long_ma):,} (Δ {delta:.1%})"
        )
    return (
        f"📈 BULLISH: **{item}** "
        f"3-scan {int(short_ma):,} > 10-scan {int(long_ma):,} (Δ +{delta:.1%})"
    )


def detect_crash_risk(item: str, bbp: float, gap_pct: float, depth: int) -> str | None:
    records       = price_history.get(item, [])
    recent        = records[-3:]
    recent_prices = [r["bbp"]     for r in recent if r.get("bbp")]
    recent_gaps   = [r["gap_pct"] for r in recent if r.get("gap_pct") is not None]

    if not recent_prices or not recent_gaps:
        return None

    avg_price = sum(recent_prices) / len(recent_prices)
    avg_gap   = sum(recent_gaps)   / len(recent_gaps)

    if not (bbp < avg_price * 0.95 and gap_pct > avg_gap * 1.5):
        return None

    depth_window = [r["depth"] for r in records[-CALIBRATION_WINDOW:]
                    if r.get("depth") is not None]
    avg_depth    = sum(depth_window) / len(depth_window) if depth_window else depth
    severity     = "🚨🚨 CRITICAL" if depth < avg_depth * 0.6 else "🚨 CRASH RISK"

    return (
        f"{severity}: **{item}** — BBP down {(1 - bbp/avg_price):.1%}, "
        f"gaps widened {(gap_pct/avg_gap - 1):.0%}, "
        f"depth {describe_depth(item, depth)}. Buyers losing conviction."
    )

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def analyze_market(orders: list, watch_items: list, fair_values: dict) -> list:
    alerts = []

    reset_info = price_history.get("_regime_reset")
    if reset_info and isinstance(reset_info, dict):
        alerts.append(
            f"⚡ REGIME RESET active since {reset_info['ts']} "
            f"({reset_info['reason'][:60]}...) — recalibrating."
        )

    item_bids = {}
    for order in orders:
        item      = order["item"]["itemId"]
        if item not in watch_items:
            continue
        remaining = order["amountOrdered"] - order["amountDelivered"]
        if remaining <= 0:
            continue
        item_bids.setdefault(item, []).append(order["itemPrice"])

    now = datetime.utcnow().isoformat()

    for item in watch_items:
        bids     = item_bids.get(item, [])
        all_real = sorted([b for b in bids if b >= MIN_BID_FLOOR], reverse=True)
        top_bids = all_real[:TOP_N_BIDS]
        bbp      = compute_bbp(top_bids)

        if bbp is None:
            alerts.append(f"❓ **{item}** — no meaningful bids this scan")
            continue

        gap_pct     = compute_gap_pct(top_bids)
        depth       = compute_depth(all_real, bbp)
        top_display = [f"{b:,}" for b in top_bids[:3]]
        demand_lbl  = describe_demand(item, gap_pct) if gap_pct is not None else "❓"
        depth_lbl   = describe_depth(item, depth)

        alerts.append(
            f"📊 **{item}** | BBP: {int(bbp):,} | "
            f"Top 3: [{', '.join(top_display)}] | "
            f"Demand: {demand_lbl} | Depth: {depth_lbl}"
        )

        # Persist to in-memory window (PostgreSQL already has the record
        # from historian.record_scan — this is just for signal detection)
        records = price_history.setdefault(item, [])
        if not isinstance(records, list):
            records = []
            price_history[item] = records

        records.append({
            "ts": now, "bbp": bbp,
            "gap_pct": gap_pct, "depth": depth,
            "top_bids": top_bids,
        })
        if len(records) > MAX_HISTORY:
            records.pop(0)

        for signal in [
            detect_crash_risk(item, bbp, gap_pct or 0, depth),
            detect_volatility(item),
            detect_demand_shift(item),
            detect_trend(item),
        ]:
            if signal:
                alerts.append(signal)

        fair = fair_values.get(item)
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

    if not alerts:
        alerts.append("✅ Market scan complete — no signals detected.")

    return alerts