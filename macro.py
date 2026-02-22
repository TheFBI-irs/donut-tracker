"""
macro.py — Economy-wide signals + regime change detection.
Updated for PostgreSQL — uses %s placeholders throughout.
"""

import logging
from historian import execute_query, get_scan_count, _connect

logger = logging.getLogger(__name__)

MACRO_MIN_SCANS         = 20
TOP_MOVERS_N            = 5
REGIME_CHANGE_THRESHOLD = 0.50
REGIME_MOVE_PCT         = 0.10


def _require_scans(n: int) -> bool:
    count = get_scan_count()
    if count < n:
        logger.info(f"Macro: {count}/{n} scans, still calibrating.")
        return False
    return True


def _latest_scan_ids(n: int) -> list[int]:
    rows = execute_query(
        "SELECT id FROM scans ORDER BY id DESC LIMIT %s", (n,)
    )
    return [r["id"] for r in rows]


# ---------------------------------------------------------------------------
# Regime change detector
# ---------------------------------------------------------------------------

def detect_regime_change() -> str | None:
    if not _require_scans(MACRO_MIN_SCANS):
        return None

    scan_ids = _latest_scan_ids(2)
    if len(scan_ids) < 2:
        return None

    newest, previous = scan_ids[0], scan_ids[1]

    rows = execute_query("""
        SELECT a.item_id,
               (a.bbp - b.bbp) / b.bbp AS pct_change
        FROM snapshots a
        JOIN snapshots b
          ON a.item_id = b.item_id AND b.scan_id = %s
        WHERE a.scan_id = %s
          AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
          AND b.bbp > 0
    """, (previous, newest))

    if not rows:
        return None

    total   = len(rows)
    rising  = [r for r in rows if r["pct_change"] >  REGIME_MOVE_PCT]
    falling = [r for r in rows if r["pct_change"] < -REGIME_MOVE_PCT]

    rise_frac = len(rising)  / total
    fall_frac = len(falling) / total

    if fall_frac >= REGIME_CHANGE_THRESHOLD:
        avg_drop = sum(r["pct_change"] for r in falling) / len(falling)
        worst    = min(falling, key=lambda r: r["pct_change"])
        return (
            f"⚡ REGIME CHANGE: {len(falling)}/{total} items fell >10% simultaneously "
            f"(avg {avg_drop:.1%}, worst: {worst['item_id']} {worst['pct_change']:.1%}). "
            f"Likely world border or mass liquidation. Calibration reset."
        )
    elif rise_frac >= REGIME_CHANGE_THRESHOLD:
        avg_rise = sum(r["pct_change"] for r in rising) / len(rising)
        best     = max(rising, key=lambda r: r["pct_change"])
        return (
            f"⚡ REGIME CHANGE: {len(rising)}/{total} items rose >10% simultaneously "
            f"(avg +{avg_rise:.1%}, leader: {best['item_id']} +{best['pct_change']:.1%}). "
            f"Possible content drop or economy injection. Calibration reset."
        )
    return None


# ---------------------------------------------------------------------------
# Market breadth
# ---------------------------------------------------------------------------

def market_breadth() -> str | None:
    if not _require_scans(MACRO_MIN_SCANS):
        return None

    scan_ids = _latest_scan_ids(3)
    if len(scan_ids) < 3:
        return None

    newest, _, oldest = scan_ids[0], scan_ids[1], scan_ids[2]

    rows = execute_query("""
        SELECT a.item_id,
               a.bbp AS bbp_new,
               b.bbp AS bbp_old
        FROM snapshots a
        JOIN snapshots b
          ON a.item_id = b.item_id AND b.scan_id = %s
        WHERE a.scan_id = %s
          AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
    """, (oldest, newest))

    if not rows:
        return None

    total   = len(rows)
    rising  = sum(1 for r in rows if r["bbp_new"] > r["bbp_old"] * 1.01)
    falling = sum(1 for r in rows if r["bbp_new"] < r["bbp_old"] * 0.99)
    pct_up  = rising / total

    if pct_up > 0.65:
        return f"📈 BULL MARKET: {rising}/{total} items rising ({pct_up:.0%} breadth)"
    elif pct_up < 0.35:
        return f"📉 BEAR MARKET: {falling}/{total} items falling ({1-pct_up:.0%} breadth)"
    return None


# ---------------------------------------------------------------------------
# Top movers outside watch list
# ---------------------------------------------------------------------------

def top_movers(watch_items: list) -> list[str]:
    if not _require_scans(MACRO_MIN_SCANS):
        return []

    scan_ids = _latest_scan_ids(2)
    if len(scan_ids) < 2:
        return []

    newest, previous = scan_ids[0], scan_ids[1]

    rows = execute_query("""
        SELECT a.item_id,
               a.bbp                   AS bbp_new,
               b.bbp                   AS bbp_old,
               (a.bbp - b.bbp) / b.bbp AS pct_change
        FROM snapshots a
        JOIN snapshots b
          ON a.item_id = b.item_id AND b.scan_id = %s
        WHERE a.scan_id = %s
          AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
          AND b.bbp > 0
          AND ABS((a.bbp - b.bbp) / b.bbp) > 0.08
        ORDER BY ABS((a.bbp - b.bbp) / b.bbp) DESC
        LIMIT %s
    """, (previous, newest, TOP_MOVERS_N * 3))

    outside = [r for r in rows if r["item_id"] not in watch_items][:TOP_MOVERS_N]

    alerts = []
    for r in outside:
        direction = "▲" if r["pct_change"] > 0 else "▼"
        alerts.append(
            f"👀 OFF-WATCH MOVER: **{r['item_id']}** "
            f"{direction}{abs(r['pct_change']):.1%} "
            f"({int(r['bbp_old']):,} → {int(r['bbp_new']):,})"
        )
    return alerts


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def analyze_macro(watch_items: list) -> list[str]:
    from tracker import reset_calibration

    alerts     = []
    scan_count = get_scan_count()

    if scan_count < MACRO_MIN_SCANS:
        alerts.append(
            f"🌐 MACRO: Calibrating — {scan_count}/{MACRO_MIN_SCANS} scans. "
            f"Unlocks in ~{(MACRO_MIN_SCANS - scan_count) * 0.5:.0f} hours."
        )
        return alerts

    regime = detect_regime_change()
    if regime:
        alerts.append(regime)
        reset_calibration(regime)
        return alerts

    breadth = market_breadth()
    if breadth:
        alerts.append(breadth)

    movers = top_movers(watch_items)
    alerts.extend(movers)

    if not alerts:
        alerts.append("🌐 MACRO: No broad market signals this scan.")

    return alerts