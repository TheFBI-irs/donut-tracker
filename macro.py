"""
macro.py — Economy-wide signals + regime change detection.

REGIME CHANGE:
  A regime change is when the economy's structure shifts fundamentally —
  world border expansion, server reset, mass liquidation event.

  Detection: if >50% of all items move >10% in the same scan direction,
  it's almost certainly a structural event, not normal price action.

  Response:
    1. Fire a high-priority alert describing the event
    2. Call tracker.reset_calibration() to wipe all rolling baselines
    3. System recalibrates from the new post-event reality automatically

  This prevents pre-event calibration data from poisoning signals
  for days after a world border expansion.
"""

import logging
from historian import get_all_items_latest, get_scan_count, _connect

logger = logging.getLogger(__name__)

MACRO_MIN_SCANS        = 20
TOP_MOVERS_N           = 5
REGIME_CHANGE_THRESHOLD = 0.50   # fraction of items that must move together
REGIME_MOVE_PCT        = 0.10    # each item must move at least this much


def _require_scans(n: int) -> bool:
    count = get_scan_count()
    if count < n:
        logger.info(f"Macro: {count}/{n} scans, still calibrating.")
        return False
    return True


# ---------------------------------------------------------------------------
# Regime change detector
# ---------------------------------------------------------------------------

def detect_regime_change() -> str | None:
    """
    Returns a description string if a regime change is detected, else None.
    Caller is responsible for triggering calibration reset.
    """
    if not _require_scans(MACRO_MIN_SCANS):
        return None

    with _connect() as conn:
        scan_ids = [r[0] for r in conn.execute(
            "SELECT id FROM scans ORDER BY id DESC LIMIT 2"
        ).fetchall()]

    if len(scan_ids) < 2:
        return None

    newest, previous = scan_ids[0], scan_ids[1]

    with _connect() as conn:
        rows = conn.execute("""
            SELECT a.item_id,
                   (a.bbp - b.bbp) / b.bbp AS pct_change
            FROM snapshots a
            JOIN snapshots b
              ON a.item_id = b.item_id AND b.scan_id = ?
            WHERE a.scan_id = ?
              AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
              AND b.bbp > 0
        """, (previous, newest)).fetchall()

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
            f"⚡ REGIME CHANGE DETECTED: {len(falling)}/{total} items fell "
            f">10% simultaneously (avg {avg_drop:.1%}, worst: "
            f"{worst['item_id']} {worst['pct_change']:.1%}). "
            f"Likely world border expansion or mass liquidation. "
            f"Calibration reset — recalibrating from new baseline."
        )
    elif rise_frac >= REGIME_CHANGE_THRESHOLD:
        avg_rise = sum(r["pct_change"] for r in rising) / len(rising)
        best     = max(rising, key=lambda r: r["pct_change"])
        return (
            f"⚡ REGIME CHANGE DETECTED: {len(rising)}/{total} items rose "
            f">10% simultaneously (avg +{avg_rise:.1%}, leader: "
            f"{best['item_id']} +{best['pct_change']:.1%}). "
            f"Possible new content drop or economy injection. "
            f"Calibration reset — recalibrating from new baseline."
        )
    return None


# ---------------------------------------------------------------------------
# Market breadth
# ---------------------------------------------------------------------------

def market_breadth() -> str | None:
    if not _require_scans(MACRO_MIN_SCANS):
        return None

    with _connect() as conn:
        scan_ids = [r[0] for r in conn.execute(
            "SELECT id FROM scans ORDER BY id DESC LIMIT 3"
        ).fetchall()]

    if len(scan_ids) < 3:
        return None

    newest, _, oldest = scan_ids[0], scan_ids[1], scan_ids[2]

    with _connect() as conn:
        rows = conn.execute("""
            SELECT a.item_id,
                   a.bbp AS bbp_new,
                   b.bbp AS bbp_old
            FROM snapshots a
            JOIN snapshots b
              ON a.item_id = b.item_id AND b.scan_id = ?
            WHERE a.scan_id = ?
              AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
        """, (oldest, newest)).fetchall()

    if not rows:
        return None

    rising  = sum(1 for r in rows if r["bbp_new"] > r["bbp_old"] * 1.01)
    falling = sum(1 for r in rows if r["bbp_new"] < r["bbp_old"] * 0.99)
    total   = len(rows)
    pct_up  = rising / total

    if pct_up > 0.65:
        return (
            f"📈 BULL MARKET: {rising}/{total} items rising "
            f"({pct_up:.0%} breadth) — economy heating up"
        )
    elif pct_up < 0.35:
        return (
            f"📉 BEAR MARKET: {falling}/{total} items falling "
            f"({1-pct_up:.0%} breadth) — economy cooling down"
        )
    return None


# ---------------------------------------------------------------------------
# Top movers outside watch list
# ---------------------------------------------------------------------------

def top_movers(watch_items: list) -> list[str]:
    if not _require_scans(MACRO_MIN_SCANS):
        return []

    with _connect() as conn:
        scan_ids = [r[0] for r in conn.execute(
            "SELECT id FROM scans ORDER BY id DESC LIMIT 2"
        ).fetchall()]

    if len(scan_ids) < 2:
        return []

    newest, previous = scan_ids[0], scan_ids[1]

    with _connect() as conn:
        rows = conn.execute("""
            SELECT a.item_id,
                   a.bbp                    AS bbp_new,
                   b.bbp                    AS bbp_old,
                   (a.bbp - b.bbp) / b.bbp  AS pct_change
            FROM snapshots a
            JOIN snapshots b
              ON a.item_id = b.item_id AND b.scan_id = ?
            WHERE a.scan_id = ?
              AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
              AND b.bbp > 0
              AND ABS((a.bbp - b.bbp) / b.bbp) > 0.08
            ORDER BY ABS((a.bbp - b.bbp) / b.bbp) DESC
            LIMIT ?
        """, (previous, newest, TOP_MOVERS_N * 3)).fetchall()

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
    # Import here to avoid circular import
    from tracker import reset_calibration

    alerts = []
    scan_count = get_scan_count()

    if scan_count < MACRO_MIN_SCANS:
        alerts.append(
            f"🌐 MACRO: Calibrating — {scan_count}/{MACRO_MIN_SCANS} scans. "
            f"Macro signals unlock in ~{(MACRO_MIN_SCANS - scan_count) * 0.5:.0f} hours."
        )
        return alerts

    # Regime change check runs first — highest priority signal
    regime = detect_regime_change()
    if regime:
        alerts.append(regime)
        reset_calibration(regime)
        # Don't run other signals this cycle — data just changed fundamentally
        return alerts

    breadth = market_breadth()
    if breadth:
        alerts.append(breadth)

    movers = top_movers(watch_items)
    alerts.extend(movers)

    if not alerts:
        alerts.append("🌐 MACRO: No broad market signals this scan.")

    return alerts