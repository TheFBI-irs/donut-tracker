"""
macro.py — Economy-wide market intelligence.

Reads the full historian database (all items, all scans) to detect
patterns that are invisible when watching 5 items in isolation:

  - Broad market regime: is the economy heating up or cooling down?
  - Correlated crashes: multiple items moving together = systemic event
  - Leaders and laggards: which items move first, which follow?
  - Unusual movers: items outside watch-list that are spiking
  - Market breadth: what % of items are in uptrend vs downtrend?
"""

import logging
from historian import get_all_items_latest, get_item_history, get_scan_count, _connect

logger = logging.getLogger(__name__)

# Minimum scans before macro signals are meaningful
MACRO_MIN_SCANS = 20

# How many top movers to surface in alerts
TOP_MOVERS_N = 5


def _require_scans(n: int) -> bool:
    count = get_scan_count()
    if count < n:
        logger.info(f"Macro: only {count}/{n} scans recorded, still calibrating.")
        return False
    return True


# ---------------------------------------------------------------------------
# Broad market regime
# Looks at what fraction of all items have rising vs falling BBP
# over the last 3 scans. Surfaces overall market direction.
# ---------------------------------------------------------------------------

def market_breadth() -> str | None:
    if not _require_scans(MACRO_MIN_SCANS):
        return None

    with _connect() as conn:
        # Get last 3 scan IDs
        scan_ids = [r[0] for r in conn.execute(
            "SELECT id FROM scans ORDER BY id DESC LIMIT 3"
        ).fetchall()]

    if len(scan_ids) < 3:
        return None

    newest, middle, oldest = scan_ids[0], scan_ids[1], scan_ids[2]

    with _connect() as conn:
        # Items present in all 3 scans
        rows = conn.execute("""
            SELECT a.item_id,
                   a.bbp AS bbp_new,
                   b.bbp AS bbp_old
            FROM snapshots a
            JOIN snapshots b
              ON a.item_id = b.item_id AND b.scan_id = ?
            WHERE a.scan_id = ?
              AND a.bbp IS NOT NULL
              AND b.bbp IS NOT NULL
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
            f"({pct_up:.0%} breadth) — broad economy heating up"
        )
    elif pct_up < 0.35:
        return (
            f"📉 BEAR MARKET: {falling}/{total} items falling "
            f"({1-pct_up:.0%} breadth) — broad economy cooling down"
        )
    return None


# ---------------------------------------------------------------------------
# Correlated crash detector
# If multiple items from different categories all drop simultaneously,
# it's a systemic event (world border, server reset, mass sell-off).
# ---------------------------------------------------------------------------

def correlated_crash() -> str | None:
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
                   a.bbp AS bbp_new,
                   b.bbp AS bbp_old,
                   (a.bbp - b.bbp) / b.bbp AS pct_change
            FROM snapshots a
            JOIN snapshots b
              ON a.item_id = b.item_id AND b.scan_id = ?
            WHERE a.scan_id = ?
              AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
              AND b.bbp > 0
              AND (a.bbp - b.bbp) / b.bbp < -0.07
            ORDER BY pct_change ASC
        """, (previous, newest)).fetchall()

    if len(rows) >= 5:
        names = [r["item_id"] for r in rows[:5]]
        worst = rows[0]["pct_change"]
        return (
            f"🚨 SYSTEMIC EVENT: {len(rows)} items crashed simultaneously "
            f"(worst: {names[0]} {worst:.1%}). Likely macro cause — "
            f"world border, reset, or mass liquidation. Top affected: {', '.join(names)}"
        )
    return None


# ---------------------------------------------------------------------------
# Top movers (outside watch list)
# Surfaces items with the biggest price moves that you might not be watching.
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
                   a.bbp              AS bbp_new,
                   b.bbp              AS bbp_old,
                   (a.bbp - b.bbp) / b.bbp AS pct_change
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

    # Exclude items already on watch list
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
# Market summary — call this once per scan cycle
# ---------------------------------------------------------------------------

def analyze_macro(watch_items: list) -> list[str]:
    alerts = []
    scan_count = get_scan_count()

    if scan_count < MACRO_MIN_SCANS:
        alerts.append(
            f"🌐 MACRO: Calibrating — {scan_count}/{MACRO_MIN_SCANS} scans collected. "
            f"Macro signals unlock after {MACRO_MIN_SCANS - scan_count} more scans "
            f"(~{(MACRO_MIN_SCANS - scan_count) * 0.5:.0f} hours)."
        )
        return alerts

    breadth = market_breadth()
    if breadth:
        alerts.append(breadth)

    crash = correlated_crash()
    if crash:
        alerts.append(crash)

    movers = top_movers(watch_items)
    alerts.extend(movers)

    if not alerts:
        alerts.append("🌐 MACRO: No broad market signals this scan.")

    return alerts