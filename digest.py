"""
digest.py — Weekly market summary digest.

Sent once per week (Sunday). Queries the historian SQLite database
to produce a human-readable summary of the past 7 days:

  - Biggest movers (up and down)
  - Most volatile items
  - Watch-list performance
  - Market regime summary (were we bull/bear this week?)
  - Thinnest markets (potential opportunities / risks)

All signals are derived from real data — no assumptions.
"""

import logging
from datetime import datetime, timezone
from historian import _connect, get_scan_count

logger = logging.getLogger(__name__)

DIGEST_MIN_SCANS = 10   # need at least this many scans to produce a digest


def _scans_last_n_days(days: int) -> list[int]:
    """Return scan IDs from the last N days."""
    with _connect() as conn:
        rows = conn.execute("""
            SELECT id FROM scans
            WHERE ts >= datetime('now', ?)
            ORDER BY id ASC
        """, (f"-{days} days",)).fetchall()
    return [r[0] for r in rows]


def biggest_movers(scan_ids: list, n: int = 5) -> tuple[list, list]:
    """
    Returns (top_gainers, top_losers) comparing first vs last scan in window.
    Each entry: { item_id, bbp_start, bbp_end, pct_change }
    """
    if len(scan_ids) < 2:
        return [], []

    first, last = scan_ids[0], scan_ids[-1]

    with _connect() as conn:
        rows = conn.execute("""
            SELECT a.item_id,
                   b.bbp                    AS bbp_start,
                   a.bbp                    AS bbp_end,
                   (a.bbp - b.bbp) / b.bbp  AS pct_change
            FROM snapshots a
            JOIN snapshots b
              ON a.item_id = b.item_id AND b.scan_id = ?
            WHERE a.scan_id = ?
              AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
              AND b.bbp > 0
            ORDER BY pct_change DESC
        """, (first, last)).fetchall()

    results   = [dict(r) for r in rows]
    gainers   = results[:n]
    losers    = sorted(results, key=lambda r: r["pct_change"])[:n]
    return gainers, losers


def most_volatile(scan_ids: list, n: int = 5) -> list[dict]:
    """
    Items with highest standard deviation of BBP as % of mean.
    High volatility = opportunity or risk depending on direction.
    """
    if not scan_ids:
        return []

    placeholders = ",".join("?" * len(scan_ids))
    with _connect() as conn:
        rows = conn.execute(f"""
            SELECT item_id,
                   AVG(bbp)  AS avg_bbp,
                   COUNT(*)  AS sample_count,
                   MAX(bbp) - MIN(bbp) AS range_bbp
            FROM snapshots
            WHERE scan_id IN ({placeholders})
              AND bbp IS NOT NULL
            GROUP BY item_id
            HAVING COUNT(*) >= 5
            ORDER BY (MAX(bbp) - MIN(bbp)) / AVG(bbp) DESC
            LIMIT ?
        """, (*scan_ids, n)).fetchall()

    return [dict(r) for r in rows]


def thinnest_markets(scan_ids: list, n: int = 5) -> list[dict]:
    """Items with the lowest average depth — fragile and price-sensitive."""
    if not scan_ids:
        return []

    placeholders = ",".join("?" * len(scan_ids))
    with _connect() as conn:
        rows = conn.execute(f"""
            SELECT item_id,
                   AVG(depth)  AS avg_depth,
                   AVG(bbp)    AS avg_bbp,
                   COUNT(*)    AS sample_count
            FROM snapshots
            WHERE scan_id IN ({placeholders})
              AND depth IS NOT NULL
              AND bbp IS NOT NULL
              AND bbp > 0
            GROUP BY item_id
            HAVING COUNT(*) >= 5
            ORDER BY avg_depth ASC
            LIMIT ?
        """, (*scan_ids, n)).fetchall()

    return [dict(r) for r in rows]


def watchlist_performance(scan_ids: list, watch_items: list) -> list[dict]:
    """BBP start, end, and % change for each watch-list item over the window."""
    if len(scan_ids) < 2:
        return []

    first, last = scan_ids[0], scan_ids[-1]
    results = []

    for item in watch_items:
        with _connect() as conn:
            row = conn.execute("""
                SELECT b.bbp AS bbp_start, a.bbp AS bbp_end
                FROM snapshots a
                JOIN snapshots b ON b.item_id = a.item_id AND b.scan_id = ?
                WHERE a.scan_id = ? AND a.item_id = ?
                  AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
            """, (first, last, item)).fetchone()

        if row:
            pct = (row["bbp_end"] - row["bbp_start"]) / row["bbp_start"]
            results.append({
                "item_id":   item,
                "bbp_start": row["bbp_start"],
                "bbp_end":   row["bbp_end"],
                "pct_change": pct,
            })

    return sorted(results, key=lambda r: r["pct_change"], reverse=True)


def market_regime_summary(scan_ids: list) -> str:
    """What fraction of scans had more items rising vs falling?"""
    if len(scan_ids) < 4:
        return "Not enough data for regime summary."

    bull_scans = 0
    bear_scans = 0

    for i in range(1, len(scan_ids)):
        prev, curr = scan_ids[i-1], scan_ids[i]
        with _connect() as conn:
            rows = conn.execute("""
                SELECT
                  SUM(CASE WHEN a.bbp > b.bbp * 1.01 THEN 1 ELSE 0 END) AS rising,
                  SUM(CASE WHEN a.bbp < b.bbp * 0.99 THEN 1 ELSE 0 END) AS falling,
                  COUNT(*) AS total
                FROM snapshots a
                JOIN snapshots b ON a.item_id = b.item_id AND b.scan_id = ?
                WHERE a.scan_id = ?
                  AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
            """, (prev, curr)).fetchone()

        if rows and rows["total"] > 0:
            if rows["rising"] / rows["total"] > 0.55:
                bull_scans += 1
            elif rows["falling"] / rows["total"] > 0.55:
                bear_scans += 1

    total = bull_scans + bear_scans
    if total == 0:
        return "Market was neutral/mixed all week."

    bull_pct = bull_scans / total
    if bull_pct > 0.6:
        return f"📈 Mostly BULLISH week ({bull_scans} bull scans vs {bear_scans} bear scans)"
    elif bull_pct < 0.4:
        return f"📉 Mostly BEARISH week ({bear_scans} bear scans vs {bull_scans} bull scans)"
    else:
        return f"↔️ Mixed week ({bull_scans} bull / {bear_scans} bear scans — no clear direction)"


def generate_digest(watch_items: list) -> list[str]:
    """
    Generate the full weekly digest.
    Returns a list of message strings to send to Discord.
    """
    scan_count = get_scan_count()
    if scan_count < DIGEST_MIN_SCANS:
        logger.info(f"Digest skipped — only {scan_count} scans recorded.")
        return []

    scan_ids = _scans_last_n_days(7)
    if len(scan_ids) < 2:
        return []

    now_str   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    messages  = []

    # Header
    messages.append(
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 **WEEKLY MARKET DIGEST** — {now_str}\n"
        f"Covering {len(scan_ids)} scans over the past 7 days\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

    # Regime
    regime = market_regime_summary(scan_ids)
    messages.append(f"**Market Regime:**\n{regime}")

    # Watch-list performance
    wl_perf = watchlist_performance(scan_ids, watch_items)
    if wl_perf:
        lines = ["**Watch-list Performance (7d):**"]
        for r in wl_perf:
            sign  = "+" if r["pct_change"] >= 0 else ""
            emoji = "📈" if r["pct_change"] >= 0 else "📉"
            lines.append(
                f"  {emoji} **{r['item_id']}** "
                f"{int(r['bbp_start']):,} → {int(r['bbp_end']):,} "
                f"({sign}{r['pct_change']:.1%})"
            )
        messages.append("\n".join(lines))

    # Biggest movers (all items)
    gainers, losers = biggest_movers(scan_ids, n=5)
    if gainers:
        lines = ["**Biggest Gainers (all items):**"]
        for r in gainers:
            lines.append(
                f"  📈 **{r['item_id']}** +{r['pct_change']:.1%} "
                f"({int(r['bbp_start']):,} → {int(r['bbp_end']):,})"
            )
        messages.append("\n".join(lines))

    if losers:
        lines = ["**Biggest Losers (all items):**"]
        for r in losers:
            lines.append(
                f"  📉 **{r['item_id']}** {r['pct_change']:.1%} "
                f"({int(r['bbp_start']):,} → {int(r['bbp_end']):,})"
            )
        messages.append("\n".join(lines))

    # Most volatile
    volatile = most_volatile(scan_ids, n=5)
    if volatile:
        lines = ["**Most Volatile Items:**"]
        for r in volatile:
            rng_pct = r["range_bbp"] / r["avg_bbp"] if r["avg_bbp"] else 0
            lines.append(
                f"  ⚠️ **{r['item_id']}** "
                f"range {rng_pct:.1%} of avg | avg BBP {int(r['avg_bbp']):,}"
            )
        messages.append("\n".join(lines))

    # Thinnest markets
    thin = thinnest_markets(scan_ids, n=5)
    if thin:
        lines = ["**Thinnest Markets (fragile, watch closely):**"]
        for r in thin:
            lines.append(
                f"  🫧 **{r['item_id']}** avg depth {r['avg_depth']:.1f} | "
                f"avg BBP {int(r['avg_bbp']):,}"
            )
        messages.append("\n".join(lines))

    messages.append("━━━━━━━━━━━━━━━━━━━━━━")
    return messages