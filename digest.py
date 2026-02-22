"""
digest.py — Weekly market summary digest.
Updated for PostgreSQL — uses %s placeholders throughout.
"""

import logging
from datetime import datetime, timezone
from historian import execute_query, get_scan_count

logger = logging.getLogger(__name__)

DIGEST_MIN_SCANS = 10


def _scans_last_n_days(days: int) -> list[int]:
    rows = execute_query(
        f"SELECT id FROM scans WHERE ts::timestamptz >= NOW() - INTERVAL '{int(days)} days' ORDER BY id ASC"
    )
    return [r["id"] for r in rows]


def biggest_movers(scan_ids: list, n: int = 5) -> tuple[list, list]:
    if len(scan_ids) < 2:
        return [], []

    first, last = scan_ids[0], scan_ids[-1]

    rows = execute_query("""
        SELECT a.item_id,
               b.bbp                   AS bbp_start,
               a.bbp                   AS bbp_end,
               (a.bbp - b.bbp) / b.bbp AS pct_change
        FROM snapshots a
        JOIN snapshots b
          ON a.item_id = b.item_id AND b.scan_id = %s
        WHERE a.scan_id = %s
          AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
          AND b.bbp > 0
        ORDER BY pct_change DESC
    """, (first, last))

    gainers = rows[:n]
    losers  = sorted(rows, key=lambda r: r["pct_change"])[:n]
    return gainers, losers


def most_volatile(scan_ids: list, n: int = 5) -> list[dict]:
    if not scan_ids:
        return []

    placeholders = ",".join(["%s"] * len(scan_ids))
    return execute_query(f"""
        SELECT item_id,
               AVG(bbp)           AS avg_bbp,
               COUNT(*)           AS sample_count,
               MAX(bbp) - MIN(bbp) AS range_bbp
        FROM snapshots
        WHERE scan_id IN ({placeholders})
          AND bbp IS NOT NULL
        GROUP BY item_id
        HAVING COUNT(*) >= 5
        ORDER BY (MAX(bbp) - MIN(bbp)) / NULLIF(AVG(bbp), 0) DESC
        LIMIT %s
    """, (*scan_ids, n))


def thinnest_markets(scan_ids: list, n: int = 5) -> list[dict]:
    if not scan_ids:
        return []

    placeholders = ",".join(["%s"] * len(scan_ids))
    return execute_query(f"""
        SELECT item_id,
               AVG(depth) AS avg_depth,
               AVG(bbp)   AS avg_bbp,
               COUNT(*)   AS sample_count
        FROM snapshots
        WHERE scan_id IN ({placeholders})
          AND depth IS NOT NULL
          AND bbp IS NOT NULL
          AND bbp > 0
        GROUP BY item_id
        HAVING COUNT(*) >= 5
        ORDER BY avg_depth ASC
        LIMIT %s
    """, (*scan_ids, n))


def watchlist_performance(scan_ids: list, watch_items: list) -> list[dict]:
    if len(scan_ids) < 2:
        return []

    first, last = scan_ids[0], scan_ids[-1]
    results = []

    for item in watch_items:
        rows = execute_query("""
            SELECT b.bbp AS bbp_start, a.bbp AS bbp_end
            FROM snapshots a
            JOIN snapshots b ON b.item_id = a.item_id AND b.scan_id = %s
            WHERE a.scan_id = %s AND a.item_id = %s
              AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
        """, (first, last, item))

        if rows:
            r   = rows[0]
            pct = (r["bbp_end"] - r["bbp_start"]) / r["bbp_start"]
            results.append({
                "item_id":    item,
                "bbp_start":  r["bbp_start"],
                "bbp_end":    r["bbp_end"],
                "pct_change": pct,
            })

    return sorted(results, key=lambda r: r["pct_change"], reverse=True)


def market_regime_summary(scan_ids: list) -> str:
    if len(scan_ids) < 4:
        return "Not enough data for regime summary."

    bull_scans = 0
    bear_scans = 0

    for i in range(1, len(scan_ids)):
        prev, curr = scan_ids[i-1], scan_ids[i]
        rows = execute_query("""
            SELECT
              SUM(CASE WHEN a.bbp > b.bbp * 1.01 THEN 1 ELSE 0 END) AS rising,
              SUM(CASE WHEN a.bbp < b.bbp * 0.99 THEN 1 ELSE 0 END) AS falling,
              COUNT(*) AS total
            FROM snapshots a
            JOIN snapshots b ON a.item_id = b.item_id AND b.scan_id = %s
            WHERE a.scan_id = %s
              AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
        """, (prev, curr))

        if rows and rows[0]["total"]:
            r = rows[0]
            if r["rising"] / r["total"] > 0.55:
                bull_scans += 1
            elif r["falling"] / r["total"] > 0.55:
                bear_scans += 1

    total = bull_scans + bear_scans
    if total == 0:
        return "Market was neutral/mixed all week."

    bull_pct = bull_scans / total
    if bull_pct > 0.6:
        return f"📈 Mostly BULLISH ({bull_scans} bull scans vs {bear_scans} bear)"
    elif bull_pct < 0.4:
        return f"📉 Mostly BEARISH ({bear_scans} bear scans vs {bull_scans} bull)"
    else:
        return f"↔️ Mixed week ({bull_scans} bull / {bear_scans} bear — no clear direction)"


def generate_digest(watch_items: list) -> list[str]:
    if get_scan_count() < DIGEST_MIN_SCANS:
        return []

    scan_ids = _scans_last_n_days(7)
    if len(scan_ids) < 2:
        return []

    now_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    messages = []

    messages.append(
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 **WEEKLY DIGEST** — {now_str}\n"
        f"Covering {len(scan_ids)} scans over 7 days\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

    messages.append(f"**Market Regime:**\n{market_regime_summary(scan_ids)}")

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

    gainers, losers = biggest_movers(scan_ids)
    if gainers:
        lines = ["**Biggest Gainers:**"]
        for r in gainers:
            lines.append(
                f"  📈 **{r['item_id']}** +{r['pct_change']:.1%} "
                f"({int(r['bbp_start']):,} → {int(r['bbp_end']):,})"
            )
        messages.append("\n".join(lines))

    if losers:
        lines = ["**Biggest Losers:**"]
        for r in losers:
            lines.append(
                f"  📉 **{r['item_id']}** {r['pct_change']:.1%} "
                f"({int(r['bbp_start']):,} → {int(r['bbp_end']):,})"
            )
        messages.append("\n".join(lines))

    volatile = most_volatile(scan_ids)
    if volatile:
        lines = ["**Most Volatile:**"]
        for r in volatile:
            rng_pct = r["range_bbp"] / r["avg_bbp"] if r["avg_bbp"] else 0
            lines.append(
                f"  ⚠️ **{r['item_id']}** range {rng_pct:.1%} | "
                f"avg BBP {int(r['avg_bbp']):,}"
            )
        messages.append("\n".join(lines))

    thin = thinnest_markets(scan_ids)
    if thin:
        lines = ["**Thinnest Markets:**"]
        for r in thin:
            lines.append(
                f"  🫧 **{r['item_id']}** avg depth {r['avg_depth']:.1f} | "
                f"avg BBP {int(r['avg_bbp']):,}"
            )
        messages.append("\n".join(lines))

    messages.append("━━━━━━━━━━━━━━━━━━━━━━")
    return messages