"""
bot.py — Interactive Discord bot for on-demand market queries.

Commands:
  !price <item>    — current BBP, demand, depth for any item
  !watchlist       — show all watched items and their current BBP
  !positions       — current P&L on open positions
  !top_movers      — biggest price moves since last scan
  !macro           — latest macro economy signals
  !depth <item>    — order book depth breakdown
  !help            — list all commands

Runs in a background thread alongside the main 30-min scan loop.
Push alerts (scheduled scans) still go via webhook — this handles
on-demand queries only.
"""

import os
import logging
import threading
import discord
from discord.ext import commands

from config import load_watchlist
from tracker import price_history
from positions import report_positions, load_positions
from historian import execute_query, get_scan_count

logger = logging.getLogger(__name__)

COMMAND_PREFIX = "!"

# ---------------------------------------------------------------------------
# Bot setup
# ---------------------------------------------------------------------------

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix=COMMAND_PREFIX, intents=intents)
bot.remove_command("help")  # we define our own


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_latest_bbp(item_id: str) -> dict | None:
    """Pull most recent snapshot for an item from PostgreSQL."""
    rows = execute_query("""
        SELECT sn.bbp, sn.gap_pct, sn.depth, sn.top_bid,
               sn.second_bid, sn.third_bid, s.ts
        FROM snapshots sn
        JOIN scans s ON s.id = sn.scan_id
        WHERE sn.item_id = %s
        ORDER BY sn.scan_id DESC
        LIMIT 1
    """, (item_id,))
    return rows[0] if rows else None


def fmt(n) -> str:
    """Format a number with commas, handle None."""
    if n is None:
        return "N/A"
    return f"{int(n):,}"


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

@bot.event
async def on_ready():
    logger.info(f"Discord bot logged in as {bot.user}")


@bot.command(name="help")
async def help_cmd(ctx):
    msg = (
        "**Donut Tracker Commands:**\n"
        "`!price <item>` — BBP, demand, depth for any item\n"
        "`!watchlist` — all watched items and current BBP\n"
        "`!positions` — current P&L on open positions\n"
        "`!top_movers` — biggest price moves since last scan\n"
        "`!macro` — latest macro economy signals\n"
        "`!depth <item>` — order book depth breakdown\n"
        "`!scans` — how many scans have been recorded\n"
    )
    await ctx.send(msg)


@bot.command(name="price")
async def price_cmd(ctx, *, item_id: str = None):
    if not item_id:
        await ctx.send("Usage: `!price <item_id>` e.g. `!price elytra`")
        return

    item_id = item_id.lower().replace(" ", "_")
    data    = get_latest_bbp(item_id)

    if not data:
        await ctx.send(f"❓ No data found for `{item_id}`. Check the item ID.")
        return

    # Pull calibrated labels from in-memory history if available
    from tracker import describe_demand, describe_depth, compute_gap_pct
    records     = price_history.get(item_id, [])
    gap_pct     = data["gap_pct"]
    depth       = data["depth"] or 0
    demand_lbl  = describe_demand(item_id, gap_pct) if gap_pct else "❓"
    depth_lbl   = describe_depth(item_id, depth)

    top_bids = [data["top_bid"], data["second_bid"], data["third_bid"]]
    top_str  = ", ".join(fmt(b) for b in top_bids if b)

    msg = (
        f"📊 **{item_id}**\n"
        f"BBP: **{fmt(data['bbp'])}** | Top 3: [{top_str}]\n"
        f"Demand: {demand_lbl}\n"
        f"Depth: {depth_lbl}\n"
        f"Last updated: {data['ts']}"
    )
    await ctx.send(msg)


@bot.command(name="watchlist")
async def watchlist_cmd(ctx):
    watch_items, fair_values = load_watchlist()
    if not watch_items:
        await ctx.send("Watch list is empty. Check `watchlist.json`.")
        return

    lines = ["**📋 Watch List:**"]
    for item in watch_items:
        data = get_latest_bbp(item)
        if not data:
            lines.append(f"  ❓ **{item}** — no data")
            continue
        bbp  = fmt(data["bbp"])
        fair = fair_values.get(item)
        fair_str = ""
        if fair and data["bbp"]:
            ratio    = data["bbp"] / fair
            sign     = "+" if ratio >= 1 else ""
            fair_str = f" ({sign}{(ratio-1)*100:.1f}% vs fair)"
        lines.append(f"  • **{item}** BBP: {bbp}{fair_str}")

    await ctx.send("\n".join(lines))


@bot.command(name="positions")
async def positions_cmd(ctx):
    watch_items, _ = load_watchlist()
    current_prices = {}
    for item in watch_items:
        data = get_latest_bbp(item)
        if data and data["bbp"]:
            current_prices[item] = data["bbp"]

    alerts = report_positions(current_prices)
    if not alerts:
        await ctx.send("No open positions. Add entries to `positions.json` to track trades.")
        return

    await ctx.send("\n".join(alerts))


@bot.command(name="top_movers")
async def top_movers_cmd(ctx):
    scan_count = get_scan_count()
    if scan_count < 2:
        await ctx.send("Not enough scan data yet. Check back after a few cycles.")
        return

    rows = execute_query("""
        SELECT a.item_id,
               b.bbp                   AS bbp_old,
               a.bbp                   AS bbp_new,
               (a.bbp - b.bbp) / b.bbp AS pct_change
        FROM snapshots a
        JOIN snapshots b
          ON a.item_id = b.item_id
          AND b.scan_id = (SELECT id FROM scans ORDER BY id DESC LIMIT 1 OFFSET 1)
        WHERE a.scan_id = (SELECT MAX(id) FROM scans)
          AND a.bbp IS NOT NULL AND b.bbp IS NOT NULL
          AND b.bbp > 0
        ORDER BY ABS((a.bbp - b.bbp) / b.bbp) DESC
        LIMIT 10
    """)

    if not rows:
        await ctx.send("No mover data available yet.")
        return

    lines = ["**🏃 Top Movers (last scan):**"]
    for r in rows:
        direction = "▲" if r["pct_change"] > 0 else "▼"
        lines.append(
            f"  {direction} **{r['item_id']}** {abs(r['pct_change']):.1%} "
            f"({fmt(r['bbp_old'])} → {fmt(r['bbp_new'])})"
        )

    await ctx.send("\n".join(lines))


@bot.command(name="macro")
async def macro_cmd(ctx):
    from macro import market_breadth, top_movers, detect_regime_change
    watch_items, _ = load_watchlist()

    results = []

    regime = detect_regime_change()
    if regime:
        results.append(regime)

    breadth = market_breadth()
    if breadth:
        results.append(breadth)

    movers = top_movers(watch_items)
    results.extend(movers)

    if not results:
        results.append("🌐 No macro signals at this time.")

    await ctx.send("\n".join(results))


@bot.command(name="depth")
async def depth_cmd(ctx, *, item_id: str = None):
    if not item_id:
        await ctx.send("Usage: `!depth <item_id>` e.g. `!depth elytra`")
        return

    item_id = item_id.lower().replace(" ", "_")

    # Get last 5 depth readings for trend
    rows = execute_query("""
        SELECT sn.depth, sn.bbp, sn.total_bids_above_floor, s.ts
        FROM snapshots sn
        JOIN scans s ON s.id = sn.scan_id
        WHERE sn.item_id = %s
          AND sn.depth IS NOT NULL
        ORDER BY sn.scan_id DESC
        LIMIT 5
    """, (item_id,))

    if not rows:
        await ctx.send(f"❓ No depth data for `{item_id}`.")
        return

    from tracker import describe_depth
    latest = rows[0]
    depth_lbl = describe_depth(item_id, latest["depth"])

    lines = [f"**📊 Depth history for {item_id}:**"]
    lines.append(f"Current: {depth_lbl} | BBP: {fmt(latest['bbp'])}")
    lines.append("Recent trend (newest first):")
    for r in rows:
        lines.append(f"  • depth {r['depth']} | {r['ts']}")

    await ctx.send("\n".join(lines))


@bot.command(name="scans")
async def scans_cmd(ctx):
    count = get_scan_count()
    hours = count * 0.5
    await ctx.send(
        f"📈 **{count}** scans recorded (~{hours:.0f} hours of history). "
        f"Next macro feature unlocks at 20 scans."
        if count < 20
        else f"📈 **{count}** scans recorded (~{hours:.0f} hours of history)."
    )


# ---------------------------------------------------------------------------
# Run bot in background thread
# ---------------------------------------------------------------------------

def run_bot():
    token = os.getenv("DISCORD_BOT_TOKEN")
    if not token:
        logger.error("DISCORD_BOT_TOKEN not set — interactive bot disabled.")
        return
    try:
        logger.info("Starting Discord bot...")
        bot.run(token)
    except Exception as e:
        logger.error(f"Discord bot failed: {e}. Main scan loop will continue without it.")


def start_bot_thread():
    """Launch bot in a daemon thread so it doesn't block the main scan loop."""
    t = threading.Thread(target=run_bot, daemon=True)
    t.start()
    return t