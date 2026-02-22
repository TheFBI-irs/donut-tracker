"""
positions.py — Personal position tracker.

Tracks what you bought, at what price, and reports unrealized P&L
each scan based on current BBP.

HOW TO ADD A POSITION:
  Edit positions.json and add an entry:
  {
    "item":         "elytra",
    "quantity":     1,
    "bought_price": 265000000,
    "bought_at":    "2026-02-22T06:00:00Z",
    "note":         "optional note"
  }

The bot will report your P&L every scan automatically.
No redeployment needed — just edit the file and push to Railway.
"""

import json
import os
import logging

logger = logging.getLogger(__name__)

POSITIONS_FILE = "positions.json"


def load_positions() -> list:
    if not os.path.exists(POSITIONS_FILE):
        return []
    try:
        with open(POSITIONS_FILE, "r") as f:
            data = json.load(f)
        return data.get("positions", [])
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Could not load positions: {e}")
        return []


def report_positions(current_prices: dict) -> list[str]:
    """
    current_prices: { item_id: bbp } from the current scan.
    Returns a list of alert strings — one per open position.
    """
    positions = load_positions()
    if not positions:
        return []

    alerts = ["💼 **POSITIONS:**"]
    total_cost  = 0
    total_value = 0

    for pos in positions:
        item        = pos.get("item")
        quantity    = pos.get("quantity", 1)
        cost_each   = pos.get("bought_price", 0)
        note        = pos.get("note", "")
        current_bbp = current_prices.get(item)

        cost_total = cost_each * quantity

        if current_bbp is None:
            alerts.append(
                f"  • **{item}** ×{quantity} | bought @ {cost_each:,} | "
                f"no current bid data"
            )
            continue

        value_total   = current_bbp * quantity
        pnl           = value_total - cost_total
        pnl_pct       = (pnl / cost_total * 100) if cost_total else 0
        pnl_sign      = "+" if pnl >= 0 else ""
        pnl_emoji     = "📈" if pnl >= 0 else "📉"

        total_cost  += cost_total
        total_value += value_total

        note_str = f" | {note}" if note else ""
        alerts.append(
            f"  {pnl_emoji} **{item}** ×{quantity} | "
            f"bought {cost_each:,} | now ~{int(current_bbp):,} | "
            f"P&L: {pnl_sign}{int(pnl):,} ({pnl_sign}{pnl_pct:.1f}%)"
            f"{note_str}"
        )

    # Portfolio total if multiple positions
    if len(positions) > 1 and total_cost > 0:
        total_pnl     = total_value - total_cost
        total_pnl_pct = (total_pnl / total_cost * 100)
        sign          = "+" if total_pnl >= 0 else ""
        alerts.append(
            f"  {'📈' if total_pnl >= 0 else '📉'} **TOTAL** | "
            f"cost {int(total_cost):,} | value ~{int(total_value):,} | "
            f"P&L: {sign}{int(total_pnl):,} ({sign}{total_pnl_pct:.1f}%)"
        )

    return alerts