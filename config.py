import os
import json
from dotenv import load_dotenv

load_dotenv()

API_URL          = "https://api.donut.auction/orders"
DISCORD_WEBHOOK  = os.getenv("DISCORD_WEBHOOK")
CHECK_INTERVAL   = 1800  # 30 minutes
WATCHLIST_FILE   = "watchlist.json"


def load_watchlist() -> tuple[list, dict]:
    """
    Load WATCH_ITEMS and FAIR_VALUES from watchlist.json.
    Falls back to empty defaults if file is missing or malformed.
    Reloaded every cycle so changes take effect without redeployment.
    """
    try:
        with open(WATCHLIST_FILE, "r") as f:
            data = json.load(f)
        items       = data.get("items", [])
        fair_values = data.get("fair_values", {})
        return items, fair_values
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"[config] Could not load watchlist: {e}. Using empty defaults.")
        return [], {}