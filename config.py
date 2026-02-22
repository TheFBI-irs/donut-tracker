import os
from dotenv import load_dotenv

load_dotenv()  # works locally, harmless on Railway

API_URL = "https://api.donut.auction/orders"

WATCH_ITEMS = [
    "elytra",
    "netherite_ingot",
    "netherite_block",
    "dragon_head",
    "enchanted_golden_apple"
]

DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")

CHECK_INTERVAL = 1800
