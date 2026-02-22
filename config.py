# config.py

# Donut Auction API endpoint
API_URL = "https://api.donut.auction/orders"

# Items we want to track
WATCH_ITEMS = [
    "elytra",
    "netherite_ingot",
    "netherite_block",
    "dragon_head",
    "enchanted_golden_apple"
]

# Alert thresholds
PRICE_CHANGE_THRESHOLD = 0.05   # 5%
VOLATILITY_THRESHOLD = 0.12     # 12%
CHECK_INTERVAL = 1800           # 30 minutes (API limit)

FAIR_VALUES = {
    "elytra": 280000000,
    "netherite_ingot": 4300000,
    "netherite_block": 38000000,
    "dragon_head": 26000000,
    "enchanted_golden_apple": 620000,
}