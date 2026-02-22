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

# Discord webhook URL
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1474894959604793469/w5UvbaICRMnKuyw97VXKI6KrYPYZ7KhNf5hexkXWEJtMDdgYeEGiFxw21OhpyowhGxZu"

# Alert thresholds
PRICE_CHANGE_THRESHOLD = 0.05   # 5%
VOLATILITY_THRESHOLD = 0.12     # 12%
CHECK_INTERVAL = 1800           # 30 minutes (API limit)

FAIR_VALUES = {
    "elytra": 280_000_000,
    "netherite_block": 5_000_000,
    "dragon_head": 90_000_000,
    "enchanted_golden_apple": 3_500_000,
}