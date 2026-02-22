import requests
from config import DISCORD_WEBHOOK


def send_alert(message):
    if not DISCORD_WEBHOOK:
        print("No webhook configured.")
        return

    payload = {
        "content": message
    }

    requests.post(DISCORD_WEBHOOK, json=payload)

