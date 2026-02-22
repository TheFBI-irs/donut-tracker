import json
import os

FILE = "prices.json"


def load_prices():
    if not os.path.exists(FILE):
        return {}

    with open(FILE, "r") as f:
        return json.load(f)


def save_prices(data):
    with open(FILE, "w") as f:
        json.dump(data, f, indent=2)