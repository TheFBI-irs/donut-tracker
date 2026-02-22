# debug.py
import json
import requests

API_URL = "https://api.donut.auction/orders"

response = requests.get(API_URL, timeout=15)
data = response.json()

print(f"Total orders in first page: {len(data['orders'])}")
print(f"nextCursor present: {data.get('nextCursor') is not None}")
print("\n--- First 3 raw orders ---")
for order in data["orders"][:3]:
    print(json.dumps(order, indent=2))