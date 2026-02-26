import requests, json

r = requests.get("https://api.donut.auction/orders")
data = r.json()

# Get first order from response
orders = data.get("orders", data.get("data", data if isinstance(data, list) else []))
if orders:
    print(json.dumps(orders[0], indent=2))