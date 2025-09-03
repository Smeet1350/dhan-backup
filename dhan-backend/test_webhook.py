# test_webhook.py
import requests
import json

BASE_URL = "http://127.0.0.1:8000"

def test_trade(payload):
    url = f"{BASE_URL}/webhook/trade"
    print(f"\n>>> Sending to {url}")
    print("Payload:", json.dumps(payload, indent=2))
    try:
        r = requests.post(url, json=payload, timeout=30)
        print("Status:", r.status_code)
        print("Response:", json.dumps(r.json(), indent=2))
    except Exception as e:
        print("❌ Request failed:", e)


if __name__ == "__main__":
    # Example 1: BANKNIFTY CE buy (using actual strike from DB)
    trade1 = {
        "index": "BANKNIFTY",
        "strike": 31500,
        "option_type": "CE",
        "side": "BUY",
        "qty": 35,
        "order_type": "MARKET",
        "price": 0
    }

    # Example 2: NIFTY PE sell (limit order, 2 lots - using actual strike)
    trade2 = {
        "index": "NIFTY",
        "strike": 20000,
        "option_type": "PE",
        "side": "SELL",
        "qty": 150,
        "order_type": "LIMIT",
        "price": 105.5
    }

    # Example 3: BANKNIFTY PE buy (no qty → defaults to 1 lot)
    trade3 = {
        "index": "BANKNIFTY",
        "strike": 33000,
        "option_type": "PE",
        "side": "BUY"
    }

    # Run tests
    for t in [trade1, trade2, trade3]:
        test_trade(t)
