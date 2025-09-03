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
    # Example 1: BANKNIFTY CE buy with strike rounding (45987 -> 46000)
    trade1 = {
        "index": "BANKNIFTY",
        "strike": 45987,  # Will be rounded to 46000
        "option_type": "CE",
        "side": "BUY",
        "lots": 1,  # New lots parameter
        "order_type": "MARKET",
        "price": 0
    }

    # Example 2: NIFTY PE sell with strike rounding (20023 -> 20000)
    trade2 = {
        "index": "NIFTY",
        "strike": 20023,  # Will be rounded to 20000
        "option_type": "PE",
        "side": "SELL",
        "lots": 2,  # 2 lots
        "order_type": "LIMIT",
        "price": 105.5
    }

    # Example 3: BANKNIFTY PE buy (no qty/lots → defaults to 1 lot)
    trade3 = {
        "index": "BANKNIFTY",
        "strike": 33000,
        "option_type": "PE",
        "side": "BUY"
    }

    # Example 4: Backward compatibility with qty parameter
    trade4 = {
        "index": "NIFTY",
        "strike": 20000,
        "option_type": "CE",
        "side": "BUY",
        "qty": 75,  # Old qty parameter still works
        "order_type": "MARKET"
    }

    # Run tests
    for t in [trade1, trade2, trade3, trade4]:
        test_trade(t)
