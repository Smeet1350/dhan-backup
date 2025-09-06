# test_webhook_simple.py
import requests
import json

BASE_URL = "http://127.0.0.1:8000"

def test_webhook(payload):
    url = f"{BASE_URL}/webhook/trade"
    print(f"\n>>> Testing webhook: {url}")
    print("Payload:", json.dumps(payload, indent=2))
    try:
        r = requests.post(url, json=payload, timeout=30)
        print("Status:", r.status_code)
        print("Response:", json.dumps(r.json(), indent=2))
        return r.status_code == 200
    except Exception as e:
        print("❌ Request failed:", e)
        return False

def test_equity():
    """Test equity order (should work if NSE_EQ is enabled)"""
    payload = {
        "symbol": "TCS",
        "side": "BUY",
        "quantity": 1,
        "order_type": "MARKET",
        "product_type": "DELIVERY"
    }
    return test_webhook(payload)

def test_options():
    """Test options order (will fail if NSE_FNO not enabled)"""
    payload = {
        "index": "NIFTY",
        "strike": 20000,
        "option_type": "CE",
        "side": "BUY",
        "lots": 1,
        "order_type": "MARKET"
    }
    return test_webhook(payload)

if __name__ == "__main__":
    print("🧪 Testing webhook functionality...")
    
    print("\n1. Testing Equity Order (TCS):")
    equity_success = test_equity()
    
    print("\n2. Testing Options Order (NIFTY):")
    options_success = test_options()
    
    print(f"\n📊 Results:")
    print(f"Equity order: {'✅ SUCCESS' if equity_success else '❌ FAILED'}")
    print(f"Options order: {'✅ SUCCESS' if options_success else '❌ FAILED'}")
    
    if not options_success:
        print("\n💡 Note: Options orders may fail if NSE F&O segment is not activated in your Dhan account")
        print("   Check your Dhan account settings and enable NSE F&O trading")
