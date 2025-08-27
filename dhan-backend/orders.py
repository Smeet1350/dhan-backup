# orders.py
import os
import sqlite3
import logging
from typing import Dict, Any, Optional

from dhanhq import dhanhq

logger = logging.getLogger("orders")
logger.setLevel(logging.INFO)

# Credentials - hardcoded (⚠️ insecure, but what you asked for)
DHAN_CLIENT_ID = "1107860004"       # replace with your client id
DHAN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU2ODM2NDA4LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwNzg2MDAwNCJ9.3cuzgiY0Qm2Id8wpMW0m90_ZxJ0TJRTV5fZ0tpAwWo3S1Mv5HbpcDNwXxXVepnOUHMRDck_AbArIoVOmlA68Dg"  # replace with your access token

# instantiate Dhan client
try:
    dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
    logger.info("Dhan client initialized")
except Exception as e:
    dhan = None
    logger.exception("Failed to init Dhan client: %s", e)

INSTRUMENT_DB_FILE = os.getenv("INSTRUMENT_DB_FILE", "instruments.db")

def resolve_symbol(symbol: str, segment_filter: Optional[str] = None) -> Dict[str, Any]:
    """Return list of possible instrument matches for symbol from DB."""
    symbol_up = symbol.strip().upper()
    if not os.path.exists(INSTRUMENT_DB_FILE):
        return {"status": "error", "message": "instrument master not available"}
    conn = sqlite3.connect(INSTRUMENT_DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    params = (symbol_up,)
    if segment_filter:
        cur.execute("""
            SELECT securityId, raw FROM instruments WHERE UPPER(tradingSymbol)=? AND UPPER(segment)=?
        """, (symbol_up, segment_filter.upper()))
    else:
        cur.execute("SELECT securityId, raw FROM instruments WHERE UPPER(tradingSymbol)=?", params)
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return {"status": "error", "message": f"symbol {symbol_up} not found"}
    results = []
    for r in rows:
        try:
            raw = r["raw"]
            if isinstance(raw, str):
                import json
                raw = json.loads(raw)
        except Exception:
            raw = {}
        results.append({
            "securityId": r["securityId"],
            "tradingSymbol": raw.get("tradingSymbol"),
            "exchange": raw.get("exchange"),
            "segment": raw.get("segment"),
            "expiry": raw.get("expiry"),
            "lotSize": raw.get("lotSize"),
            "raw": raw
        })
    return {"status": "success", "results": results}

def place_order_via_dhan(security_id: str, exchange_segment: str, transaction_type: str, quantity: int,
                         order_type: str = "MARKET", price: float = 0.0, product_type: str = "DELIVERY",
                         validity: str = "DAY", disclosed_qty: int = 0) -> Dict[str, Any]:
    """Place order using dhanhq client. Returns dhanhq response dict or error dict."""
    if dhan is None:
        return {"status": "error", "message": "Dhan client not initialized"}

    try:
        # Map simple field names to dhanhq parameters; adjust if SDK expects different keys
        order = dhan.place_order(
            security_id=str(security_id),
            exchange_segment=exchange_segment,
            transaction_type=transaction_type,
            quantity=int(quantity),
            price=float(price),
            order_type=order_type,
            product_type=product_type,
            validity=validity,
            disclosed_quantity=int(disclosed_qty)
        )
        logger.info("Dhan place_order response: %s", order)
        return order
    except Exception as e:
        logger.exception("Exception placing order: %s", e)
        return {"status": "error", "message": str(e)}

def get_order_list() -> Dict[str, Any]:
    if dhan is None:
        return {"status": "error", "message": "Dhan client not initialized"}
    try:
        res = dhan.get_order_list()
        logger.info("Dhan get_order_list response: %s", res)
        return res
    except Exception as e:
        logger.exception("Exception fetching orders: %s", e)
        return {"status": "error", "message": str(e)}

def cancel_order(order_id: str) -> Dict[str, Any]:
    if dhan is None:
        return {"status": "error", "message": "Dhan client not initialized"}
    try:
        res = dhan.cancel_order(order_id)
        logger.info("Dhan cancel_order response: %s", res)
        return res
    except Exception as e:
        logger.exception("Exception cancelling order: %s", e)
        return {"status": "error", "message": str(e)}

# helper for balance/holdings/positions
def get_funds() -> Dict[str, Any]:
    if dhan is None:
        return {"status": "error", "message": "Dhan client not initialized"}
    try:
        res = dhan.get_fund_limits()
        logger.info("Dhan funds response: %s", res)
        return res
    except Exception as e:
        logger.exception("Exception fetching funds: %s", e)
        return {"status": "error", "message": str(e)}

def get_holdings() -> Dict[str, Any]:
    if dhan is None:
        return {"status": "error", "message": "Dhan client not initialized"}
    try:
        res = dhan.get_holdings()
        logger.info("Dhan holdings response: %s", res)
        return res
    except Exception as e:
        logger.exception("Exception fetching holdings: %s", e)
        return {"status": "error", "message": str(e)}

def get_positions() -> Dict[str, Any]:
    if dhan is None:
        return {"status": "error", "message": "Dhan client not initialized"}
    try:
        res = dhan.get_positions()
        logger.info("Dhan positions response: %s", res)
        return res
    except Exception as e:
        logger.exception("Exception fetching positions: %s", e)
        return {"status": "error", "message": str(e)}
