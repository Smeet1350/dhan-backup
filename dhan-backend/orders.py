# orders.py
from __future__ import annotations

import logging
import os
import sys
import uuid
from typing import Dict, Any, Optional, Tuple

from dhanhq import dhanhq

logger = logging.getLogger("orders")
if not logger.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s orders: %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.DEBUG if os.getenv("LOG_LEVEL", "INFO").upper() == "DEBUG" else logging.INFO)

_dhan: Optional[dhanhq] = None
_dhan_ready: bool = False
_dhan_error: str = ""

EX_SEG_MAP = {
    "NSE_EQ": "NSE",
    "BSE_EQ": "BSE",
    "NSE_FNO": "NSE_FNO",
    "MCX": "MCX",
}

SIDE_MAP = {"BUY": "BUY", "SELL": "SELL"}
ORDTYPE_MAP = {"MARKET": "MARKET", "LIMIT": "LIMIT"}
PRODUCT_MAP = {
    "DELIVERY": "CNC",     # SDK expects CNC for delivery
    "CNC": "CNC",
    "INTRADAY": "INTRADAY",  # SDK 2.0.2 expects INTRADAY, not INTRA
}
VALIDITY_MAP = {"DAY": "DAY", "IOC": "IOC"}

def init_broker(client_id: str, access_token: str) -> Tuple[bool, str]:
    global _dhan, _dhan_ready, _dhan_error
    try:
        if not client_id or not access_token:
            raise RuntimeError("Missing DHAN credentials")
        _dhan = dhanhq(client_id, access_token)
        _dhan_ready = True
        _dhan_error = ""
        logger.info("Dhan client initialized for client_id=%s", client_id)
        return True, ""
    except Exception as e:
        _dhan = None
        _dhan_ready = False
        _dhan_error = str(e)
        logger.exception("Failed to initialize Dhan client")
        return False, _dhan_error

def broker_ready() -> Tuple[bool, str]:
    return (_dhan_ready and _dhan is not None), _dhan_error

def _map_enum(name: str, val: str, mapping: Dict[str, str]) -> str:
    if not val:
        raise ValueError(f"{name} is empty")
    v = mapping.get(val.upper())
    if not v:
        raise ValueError(f"Unsupported {name}: {val}")
    return v

def place_order_via_broker(
    *,
    security_id: str,
    segment: str,
    side: str,
    qty: int,
    order_type: str,
    price: float | None,
    product_type: str,
    validity: str,
    symbol: str = "",
    disclosed_qty: int | None = 0,

) -> Dict[str, Any]:
    ok, why = broker_ready()
    if not ok:
        return {"status": "error", "message": f"Broker not ready: {why}"}

    try:
        ex_const = getattr(_dhan, EX_SEG_MAP[segment])
        side_const = SIDE_MAP[side]
        ordtype_const = ORDTYPE_MAP[order_type]
        prod_const = PRODUCT_MAP[product_type]
        validity_const = VALIDITY_MAP[validity]

        # Always pass price (SDK 2.0.2 requirement)
        if ordtype_const == "MARKET":
            price_to_send = 0.0
        else:  # LIMIT
            if not price or price <= 0:
                raise ValueError("Limit order requires a positive price")
            price_to_send = float(price)

        payload = {
            "security_id": str(security_id),
            "exchange_segment": ex_const,
            "transaction_type": side_const,
            "quantity": int(qty),
            "order_type": ordtype_const,
            "product_type": prod_const,
            "validity": validity_const,
            "price": price_to_send,
            "disclosed_quantity": int(disclosed_qty or 0),
        }

        logger.info("Placing order payload=%s | symbol=%s", payload, symbol)
        resp = _dhan.place_order(**payload)
        logger.info("Dhan response: %s", resp)

        if resp and isinstance(resp, dict) and "orderId" in resp:
            return {"status": "success", "message": resp.get("message", "Order placed"), "broker": resp}
        
        # error case - extract detailed error message
        err_msg = (
            resp.get("remarks", {}).get("error_message")
            or resp.get("data", {}).get("errorMessage")
            or resp.get("message", "Order rejected")
        )
        return {"status": "error", "message": err_msg, "broker": resp}
    except Exception as e:
        logger.exception("place_order_via_broker failed")
        return {"status": "error", "message": str(e)}

def _safe_call(method_name: str, *args, **kwargs) -> Dict[str, Any]:
    ok, why = broker_ready()
    if not ok:
        msg = f"Broker not ready: {why or 'unknown'}"
        logger.error(msg)
        return {"status": "error", "message": msg}
    try:
        if not hasattr(_dhan, method_name):
            raise AttributeError(f"Dhan SDK missing method: {method_name}")
        logger.debug("Calling SDK: %s args=%s kwargs=%s", method_name, args, kwargs)
        method = getattr(_dhan, method_name)
        resp = method(*args, **kwargs)
        logger.info("SDK %s response: %s", method_name, resp)
        return resp
    except Exception as e:
        logger.exception("SDK call %s failed", method_name)
        return {"status": "error", "message": str(e)}

def get_funds() -> Dict[str, Any]:
    res = _safe_call("get_fund_limits")
    if isinstance(res, dict) and res.get("status") == "success":
        return {"status": "success", "funds": res.get("data", {})}
    return {"status": "error", "funds": {}, "message": res}

def get_holdings() -> Dict[str, Any]:
    res = _safe_call("get_holdings")
    if isinstance(res, dict) and res.get("status") == "success":
        return {"status": "success", "holdings": res.get("data", [])}
    return {"status": "error", "holdings": [], "message": res}

def get_positions() -> Dict[str, Any]:
    res = _safe_call("get_positions")
    if isinstance(res, dict) and res.get("status") == "success":
        return {"status": "success", "positions": res.get("data", [])}
    return {"status": "error", "positions": [], "message": res}

def get_orders() -> Dict[str, Any]:
    res = _safe_call("get_order_list")
    if isinstance(res, dict) and res.get("status") == "success":
        return {"status": "success", "orders": res.get("data", [])}
    return {"status": "error", "orders": [], "message": res}

def cancel_order(order_id: str) -> Dict[str, Any]:
    if not order_id:
        return {"status": "error", "message": "order_id is required"}
    return _safe_call("cancel_order", order_id)

# --- compatibility shims expected by main.py ---
def get_order_list() -> Dict[str, Any]:
    return get_orders()

def cancel_order_via_broker(order_id: str) -> Dict[str, Any]:
    return cancel_order(order_id)
