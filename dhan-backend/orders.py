# orders.py
from __future__ import annotations

import json
import logging
import os
import sys
from typing import Dict, Any, Optional, Tuple

from dhanhq import dhanhq
from utils import call_with_timeout

logger = logging.getLogger("orders")
if not logger.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s orders: %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.DEBUG if os.getenv("LOG_LEVEL", "INFO").upper() == "DEBUG" else logging.INFO)

_dhan: Optional[dhanhq] = None
_dhan_ready: bool = False
_dhan_error: str = ""

# Expect a function in main.py that returns a configured dhanhq client
try:
    from main import get_dhan_client
except Exception:
    get_dhan_client = None

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


# --- Broker init ---
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


# --- Normalization ---
ERROR_HINTS = {
    "DH-906": "Exchange segment not enabled on your Dhan account. Enable NSE F&O (or place only Equity orders)."
}

def with_hints(payload):
    try:
        code = (
            payload.get("broker", {}).get("remarks", {}).get("error_code")
            or payload.get("broker", {}).get("data", {}).get("errorCode")
        )
        if code in ERROR_HINTS and payload.get("status") == "error":
            payload["message"] = f'{payload.get("message")} — {ERROR_HINTS[code]}'
    except Exception:
        pass
    return payload

def normalize_response(res, success_msg="Success", error_msg="Error"):
    """
    Normalize Dhan SDK response into consistent shape.
    Always returns: {status, message, broker, data}
    """
    try:
        # Exceptions
        if isinstance(res, Exception):
            import traceback
            return {
                "status": "error",
                "message": str(res) or error_msg,
                "broker": {
                    "raw": str(res),
                    "trace": traceback.format_exc()
                },
                "data": None
            }

        # Strings
        if isinstance(res, str):
            try:
                res = json.loads(res)
            except Exception:
                return {"status": "error", "message": res,
                        "broker": {"raw": res}, "data": None}

        # Lists
        if isinstance(res, list):
            return {"status": "success", "message": success_msg,
                    "broker": {"raw": res}, "data": res}

        # Dicts
        if isinstance(res, dict):
            raw = res.copy()
            
            # Friendly message for segment activation errors (DH-906)
            err_code = str(
                (raw.get("remarks") or {}).get("error_code") or
                (raw.get("data") or {}).get("errorCode") or ""
            ).upper()

            if err_code == "DH-906":
                seg = (raw.get("request") or {}).get("exchange_segment") or (raw.get("data") or {}).get("exchange_segment") or ""
                seg_str = f" ({seg})" if seg else ""
                return {"status": "error",
                        "message": f"Exchange segment not activated{seg_str}. Enable it in Dhan or switch segment.",
                        "broker": raw, "data": raw.get("data")}
            
            if str(raw.get("status", "")).lower() == "success" \
               or "orderId" in raw \
               or ("data" in raw and isinstance(raw["data"], dict) and "orderId" in raw["data"]):
                return {"status": "success",
                        "message": raw.get("message") or success_msg,
                        "broker": raw, "data": raw.get("data", raw)}
            msg = (raw.get("remarks", {}).get("error_message")
                   or raw.get("data", {}).get("errorMessage")
                   or raw.get("message")
                   or error_msg)
            result = {"status": "error", "message": str(msg),
                    "broker": raw, "data": raw.get("data")}
            return with_hints(result)

        result = {"status": "error", "message": str(res),
                "broker": {"raw": str(res)}, "data": None}
        return with_hints(result)
    except Exception as e:
        result = {"status": "error", "message": f"Normalization failed: {e}",
                "broker": {"raw": str(res)}, "data": None}
        return with_hints(result)


# --- Order placement ---
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
    timeout: int = 15,
):
    """Place an order using the Dhan SDK, with a timeout to avoid indefinite blocking.
    Callers should offload this function to a threadpool when used from async contexts.
    """
    ok, why = broker_ready()
    if not ok:
        return RuntimeError(f"Broker not ready: {why}")

    try:
        ex_const = getattr(_dhan, EX_SEG_MAP[segment])
        payload = {
            "security_id": str(security_id),
            "exchange_segment": ex_const,
            "transaction_type": SIDE_MAP[side],
            "quantity": int(qty),
            "order_type": ORDTYPE_MAP[order_type],
            "product_type": PRODUCT_MAP[product_type],
            "validity": VALIDITY_MAP[validity],
            "price": 0.0 if order_type == "MARKET" else float(price),
            "disclosed_quantity": int(disclosed_qty or 0),
        }
        logger.info("Placing order: payload=%s", payload)
        
        # Wrap the SDK call with timeout
        def _inner():
            return _dhan.place_order(**payload)
        
        return call_with_timeout(_inner, timeout=timeout)
    except Exception as e:
        logger.exception("place_order_via_broker failed")
        return e


# --- Safe wrapper for other SDK calls ---
def _safe_call(method_name: str, *args, timeout: int = 10, **kwargs):
    ok, why = broker_ready()
    if not ok:
        return RuntimeError(f"Broker not ready: {why}")
    try:
        method = getattr(_dhan, method_name, None)
        if not method:
            raise AttributeError(f"Dhan SDK missing method: {method_name}")
        
        def _inner():
            return method(*args, **kwargs)
        
        return call_with_timeout(_inner, timeout=timeout)
    except Exception as e:
        logger.exception("SDK call %s failed", method_name)
        return e


# --- API-facing wrappers ---
def get_funds():
    return _safe_call("get_fund_limits")

def get_holdings():
    return _safe_call("get_holdings")

def get_positions():
    return _safe_call("get_positions")

def get_orders():
    return _safe_call("get_order_list")

def cancel_order(order_id: str):
    if not order_id:
        return ValueError("order_id is required")
    return _safe_call("cancel_order", order_id)

# --- Compatibility shims expected by main.py ---
def get_order_list():
    return get_orders()

def cancel_order_via_broker(order_id: str):
    return cancel_order(order_id)
