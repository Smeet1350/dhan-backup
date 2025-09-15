# orders.py
from __future__ import annotations

import json
import logging
import os
import sys
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
    "DELIVERY": "CNC",     # Delivery -> CNC in SDK
    "CNC": "CNC",
    # map both INTRADAY and INTRA inputs to the SDK constant 'INTRA' used in examples
    "INTRADAY": "INTRA",
    "INTRA": "INTRA",
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
def normalize_response(res, success_msg="Success", error_msg="Error"):
    """
    Normalize Dhan SDK response into consistent shape.
    Returns: {status, message, broker, data}
    Maps common broker errors to friendly messages (insufficient margin, closed market, restricted).
    """
    try:
        # Exceptions
        if isinstance(res, Exception):
            import traceback
            raw = str(res) or repr(res)
            lower = raw.lower()
            if "insufficient" in lower or "margin" in lower:
                msg = "Insufficient funds / margin"
            elif "restricted" in lower or "not allowed" in lower or "trade restricted" in lower:
                msg = "Trading restricted for this instrument or product"
            elif "closed" in lower or "market is closed" in lower:
                msg = "Market is Closed"
            else:
                msg = raw
            return {
                "status": "error",
                "message": msg,
                "broker": {"raw": raw, "trace": traceback.format_exc()},
                "data": None,
            }

        # Strings -> try json
        if isinstance(res, str):
            try:
                res = json.loads(res)
            except Exception:
                lower = res.lower()
                if "insufficient" in lower or "margin" in lower:
                    return {"status": "error", "message": "Insufficient funds / margin", "broker": {"raw": res}, "data": None}
                if "restricted" in lower or "not allowed" in lower:
                    return {"status": "error", "message": "Trading restricted for this instrument or product", "broker": {"raw": res}, "data": None}
                if "closed" in lower or "market is closed" in lower:
                    return {"status": "error", "message": "Market is Closed", "broker": {"raw": res}, "data": None}
                return {"status": "error", "message": res, "broker": {"raw": res}, "data": None}

        # Lists -> success
        if isinstance(res, list):
            return {"status": "success", "message": success_msg, "broker": {"raw": res}, "data": res}

        # Dicts
        if isinstance(res, dict):
            raw = res.copy()
            # success detection
            if str(raw.get("status", "")).lower() == "success" \
               or "orderId" in raw \
               or ("data" in raw and isinstance(raw["data"], dict) and "orderId" in raw["data"]):
                return {"status": "success", "message": raw.get("message") or success_msg, "broker": raw, "data": raw.get("data", raw)}

            # pick candidate message fields
            candidates = []
            if isinstance(raw.get("remarks"), dict):
                candidates.append(raw["remarks"].get("error_message"))
            if isinstance(raw.get("data"), dict):
                candidates.append(raw["data"].get("errorMessage"))
                candidates.append(raw["data"].get("error_message"))
                candidates.append(raw["data"].get("message"))
            candidates.append(raw.get("message"))
            candidates.append(raw.get("error"))
            msg = next((str(c) for c in candidates if c), None) or error_msg

            low = str(msg).lower()
            if "insufficient" in low or ("margin" in low and "insufficient" in low) or "insufficent" in low:
                msg = "Insufficient funds / margin"
            elif "restricted" in low or ("not allowed" in low and "trade" in low) or "trade restricted" in low:
                msg = "Trading restricted for this instrument or product"
            elif "closed" in low or "market is closed" in low:
                msg = "Market is Closed"
            return {"status": "error", "message": str(msg), "broker": raw, "data": raw.get("data")}

        # fallback
        return {"status": "error", "message": str(res), "broker": {"raw": str(res)}, "data": None}
    except Exception as e:
        return {"status": "error", "message": f"Normalization failed: {e}", "broker": {"raw": str(res)}, "data": None}


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
):
    ok, why = broker_ready()
    if not ok:
        return RuntimeError(f"Broker not ready: {why}")

    try:
        # helper to fetch SDK constant if present, otherwise fall back to raw string
        def _sdk_const(name: str):
            try:
                return getattr(_dhan, name)
            except Exception:
                return name

        ex_const = _sdk_const(EX_SEG_MAP.get(segment, segment))
        tx_const = _sdk_const(SIDE_MAP.get(side, side))
        ordtype_const = _sdk_const(ORDTYPE_MAP.get(order_type, order_type))
        prod_const = _sdk_const(PRODUCT_MAP.get(product_type, product_type))
        valid_const = _sdk_const(VALIDITY_MAP.get(validity, validity))

        # price must always be numeric for the SDK; use 0.0 for market orders
        price_val = 0.0 if (order_type == "MARKET" or price is None) else float(price)

        payload = {
            "security_id": str(security_id),
            "exchange_segment": ex_const,
            "transaction_type": tx_const,
            "quantity": int(qty),
            "order_type": ordtype_const,
            "product_type": prod_const,
            "validity": valid_const,
            "price": float(price_val),
            "disclosed_quantity": int(disclosed_qty or 0),
        }
        logger.info("Placing order: payload=%s", payload)
        try:
            return _dhan.place_order(**payload)
        except TypeError as te:
            # Fallback: some SDK versions expect positional args — try a positional call
            logger.warning("place_order keyword call failed, trying positional fallback: %s", te)
            try:
                return _dhan.place_order(
                    payload["security_id"],
                    payload["exchange_segment"],
                    payload["transaction_type"],
                    payload["quantity"],
                    payload["order_type"],
                    payload["price"],
                    payload["product_type"],
                    payload["validity"],
                    payload["disclosed_quantity"],
                )
            except Exception:
                logger.exception("Fallback positional place_order also failed")
                raise
    except Exception as e:
        logger.exception("place_order_via_broker failed")
        return e


# --- Safe wrapper for other SDK calls ---
def _safe_call(method_name: str, *args, **kwargs):
    ok, why = broker_ready()
    if not ok:
        return RuntimeError(f"Broker not ready: {why}")
    try:
        method = getattr(_dhan, method_name, None)
        if not method:
            raise AttributeError(f"Dhan SDK missing method: {method_name}")
        return method(*args, **kwargs)  # return raw
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
