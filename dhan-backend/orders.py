# orders.py
from __future__ import annotations

import logging
import os
from typing import Dict, Any, Optional, Tuple

from dhanhq import dhanhq

# Ensure logging is set up
import logging

logger = logging.getLogger("orders")
logger.setLevel(logging.INFO)   # or DEBUG if you want verbose

# --- replace the whole client init block with this ---
from typing import Tuple

_dhan: Optional[dhanhq] = None
_dhan_ready: bool = False
_dhan_error: str = ""

def init_broker(client_id: str, access_token: str) -> Tuple[bool, str]:
    """Initialize the Dhan client from main.py (no .env needed)."""
    global _dhan, _dhan_ready, _dhan_error
    try:
        if not client_id or not access_token:
            raise RuntimeError("Missing DHAN credentials")
        _dhan = dhanhq(client_id, access_token)
        _dhan_ready = True
        _dhan_error = ""
        logger.info("✅ Dhan client initialized (via main.py)")
        return True, ""
    except Exception as e:
        _dhan = None
        _dhan_ready = False
        _dhan_error = str(e)
        logger.exception("❌ Failed to init Dhan client")
        return False, _dhan_error

INSTRUMENT_DB_FILE = os.getenv("INSTRUMENT_DB_FILE", "instruments.db")

def broker_ready() -> Tuple[bool, str]:
    """Return (is_ready, reason)."""
    return _dhan_ready, _dhan_error

# ========= Mapping helpers =========
EX_SEG_MAP = {
    "NSE_EQ": "NSE", "NSE": "NSE",
    "BSE_EQ": "BSE", "BSE": "BSE",
    "NSE_FNO": "NSE_FNO",
    "MCX": "MCX",
}
TXN_MAP = {"BUY": "BUY", "SELL": "SELL"}
ORDERTYPE_MAP = {"MARKET": "MARKET", "LIMIT": "LIMIT"}
PRODUCT_MAP = {"DELIVERY": "CNC", "CNC": "CNC", "INTRADAY": "INTRADAY"}

# ========= Safe wrapper =========
def _wrap_call(name: str, func) -> Dict[str, Any]:
    if not _dhan:
        return {"status": "error", "message": "Dhan client not initialized"}
    try:
        res = func()
        logger.info("✅ %s fetched", name)
        return {"status": "success", name: res}
    except Exception as e:
        logger.exception("❌ %s failed", name)
        return {"status": "error", "message": str(e)}

# ========= Public API wrappers =========
def get_funds() -> Dict[str, Any]:
    if not _dhan:
        return {"status": "error", "message": "Dhan client not initialized", "funds": {}}
    try:
        res = _dhan.get_fund_limits()
        logger.info("✅ funds raw: %s", res)
        data = (res or {}).get("data") or {}
        return {"status": "success", "funds": data}
    except Exception as e:
        logger.exception("❌ get_funds failed")
        return {"status": "error", "message": str(e), "funds": {}}

def get_holdings() -> Dict[str, Any]:
    if not _dhan:
        return {"status": "error", "message": "Dhan client not initialized", "holdings": []}
    try:
        res = _dhan.get_holdings()
        logger.info("✅ holdings raw: %s", res)
        data = (res or {}).get("data") or []
        return {"status": "success", "holdings": data}
    except Exception as e:
        logger.exception("❌ get_holdings failed")
        return {"status": "error", "message": str(e), "holdings": []}

def get_positions() -> Dict[str, Any]:
    if not _dhan:
        return {"status": "error", "message": "Dhan client not initialized", "positions": []}
    try:
        res = _dhan.get_positions()
        logger.info("✅ positions raw: %s", res)
        data = (res or {}).get("data") or []
        return {"status": "success", "positions": data}
    except Exception as e:
        logger.exception("❌ get_positions failed")
        return {"status": "error", "message": str(e), "positions": []}

def get_order_list() -> Dict[str, Any]:
    if not _dhan:
        return {"status": "error", "message": "Dhan client not initialized", "orders": []}
    try:
        # orders list
        res = _dhan.get_order_list()
        orders = (res or {}).get("data") or []
        return {"status": "success", "orders": orders}
    except Exception as e:
        logger.exception("❌ get_order_list failed")
        return {"status": "error", "message": str(e), "orders": []}

def cancel_order_via_broker(order_id: str) -> Dict[str, Any]:
    if not _dhan:
        return {"status": "error", "message": "Dhan client not initialized"}
    try:
        logger.info("🛑 Cancel order | order_id=%s", order_id)
        # cancel
        return _dhan.cancel_order(order_id)
    except Exception as e:
        logger.exception("💥 cancel_order exception")
        return {"status": "error", "message": str(e)}

# ========= Place order =========
def place_order_via_broker(
    security_id: str,
    exchange_segment: str,
    transaction_type: str,
    quantity: int,
    order_type: str = "MARKET",
    price: float = 0.0,
    product_type: str = "DELIVERY",
    validity: str = "DAY",
    disclosed_qty: int = 0,
) -> Dict[str, Any]:
    if not _dhan:
        return {"status": "error", "message": "Dhan client not initialized"}

    try:
        # 1) Map to SDK constants (NOT raw strings)
        exseg = exchange_segment.upper()
        if   exseg in ("NSE_EQ", "NSE"): exseg_const = _dhan.NSE
        elif exseg in ("BSE_EQ", "BSE"): exseg_const = _dhan.BSE
        elif exseg == "NSE_FNO":         exseg_const = _dhan.NSE_FNO
        elif exseg == "MCX":             exseg_const = _dhan.MCX
        else:
            return {"status": "error", "message": f"Unsupported segment: {exchange_segment}"}

        txn_const   = _dhan.BUY if transaction_type.upper() == "BUY" else _dhan.SELL
        otype_const = _dhan.MARKET if order_type.upper() == "MARKET" else _dhan.LIMIT

        # Dhan SDK uses CNC (delivery) and INTRA (intraday)
        prod = product_type.upper()
        prod_const = _dhan.CNC if prod in ("DELIVERY", "CNC") else getattr(_dhan, "INTRADAY", getattr(_dhan, "INTRA"))

        logger.info(
            "📤 place_order mapped | sid=%s exseg=%s side=%s qty=%s type=%s price=%s product=%s validity=%s",
            security_id, exseg_const, txn_const, quantity, otype_const, price, prod_const, validity
        )

        # Then call the official method:
        resp = _dhan.place_order(
            security_id=str(security_id),
            exchange_segment=exseg_const,
            transaction_type=txn_const,
            quantity=int(quantity),
            order_type=otype_const,
            product_type=prod_const,
            price=float(price or 0.0),
            validity=validity.upper(),            # "DAY"/"IOC"
            disclosed_quantity=int(disclosed_qty or 0),
        )

        if resp and resp.get("status") == "success":
            logger.info("✅ Order success: %s", resp)
            return resp
        else:
            logger.error("❌ Order failed: %s", resp)
            return resp or {"status": "error", "message": "Empty response from Dhan"}

    except Exception as e:
        logger.exception("💥 place_order exception")
        return {"status": "error", "message": str(e)}
