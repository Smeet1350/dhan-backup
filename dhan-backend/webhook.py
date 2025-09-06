# webhook.py
import json, time, uuid
import os
import logging
import threading
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path

from starlette.concurrency import run_in_threadpool

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from typing import Dict

# import your existing stuff
from config import SQLITE_PATH
from scheduler import ensure_fresh_db
from orders import (
    broker_ready, place_order_via_broker, normalize_response
)

LOG = logging.getLogger("backend")
# keep alert log bounded to avoid unbounded memory growth
ALERTS_LOG = deque(maxlen=500)

router = APIRouter()


def _nonblocking_ensure_db_refresh(db_path: str):
    """Cheap check of DB mtime and spawn background refresh if stale.
    IMPORTANT: This function must NOT perform heavy IO on the request path.
    """
    try:
        p = Path(db_path)
        stale = True
        if p.exists():
            mtime = datetime.fromtimestamp(p.stat().st_mtime)
            # treat DB as stale after 24 hours (adjust if needed)
            if mtime > datetime.now() - timedelta(hours=24):
                stale = False
        if stale:
            def bg():
                try:
                    ensure_fresh_db(db_path)
                except Exception:
                    LOG.exception("Background DB refresh failed")
            t = threading.Thread(target=bg, daemon=True)
            t.start()
    except Exception:
        LOG.exception("Non-blocking DB freshness check failed (ignored)")


async def _parse_tv_request(req: Request) -> Dict:
    """
    Accept JSON, raw JSON text, or form-encoded alert=<json>.
    """
    # 1) application/json
    try:
        return await req.json()
    except Exception:
        pass

    # 2) x-www-form-urlencoded (TradingView "alert" field)
    try:
        form = await req.form()
        if "alert" in form:
            return json.loads(str(form["alert"]))
    except Exception:
        pass

    # 3) raw body as JSON text
    try:
        raw = (await req.body() or b"").decode().strip()
        if raw:
            return json.loads(raw)
    except Exception:
        pass

    return {}

async def _process(req: Request) -> Dict:
    body = await _parse_tv_request(req)
    LOG.info("Webhook payload: %s", body)

    # cheap non-blocking DB refresh check (no heavy work here)
    _nonblocking_ensure_db_refresh(SQLITE_PATH)

    # Allow simple equity payloads as well (symbol/side/quantity)
    is_equity = "symbol" in body

    try:
        ok, why = broker_ready()
        if not ok:
            return {"status": "error", "message": f"Broker not ready: {why}"}

        if is_equity:
            symbol = str(body.get("symbol") or "").strip()
            if not symbol:
                return {"status":"error","message":"Missing symbol"}
            side = str(body.get("side","BUY")).upper()
            qty  = int(body.get("quantity") or body.get("qty") or 1)
            order_type = str(body.get("order_type","MARKET")).upper()
            price = float(body.get("price") or 0)
            product_type = str(body.get("product_type","INTRADAY")).upper()
            validity = str(body.get("validity","DAY")).upper()

            # For equity, we need to find by symbol directly
            from scheduler import resolve_symbol
            # Offload blocking DB lookup to threadpool
            inst = await run_in_threadpool(resolve_symbol, SQLITE_PATH, symbol, "NSE_EQ")
            if not inst or not inst.get("securityId"):
                LOG.warning("Symbol not found: %s", symbol)
                # schedule a background refresh but still respond quickly
                _nonblocking_ensure_db_refresh(SQLITE_PATH)
                return {"status": "error", "message": f"No instrument for {symbol}"}
            segment = inst.get("segment") or "NSE_EQ"

            # Place order via threadpool with internal timeout
            LOG.info("Placing equity order: %s %s %s qty=%d", inst["tradingSymbol"], side, order_type, qty)
            raw_res = await run_in_threadpool(place_order_via_broker,
                security_id=str(inst["securityId"]),
                segment=segment,
                side=side,
                qty=qty,
                order_type=order_type,
                price=None if order_type=="MARKET" else price,
                product_type=product_type,
                validity=validity,
                symbol=inst["tradingSymbol"],
                disclosed_qty=0,
            )
            res = normalize_response(raw_res, success_msg="Equity order placed", error_msg="Equity order failed")
            LOG.info("Equity order result: %s", res.get("status", "unknown"))

        else:
            # Options payload (index/strike/option_type/lots or qty)
            index_symbol = str(body.get("index","")).upper()
            raw_strike = int(body.get("strike") or 0)
            option_type = str(body.get("option_type","")).upper()
            side = str(body.get("side","BUY")).upper()
            order_type = str(body.get("order_type","MARKET")).upper()
            price = float(body.get("price") or 0)
            product_type = str(body.get("product_type","INTRADAY")).upper()
            validity = str(body.get("validity","DAY")).upper()

            if not index_symbol or raw_strike <= 0 or option_type not in ("CE","PE"):
                return {"status":"error","message":"Invalid options payload"}

            # Import helper functions
            from scheduler import resolve_symbol
            
            strike = round_strike(raw_strike, index_symbol)
            # Try multiple symbol formats for options
            trading_symbols = [
                f"{index_symbol}-{strike}-{option_type}",
                f"{index_symbol}{strike}{option_type}",
                f"{index_symbol} {strike} {option_type}",
            ]
            
            inst = None
            for symbol_format in trading_symbols:
                inst = await run_in_threadpool(resolve_symbol, SQLITE_PATH, symbol_format, "NSE_FNO")
                if inst:
                    break
            if not inst:
                LOG.warning("Instrument not found: %s %s%s", index_symbol, strike, option_type)
                _nonblocking_ensure_db_refresh(SQLITE_PATH)
                return {"status": "error", "message": f"No instrument for {index_symbol} {strike}{option_type}"}

            lot_size = int(inst.get("lotSize") or 1)
            lots = int(body.get("lots") or 0)
            qty  = int(body.get("qty") or 0)
            if lots > 0:
                qty = lots * lot_size
            elif qty <= 0:
                qty = lot_size
            elif qty % lot_size != 0:
                return {"status":"error","message":f"Qty {qty} not multiple of lot {lot_size}"}

            segment = inst.get("segment") or "NSE_FNO"
            # Place order via threadpool with internal timeout
            LOG.info("Placing F&O order: %s %s %s qty=%d", inst["tradingSymbol"], side, order_type, qty)
            raw_res = await run_in_threadpool(place_order_via_broker,
                security_id=str(inst["securityId"]),
                segment=segment,
                side=side,
                qty=qty,
                order_type=order_type,
                price=None if order_type=="MARKET" else price,
                product_type=product_type,
                validity=validity,
                symbol=inst["tradingSymbol"],
                disclosed_qty=0,
            )
            res = normalize_response(raw_res, success_msg="F&O order placed", error_msg="F&O order failed")
            LOG.info("F&O order result: %s", res.get("status", "unknown"))

        # append to bounded alerts log (safe in-memory)
        alert_entry = {
            "id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "ts_epoch": int(time.time()*1000),
            "request": body,
            "instrument": {k: res.get("symbol") if k=="tradingSymbol" else v for k,v in ({} if not is_equity else {"tradingSymbol": symbol}).items()},
            "response": res,
        }
        ALERTS_LOG.appendleft(alert_entry)

        status_code = 200 if res.get("status") == "success" else 400
        return res | {"status_code": status_code}

    except Exception as e:
        LOG.exception("Webhook processing failed")
        # Return a safe 500 without leaking internals
        raise HTTPException(status_code=500, detail="internal_error")

# Routes that all hit the same processor
@router.post("/")
async def webhook_root(req: Request):
    out = await _process(req); code = out.pop("status_code", 200)
    return JSONResponse(status_code=code, content=out)

@router.post("/webhook")
async def webhook_compat(req: Request):
    out = await _process(req); code = out.pop("status_code", 200)
    return JSONResponse(status_code=code, content=out)

@router.post("/webhook/trade")
async def webhook_trade(req: Request):
    out = await _process(req); code = out.pop("status_code", 200)
    return JSONResponse(status_code=code, content=out)

# Helper functions for options trading
def round_strike(strike: int, index_symbol: str) -> int:
    """Round strike price to valid trading levels based on index."""
    step = 50 if "NIFTY" in index_symbol.upper() else 100
    return round(strike / step) * step

# Alerts endpoint
@router.get("/webhook/alerts")
async def get_alerts(limit: int = 100):
    """Get recent webhook alerts."""
    limit = max(1, min(limit, 500))
    alerts_list = list(ALERTS_LOG)[:limit] if hasattr(ALERTS_LOG, '__iter__') else []
    return {"status": "success", "alerts": alerts_list}

# Test endpoint
@router.post("/webhook/test")
async def webhook_test(req: Request):
    sample = {
        "index": "NIFTY", "strike": 20000, "option_type": "CE", "side": "BUY",
        "order_type": "MARKET", "lots": 1
    }
    from fastapi import Request
    class MockRequest:
        async def json(self): return sample
        async def form(self): return {}
        async def body(self): return b""
    return await _process(MockRequest())