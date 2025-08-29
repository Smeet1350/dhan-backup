# main.py
from __future__ import annotations

import logging
import os
import sqlite3
import time
import uuid
from typing import Any, Dict, Optional

from fastapi import FastAPI, Query, Header
from fastapi.middleware.cors import CORSMiddleware

# ====== Add your Dhan credentials here (no .env needed) ======
DHAN_CLIENT_ID = "1107860004"
DHAN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU2ODM2NDA4LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwNzg2MDAwNCJ9.3cuzgiY0Qm2Id8wpMW0m90_ZxJ0TJRTV5fZ0tpAwWo3S1Mv5HbpcDNwXxXVepnOUHMRDck_AbArIoVOmlA68Dg"

# Local modules
from scheduler import (
    start_scheduler,
    download_and_populate,
    cleanup_instruments,
    db_is_current,
    symbol_search,
    resolve_symbol,
    ensure_fresh_db,
)
from orders import (
    broker_ready,
    get_funds,
    get_holdings,
    get_positions,
    get_order_list,
    place_order_via_broker,
    cancel_order_via_broker,
    init_broker,
)

# -------- Logging (production-friendly) --------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s.%(msecs)03d %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("backend")
SQLITE_PATH = os.getenv("INSTRUMENTS_DB", "instruments.db")

# -------- FastAPI app --------
app = FastAPI(title="Dhan Automation", version="1.0.0")

# CORS middleware must come first (outermost) and include both localhost variants
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------- Request ID middleware for traceability --------
@app.middleware("http")
async def add_request_id(request, call_next):
    import time, uuid, logging
    LOG = logging.getLogger("backend")
    rid = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    start = time.time()
    response = None
    try:
        response = await call_next(request)
        return response
    except Exception:
        LOG.exception("Unhandled exception | rid=%s", rid)
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": "Internal Server Error", "rid": rid},
        )
    finally:
        elapsed = (time.time() - start) * 1000
        status_code = getattr(response, "status_code", 500)
        LOG.info(
            "access | rid=%s method=%s path=%s status=%s elapsed_ms=%.2f",
            rid, request.method, request.url.path, status_code, elapsed,
        )
        if response:
            response.headers["X-Request-ID"] = rid

# -------- Start scheduler (08:00 IST download / 15:45 IST cleanup) --------
start_scheduler()

# -------- Ensure instruments DB exists at boot --------
ensure_fresh_db(SQLITE_PATH)  # SQLITE_PATH is already set in your file

# -------- Initialize broker from main.py creds --------
ok, why = init_broker(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
if not ok:
    LOG.error("Broker init failed: %s", why)

LOG.info("Backend boot complete")

# =======================
# Health / status
# =======================
@app.get("/status")
def api_status():
    ok_db = db_is_current(SQLITE_PATH)
    ok_broker, why = broker_ready()
    status = "ok" if ok_broker else "degraded"
    msg = "Backend running"
    if not ok_db:
        msg += " (instrument DB may be outdated)"
    if not ok_broker:
        msg += f" (broker not ready: {why})"
    return {
        "status": status,
        "message": msg,
        "instruments_db_current_today": ok_db,
        "broker_ready": ok_broker,
        "why": why,
    }

# =======================
# Instrument master helpers
# =======================
@app.get("/symbol-search")
def api_symbol_search(
    query: str = Query(..., min_length=1),
    segment: str = Query(..., regex="^(NSE_EQ|BSE_EQ|NSE_FNO|MCX)$"),
    limit: int = Query(30, ge=1, le=100),
):
    try:
        results = symbol_search(SQLITE_PATH, query, segment, limit)
        return {"status": "success", "results": results}
    except Exception as e:
        LOG.exception("symbol-search failed")
        return {"status": "error", "message": str(e), "results": []}

@app.post("/instruments/download")
def api_download_master():
    try:
        res = download_and_populate(SQLITE_PATH)
        return {"status": "success", **res}
    except Exception as e:
        LOG.exception("manual download failed")
        return {"status": "error", "message": str(e)}

@app.post("/instruments/cleanup")
def api_cleanup_master():
    try:
        res = cleanup_instruments(SQLITE_PATH)
        return {"status": "success", **res}
    except Exception as e:
        LOG.exception("manual cleanup failed")
        return {"status": "error", "message": str(e)}

# =======================
# Funds / holdings / positions / orders
# =======================
@app.get("/funds")
def api_funds():
    return get_funds()

@app.get("/holdings")
def api_holdings():
    return get_holdings()

@app.get("/positions")
def api_positions():
    return get_positions()

@app.get("/orders")
def api_orders():
    return get_order_list()

# =======================
# Place / Cancel order
# =======================
@app.post("/order/place")
def api_place_order(
    symbol: str = Query(..., description="Trading symbol, e.g., TCS"),
    segment: str = Query(..., regex="^(NSE_EQ|BSE_EQ|NSE_FNO|MCX)$"),
    side: str = Query(..., regex="^(BUY|SELL)$"),
    qty: int = Query(..., ge=1),
    order_type: str = Query("MARKET", regex="^(MARKET|LIMIT)$"),
    price: float = Query(0.0, ge=0.0),
    product_type: str = Query("DELIVERY", regex="^(DELIVERY|CNC|INTRADAY)$"),
    validity: str = Query("DAY", regex="^(DAY|IOC)$"),
    security_id: str | None = Query(default=None, description="Optional: pass to skip DB resolve"),
    x_request_id: str | None = Header(default=None, alias="X-Request-ID"),
):
    rid = x_request_id
    
    # Broker guard at the top:
    ok, why = broker_ready()
    if not ok:
        return {"status": "error", "message": f"Broker not ready: {why or 'unknown'}"}
    
    # Resolve guard before using inst["securityId"]:
    if not security_id:
        inst = resolve_symbol(SQLITE_PATH, symbol, segment)
        if not inst:
            sugg = [i["tradingSymbol"] for i in symbol_search(SQLITE_PATH, symbol, segment, 5)] or []
            return {"status":"error","message":f"Symbol not found in instruments: {symbol} ({segment})","suggestions":sugg}
        security_id = str(inst["securityId"])
        lot = int(inst.get("lotSize") or 1)
    else:
        lot = 1  # if sent from UI, we can't know lot here; it's fine

    # Lot check before calling broker:
    if segment in ("NSE_FNO","MCX") and lot > 1 and qty % lot != 0:
        return {"status":"error","message":f"Qty must be multiple of lot size ({lot}) for {segment}"}

    # best-effort lot fetch if not already set
    if not lot or lot == 1:
        try:
            inst = inst or resolve_symbol(SQLITE_PATH, symbol, segment)
            lot = int((inst or {}).get("lotSize") or 1)
        except Exception:
            lot = lot or 1

    if segment in ("NSE_FNO", "MCX") and lot > 1 and (qty % lot != 0):
        return {"status":"error","message":f"Qty must be multiple of lot size ({lot}) for {segment}"}

    # Log the exact intent (so you can correlate with UI and broker):
    LOG.info("➡️ api_place_order | symbol=%s sid=%s seg=%s side=%s qty=%s type=%s price=%s product=%s validity=%s",
             symbol, security_id, segment, side, qty, order_type, price, product_type, validity)

    res = place_order_via_broker(
        security_id=security_id,
        exchange_segment=segment,
        transaction_type=side,
        quantity=qty,
        order_type=order_type,
        price=price,
        product_type=product_type,
        validity=validity,
    )

    payload = {
        "rid": rid,
        "preview": {
            "symbol": symbol, "segment": segment, "side": side, "qty": qty,
            "order_type": order_type, "price": price, "product_type": product_type,
            "validity": validity, "security_id": str(security_id), "lot": lot
        },
        "broker": res,
    }
    if res.get("status") == "success":
        return {"status": "success", **payload}
    else:
        return {"status": "error", "message": res.get("message") or "Order failed", **payload}

@app.post("/order/cancel")
def api_cancel(order_id: str = Query(..., min_length=1)):
    return cancel_order_via_broker(order_id)

# =======================
# Debug endpoint
# =======================
@app.get("/debug/broker")
def debug_broker():
    # Call all four and report their statuses/messages
    from orders import get_funds, get_holdings, get_positions, get_order_list, broker_ready
    ok, why = broker_ready()
    res = {
        "broker_ready": ok,
        "why": why,
        "funds": get_funds(),
        "holdings": get_holdings(),
        "positions": get_positions(),
        "orders": get_order_list(),
    }
    return res

@app.get("/debug/resolve")
def debug_resolve(symbol: str, segment: str):
    inst = resolve_symbol(SQLITE_PATH, symbol, segment)
    like = symbol_search(SQLITE_PATH, symbol, segment, limit=5)
    return {"inst": inst, "suggestions": like}

@app.get("/debug/instruments/count")
def debug_inst_count():
    import os, sqlite3
    if not os.path.exists(SQLITE_PATH):
        return {"exists": False, "rows": 0}
    conn = sqlite3.connect(SQLITE_PATH)
    try:
        c = conn.execute("SELECT COUNT(*) FROM instruments").fetchone()
        return {"exists": True, "rows": int(c[0])}
    finally:
        conn.close()
