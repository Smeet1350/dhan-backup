# main.py
from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sqlite3
import time
import uuid
from typing import Any, Dict, Optional

from fastapi import FastAPI, Query, Header
from fastapi.middleware.cors import CORSMiddleware

# ====== Dhan credentials (prefer env vars, fallback to hardcoded) ======
# Prefer env vars; fall back to current values (so behaviour doesn't break instantly)
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "1107860004")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", os.getenv("DHAN_ACCESS_TOKEN_FALLBACK", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU5NDI4NjYwLCJpYXQiOjE3NTY4MzY2NjAsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA3ODYwMDA0In0.ItPA3IuAidky2QpjG89uD0S60ysgAURoEDhaNrirzc6e1JENEbh3rij9wRPXgDjE_1Lkoovo5Qw5cCjLevRzhg"))

from scheduler import (
    start_scheduler,
    download_and_populate,
    cleanup_instruments,
    db_is_current,
    symbol_search,
    resolve_symbol,
    ensure_fresh_db,
)
from webhook import router as webhook_router
from orders import (
    broker_ready,
    get_funds,
    get_holdings,
    get_positions,
    get_order_list,
    place_order_via_broker,
    cancel_order_via_broker,
    init_broker,
    normalize_response,
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s.%(msecs)03d %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("backend")

# Setup alerts logging with daily rotation
handler = TimedRotatingFileHandler("alerts.log", when="midnight", backupCount=7)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
handler.setFormatter(formatter)

alerts_logger = logging.getLogger("alerts")
alerts_logger.setLevel(logging.INFO)
alerts_logger.addHandler(handler)

from config import SQLITE_PATH

app = FastAPI(title="Dhan Automation", version="1.0.0")

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

app.include_router(webhook_router)

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

# --- safer startup / shutdown flow (prevents duplicate schedulers on reload) ---
from dotenv import load_dotenv
load_dotenv()  # optional .env support (python-dotenv in requirements)

db_fresh = False

@app.on_event("startup")
def app_startup():
    global db_fresh
    LOG.info("Starting app startup tasks")

    # 1) Start scheduler (store on app.state so shutdown can access it)
    try:
        app.state.scheduler = start_scheduler(SQLITE_PATH)
        LOG.info("Scheduler started (app.state.scheduler)")
    except Exception:
        LOG.exception("Failed to start scheduler")

    # 2) Ensure instruments DB is fresh (may trigger download)
    try:
        db_fresh = ensure_fresh_db(SQLITE_PATH)
        if not db_is_current(SQLITE_PATH):
            LOG.warning("Instrument DB outdated — forcing fresh download now")
            download_and_populate(SQLITE_PATH)
    except Exception:
        LOG.exception("DB freshness check failed")

    # 3) Init broker (read creds from env first, fallback to current constants)
    client_id = os.getenv("DHAN_CLIENT_ID", DHAN_CLIENT_ID)
    access_token = os.getenv("DHAN_ACCESS_TOKEN", DHAN_ACCESS_TOKEN)
    ok, why = init_broker(client_id, access_token)
    if not ok:
        LOG.error("Broker init failed at startup: %s", why)
    else:
        LOG.info("Broker initialized at startup")

    LOG.info("Backend startup complete")

@app.on_event("shutdown")
def app_shutdown():
    LOG.info("Shutdown event triggered")
    # Shutdown scheduler if present
    sched = getattr(app.state, "scheduler", None)
    if sched:
        try:
            sched.shutdown(wait=False)
            LOG.info("Scheduler shut down cleanly")
        except Exception:
            LOG.exception("Scheduler shutdown failed")

@app.get("/status")
def api_status():
    ok_db = db_is_current(SQLITE_PATH)
    ok_broker, why = broker_ready()
    status = "ok" if ok_broker else "degraded"
    msg = "Backend running"
    if not ok_db:
        msg += " (instrument DB may be outdated)"
    if db_fresh:
        msg += " (Instrument DB auto-refreshed)"
    if not ok_broker:
        msg += f" (broker not ready: {why})"
    return {
        "status": status,
        "message": msg,
        "instruments_db_current_today": ok_db,
        "broker_ready": ok_broker,
        "why": why,
    }

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

@app.get("/resolve-symbol")
def api_resolve_symbol(
    symbol: str = Query(..., min_length=1),
    segment: str = Query(..., regex="^(NSE_EQ|BSE_EQ|NSE_FNO|MCX)$"),
):
    try:
        inst = resolve_symbol(SQLITE_PATH, symbol, segment)
        if not inst:
            like = [i["tradingSymbol"] for i in symbol_search(SQLITE_PATH, symbol, segment, limit=5)]
            return {"status": "error", "message": f"Symbol not found: {symbol} ({segment})", "suggestions": like}
        return {"status": "success", "inst": inst}
    except Exception as e:
        LOG.exception("resolve-symbol failed")
        return {"status": "error", "message": str(e)}

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

@app.get("/funds")
def api_funds():
    return normalize_response(get_funds(), success_msg="Funds retrieved", error_msg="Funds fetch failed")

@app.get("/holdings")
def api_holdings():
    return normalize_response(get_holdings(), success_msg="Holdings retrieved", error_msg="Holdings fetch failed")

@app.get("/positions")
def api_positions():
    return normalize_response(get_positions(), success_msg="Positions retrieved", error_msg="Positions fetch failed")

@app.get("/orders")
def api_orders():
    return normalize_response(get_order_list(), success_msg="Orders retrieved", error_msg="Orders fetch failed")

@app.post("/order/place")
def api_place_order(
    symbol: str = Query(..., description="Trading symbol, e.g., TCS"),
    segment: str = Query(..., regex="^(NSE_EQ|BSE_EQ|NSE_FNO|MCX)$"),
    side: str = Query(..., regex="^(BUY|SELL)$"),
    qty: int = Query(0, ge=0),
    lots: int = Query(0, ge=0, description="Optional: number of lots (preferred). Backend computes qty = lots * lotSize"),
    order_type: str = Query("MARKET", regex="^(MARKET|LIMIT)$"),
    price: float = Query(0.0, ge=0.0),
    product_type: str = Query("DELIVERY", regex="^(DELIVERY|CNC|INTRADAY|INTRA)$"),
    validity: str = Query("DAY", regex="^(DAY|IOC)$"),
    security_id: str | None = Query(default=None, description="Optional: pass to skip DB resolve"),
    disclosed_qty: int = Query(0, ge=0),
    x_request_id: str | None = Header(default=None, alias="X-Request-ID"),
):
    rid = x_request_id or str(uuid.uuid4())[:8]
    t0 = time.time()
    LOG.info("(%s) /order/place start", rid)

    ok, why = broker_ready()
    if not ok:
        return {"status": "error", "message": f"Broker not ready: {why or 'unknown'}"}

    LOG.debug("(%s) input symbol=%s segment=%s qty=%s lots=%s order_type=%s price=%s product=%s validity=%s security_id=%s",
              rid, symbol, segment, qty, lots, order_type, price, product_type, validity, security_id)

    inst = None
    # Resolve instrument if security_id not provided
    if not security_id:
        inst = resolve_symbol(SQLITE_PATH, symbol, segment)
        if not inst:
            like = [i["tradingSymbol"] for i in symbol_search(SQLITE_PATH, symbol, segment, limit=5)]
            LOG.warning("(%s) resolve failed for symbol=%s segment=%s", rid, symbol, segment)
            return {"status": "error", "rid": rid,
                    "message": f"Symbol not found: {symbol} ({segment})",
                    "suggestions": like}
        security_id = str(inst["securityId"])
    else:
        # try to fetch instrument record by security_id for lotSize if possible
        try:
            with sqlite3.connect(SQLITE_PATH) as conn:
                cur = conn.execute(
                    "SELECT securityId, tradingSymbol, segment, lotSize, expiry FROM instruments WHERE securityId = ? LIMIT 1",
                    (str(security_id),)
                ).fetchone()
                if cur:
                    cols = ["securityId", "tradingSymbol", "segment", "lotSize", "expiry"]
                    inst = dict(zip(cols, cur))
        except Exception:
            LOG.exception("(%s) DB lookup by security_id failed", rid)

    lot = int((inst or {}).get("lotSize") or 1)

    # --- Quantity resolution ---
    if segment in ("NSE_FNO", "MCX"):
        # FNO/MCX: qty must always come from lots × lotSize
        if lots and int(lots) > 0:
            computed_qty = int(lots) * lot
        else:
            # fallback: if qty given and valid multiple, use it; else default 1 lot
            if qty and qty % lot == 0:
                computed_qty = int(qty)
                lots = computed_qty // lot
            else:
                computed_qty = lot
                lots = 1
    else:
        # Equity/BSE_EQ: use qty directly, or fallback 1
        computed_qty = int(qty or 0)
        if computed_qty <= 0:
            computed_qty = 1

    LOG.info("(%s) Final qty resolved: lots=%s lotSize=%s -> qty=%s", rid, lots, lot, computed_qty)

    # If user mistakenly uses DELIVERY/CNC for FNO/MCX, override product to intraday but report it
    forced_product = None
    if segment in ("NSE_FNO", "MCX") and product_type in ("DELIVERY", "CNC"):
        forced_product = "INTRADAY"
        LOG.info("(%s) Forcing product_type %s -> %s for segment %s", rid, product_type, forced_product, segment)
        product_type = forced_product

    LOG.debug("(%s) calling place_order_via_broker sid=%s qty=%s", rid, security_id, computed_qty)
    raw_res = place_order_via_broker(
        security_id=security_id,
        segment=segment,
        side=side,
        qty=computed_qty,
        order_type=order_type,
        price=None if order_type == "MARKET" else price,
        product_type=product_type,
        validity=validity,
        symbol=symbol,
        disclosed_qty=disclosed_qty,
    )
    normalized = normalize_response(raw_res, success_msg="Order placed successfully", error_msg="Order rejected")
    elapsed = time.time() - t0
    from fastapi.responses import JSONResponse
    preview = {
            "symbol": symbol, "segment": segment, "side": side, "qty": int(computed_qty),
            "order_type": order_type, "price": float(price or 0),
            "product_type": product_type, "validity": validity,
            "security_id": str(security_id), "lot": lot, "lots": int(lots or 0),
            "qty_calc": f"{lots} × {lot} = {computed_qty}" if lots else str(computed_qty)
    }
    if forced_product:
        preview["forced_product_type"] = forced_product

    return JSONResponse(content={
        "rid": rid,
        **normalized,
        "preview": preview,
        "elapsed_s": elapsed,
    })

@app.post("/order/cancel")
def api_cancel(order_id: str = Query(..., min_length=1)):
    raw_res = cancel_order_via_broker(order_id)
    return normalize_response(raw_res, success_msg="Order cancelled", error_msg="Cancel failed")

@app.post("/order/place-simple")
def api_place_order_simple(
    security_id: str = Query(..., description="Exact Dhan security id"),
    segment: str = Query(..., regex="^(NSE_EQ|BSE_EQ|NSE_FNO|MCX)$"),
    side: str = Query(..., regex="^(BUY|SELL)$"),
    qty: int = Query(..., ge=1),
    order_type: str = Query("MARKET", regex="^(MARKET|LIMIT)$"),
    price: float = Query(0.0, ge=0.0),
    product_type: str = Query("DELIVERY", regex="^(DELIVERY|CNC|INTRADAY)$"),
    validity: str = Query("DAY", regex="^(DAY|IOC)$"),
):
    ok, why = broker_ready()
    if not ok:
        return {"status": "error", "message": f"Broker not ready: {why or 'unknown'}"}

    res = place_order_via_broker(
        security_id=security_id,
        segment=segment,
        side=side,
        qty=qty,
        order_type=order_type,
        price=price,
        product_type=product_type,
        validity=validity,
        symbol="",
        disclosed_qty=0,
    )
    return normalize_response(res, success_msg="Order placed successfully", error_msg="Order rejected")

@app.get("/debug/broker")
def debug_broker():
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
    with sqlite3.connect(SQLITE_PATH) as conn:
        c = conn.execute("SELECT COUNT(*) FROM instruments").fetchone()
        return {"exists": True, "rows": int(c[0])}

@app.get("/debug/segments")
def debug_segments():
    import sqlite3
    with sqlite3.connect(SQLITE_PATH) as conn:
        rows = conn.execute("SELECT DISTINCT segment, COUNT(*) FROM instruments GROUP BY segment").fetchall()
        return [{"segment": r[0], "count": r[1]} for r in rows]
