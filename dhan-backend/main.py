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
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
from pathlib import Path

# ====== Dhan credentials (env first; fallback to current) ======
import os
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
    LOG.error("DHAN_CLIENT_ID/DHAN_ACCESS_TOKEN missing. Set them as environment variables.")
    # fail fast in production - uncomment to exit
    # import sys; sys.exit(1)

# Fallback to hardcoded values for development (remove in production)
if not DHAN_CLIENT_ID:
    DHAN_CLIENT_ID = "1107860004"
if not DHAN_ACCESS_TOKEN:
    DHAN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU5NDI4NjYwLCJpYXQiOjE3NTY4MzY2NjAsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA3ODYwMDA0In0.ItPA3IuAidky2QpjG89uD0S60ysgAURoEDhaNrirzc6e1JENEbh3rij9wRPXgDjE_1Lkoovo5Qw5cCjLevRzhg"

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

# Global Dhan client instance
_DHAN_CLIENT = None

def get_dhan_client():
    """Return a configured dhanhq client instance (cached)."""
    global _DHAN_CLIENT
    if _DHAN_CLIENT is not None:
        return _DHAN_CLIENT
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        raise RuntimeError("Dhan credentials not configured")
    # Create dhanhq client instance
    _DHAN_CLIENT = dhanhq(client_id=DHAN_CLIENT_ID, access_token=DHAN_ACCESS_TOKEN)
    return _DHAN_CLIENT

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s.%(msecs)03d %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
from config import SQLITE_PATH, ALERTS_LOG_PATH

LOG = logging.getLogger("backend")

# Setup alerts logging with daily rotation
handler = TimedRotatingFileHandler(ALERTS_LOG_PATH, when="midnight", backupCount=7)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
handler.setFormatter(formatter)

alerts_logger = logging.getLogger("alerts")
alerts_logger.setLevel(logging.INFO)
alerts_logger.addHandler(handler)

app = FastAPI(title="Dhan Automation", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # simple for testing; tighten later if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include webhook routes at root
app.include_router(webhook_router, tags=["webhook"])

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

start_scheduler()
db_fresh = False  # set True if refresh triggers

def _refresh_instruments_on_boot():
    global db_fresh
    try:
        if ensure_fresh_db(SQLITE_PATH):
            db_fresh = True
        if not db_is_current(SQLITE_PATH):
            LOG.warning("Instrument DB outdated — forcing fresh download now (background)")
            download_and_populate(SQLITE_PATH)
    except Exception:
        LOG.exception("Background instrument refresh failed")

@app.on_event("startup")
async def _kick_off_refresh():
    import threading
    threading.Thread(target=_refresh_instruments_on_boot, daemon=True).start()

ok, why = init_broker(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
if not ok:
    LOG.error("Broker init failed: %s", why)

LOG.info("Backend boot complete")

@app.get("/healthz")
def healthz():
    return {"ok": True}

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
    qty: int = Query(..., ge=1),
    order_type: str = Query("MARKET", regex="^(MARKET|LIMIT)$"),
    price: float = Query(0.0, ge=0.0),
    product_type: str = Query("DELIVERY", regex="^(DELIVERY|CNC|INTRADAY)$"),
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

    LOG.debug("(%s) input symbol=%s segment=%s qty=%s order_type=%s price=%s product=%s validity=%s security_id=%s",
              rid, symbol, segment, qty, order_type, price, product_type, validity, security_id)

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
        inst = resolve_symbol(SQLITE_PATH, symbol, segment)

    lot = int((inst or {}).get("lotSize") or 1)
    if segment in ("NSE_FNO", "MCX") and lot > 1 and (int(qty) % lot != 0):
        LOG.warning("(%s) qty %s is not multiple of lot %s", rid, qty, lot)
        return {"status": "error", "rid": rid, "message": f"Qty must be multiple of lot size ({lot})"}

    LOG.debug("(%s) calling place_order_via_broker sid=%s", rid, security_id)
    raw_res = place_order_via_broker(
        security_id=security_id,
        segment=segment,
        side=side,
        qty=qty,
        order_type=order_type,
        price=price,
        product_type=product_type,
        validity=validity,
        symbol=symbol,
        disclosed_qty=disclosed_qty,
    )
    normalized = normalize_response(raw_res, success_msg="Order placed successfully", error_msg="Order rejected")
    elapsed = time.time() - t0
    # Always ensure JSONResponse returns immediately
    from fastapi.responses import JSONResponse
    return JSONResponse(content={
        "rid": rid,
        **normalized,
        "preview": {
            "symbol": symbol, "segment": segment, "side": side, "qty": int(qty),
            "order_type": order_type, "price": float(price or 0),
            "product_type": product_type, "validity": validity,
            "security_id": str(security_id), "lot": lot
        },
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
    conn = sqlite3.connect(SQLITE_PATH)
    try:
        c = conn.execute("SELECT COUNT(*) FROM instruments").fetchone()
        return {"exists": True, "rows": int(c[0])}
    finally:
        conn.close()

@app.get("/debug/segments")
def debug_segments():
    import sqlite3
    conn = sqlite3.connect(SQLITE_PATH)
    try:
        rows = conn.execute("SELECT DISTINCT segment, COUNT(*) FROM instruments GROUP BY segment").fetchall()
        return [{"segment": r[0], "count": r[1]} for r in rows]
    finally:
        conn.close()

# ---------- STATIC / SPA (Render-ready, zero npm at deploy) ----------

LOG = logging.getLogger("backend")

BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"              # <-- we commit built files here
INDEX_HTML = STATIC_DIR / "index.html"
ASSETS_DIR = STATIC_DIR / "assets"

if INDEX_HTML.exists() and ASSETS_DIR.exists():
    LOG.info("Serving SPA from %s", STATIC_DIR)
    # Vite index.html references /assets/...; mount that exact path.
    app.mount("/assets", StaticFiles(directory=ASSETS_DIR), name="assets")

    @app.get("/", include_in_schema=False)
    def serve_index_root():
        return FileResponse(INDEX_HTML)

    # Serve any file that actually exists under /static (favicon, icons, etc.)
    @app.get("/static/{path:path}", include_in_schema=False)
    def serve_static_passthrough(path: str):
        file_path = STATIC_DIR / path
        if file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(INDEX_HTML)

    # SPA fallback for client-side routes (/orders, /positions, etc.)
    @app.get("/{path:path}", include_in_schema=False)
    def spa_fallback(path: str):
        f = STATIC_DIR / path
        if f.is_file():
            return FileResponse(f)
        return FileResponse(INDEX_HTML)
else:
    LOG.warning("Frontend bundle missing. Expected %s and %s", INDEX_HTML, ASSETS_DIR)

    @app.get("/", include_in_schema=False)
    def placeholder():
        # Minimal page so you don't get a black screen if bundle is missing
        html = """
        <!doctype html><meta charset="utf-8">
        <title>Trading View x Dhan</title>
        <div style="font:16px system-ui;padding:24px">
          <h1>Backend is running ✅</h1>
          <p>No frontend bundle found at <code>dhan-backend/static/</code>.</p>
          <p>Commit your built UI (Vite <code>dist</code>) into that folder to enable the app.</p>
        </div>
        """
        return HTMLResponse(html)

# Optional: run with "python main.py" locally or on Render (reads $PORT automatically)
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
