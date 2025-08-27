# main.py
import os
import logging
from datetime import datetime, time
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

# configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dhan-backend")

# import scheduler and orders modules we provided
from scheduler import start_scheduler, instruments_by_symbol, instruments_by_id, last_updated, search_instruments, load_index_from_db, db_is_current
from orders import (resolve_symbol, place_order_via_dhan,
                    get_order_list, cancel_order, get_funds, get_holdings, get_positions)

app = FastAPI(title="Dhan Automation Backend")

# CORS - allow react dev server origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    logger.info("Starting scheduler...")
    # start scheduler (download at 8AM, cleanup at 4PM)
    start_scheduler()
    # ensure DB loaded if exists
    try:
        if db_is_current():
            load_index_from_db()
    except Exception:
        logger.exception("Failed to load index at startup (non-fatal)")

# -------------------
# Health & status
# -------------------
@app.get("/status")
def status():
    try:
        return {
            "status": "ok",
            "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "instruments_loaded": bool(instruments_by_id),
            "instruments_last_updated": last_updated
        }
    except Exception as e:
        logger.exception("Status error")
        return {"status": "error", "message": str(e)}

# -------------------
# Instruments endpoints
# -------------------
@app.get("/instruments/status")
def instruments_status():
    try:
        return {
            "status": "success",
            "last_updated": last_updated,
            "in_memory_count": len(instruments_by_id),
            "db_exists": os.path.exists("instruments.db")
        }
    except Exception as e:
        logger.exception("Instruments status error")
        return {"status": "failed", "message": str(e)}

@app.get("/symbol-search")
def symbol_search(query: str = Query(..., min_length=1), segment: str = Query(None), limit: int = Query(20, ge=1, le=100)):
    try:
        results = search_instruments(query, segment, limit)
        return {"status": "success", "results": results}
    except Exception as e:
        logger.exception("Symbol search failed")
        return {"status": "failed", "message": str(e)}

@app.get("/symbol/{symbol}")
def symbol_lookup(symbol: str):
    try:
        res = resolve_symbol(symbol)
        return res
    except Exception as e:
        logger.exception("Symbol lookup failed")
        return {"status": "failed", "message": str(e)}

@app.post("/instruments/refresh")
def instruments_refresh():
    try:
        res = start_scheduler()  # scheduler will attempt immediate download on startup; provide manual endpoint below if needed
        # But we want a manual download: call scheduler.download job directly if needed - easiest is to call scheduler's download function
        # For simplicity, we will call scheduler.download via the module's function if exists.
        from scheduler import download_and_populate
        r = download_and_populate()
        return {"status": "success", "result": r}
    except Exception as e:
        logger.exception("Manual instruments refresh failed")
        return {"status": "failed", "message": str(e)}

@app.post("/instruments/cleanup")
def instruments_cleanup():
    try:
        from scheduler import cleanup_instruments
        r = cleanup_instruments()
        return r
    except Exception as e:
        logger.exception("Manual instruments cleanup failed")
        return {"status": "failed", "message": str(e)}

# -------------------
# Funds / holdings / positions
# -------------------
@app.get("/funds")
def api_funds():
    try:
        res = get_funds()
        if res.get("status") and res.get("status").lower() != "success" and res.get("status").lower() != "ok":
            # some dhanhq responses use "status": "success" or dictionary with status
            # normalize to {"status":"failed", "message":...}
            msg = res.get("remarks") or res.get("message") or str(res)
            return {"status": "failed", "message": str(msg)}
        return {"status": "success", "funds": res.get("data") or res}
    except Exception as e:
        logger.exception("Funds endpoint error")
        return {"status": "failed", "message": str(e)}

@app.get("/holdings")
def api_holdings():
    try:
        res = get_holdings()
        logger.info("Holdings raw: %s", res)
        if res.get("status") and res.get("status").lower() != "success":
            return {"status": "failed", "message": res.get("remarks") or res.get("message") or str(res)}
        # Many dhan responses have structure {status, remarks, data: [...]}
        data = res.get("data") if isinstance(res, dict) else res
        return {"status": "success", "holdings": data}
    except Exception as e:
        logger.exception("Holdings endpoint error")
        return {"status": "failed", "message": str(e)}

@app.get("/positions")
def api_positions():
    try:
        res = get_positions()
        logger.info("Positions raw: %s", res)
        if res.get("status") and res.get("status").lower() != "success":
            return {"status": "failed", "message": res.get("remarks") or res.get("message") or str(res)}
        data = res.get("data") if isinstance(res, dict) else res
        return {"status": "success", "positions": data}
    except Exception as e:
        logger.exception("Positions endpoint error")
        return {"status": "failed", "message": str(e)}

# -------------------
# Orders endpoints
# -------------------
@app.get("/orders")
def api_get_orders():
    try:
        res = get_order_list()
        logger.info("Orders raw: %s", res)
        if res.get("status") and res.get("status").lower() != "success":
            return {"status": "failed", "message": res.get("remarks") or res.get("message") or str(res)}
        data = res.get("data") if isinstance(res, dict) else res
        return {"status": "success", "orders": data}
    except Exception as e:
        logger.exception("Get orders endpoint error")
        return {"status": "failed", "message": str(e)}

@app.post("/order/place")
def api_place_order(
    symbol: str = Query(..., description="Trading symbol, e.g., TCS"),
    qty: int = Query(..., description="Quantity"),
    side: str = Query("BUY", description="BUY/SELL"),
    segment: str = Query(None, description="Optional segment filter like NSE_EQ or NSE_FNO"),
    order_type: str = Query("MARKET", description="MARKET/LIMIT"),
    price: float = Query(0.0, description="Limit price if LIMIT"),
    product_type: str = Query("DELIVERY", description="DELIVERY/INTRADAY"),
    validity: str = Query("DAY", description="DAY/IOC")
):
    try:
        # check market open roughly (9:00 - 16:00 IST)
        now_ist = datetime.now().astimezone()
        if now_ist.weekday() >= 5:
            return {"status": "failed", "message": "Market closed (weekend)"}
        # simple hour check - you can improve with calendar
        if now_ist.hour < 9 or now_ist.hour >= 16:
            market_notice = "Markets appear closed by clock"
        else:
            market_notice = ""

        # resolve symbol to instrument(s)
        res = resolve_symbol(symbol, segment_filter=segment)
        if res.get("status") != "success":
            return {"status": "failed", "message": res.get("message")}

        inst = res["results"][0]  # choose first match; you can return matches for user to pick
        security_id = inst["securityId"]
        exchange_segment = inst.get("segment") or inst.get("exchange") or ""

        # place order via orders module
        dh_res = place_order_via_dhan(security_id, exchange_segment, side, qty,
                                      order_type, price, product_type, validity)
        logger.info("Place order result: %s", dh_res)
        # dh_res may contain status/key names per Dhan; normalize
        if dh_res.get("status") and dh_res.get("status").lower() == "success":
            return {"status": "success", "message": "Order placed", "preview": {
                "symbol": symbol,
                "securityId": security_id,
                "side": side,
                "qty": qty,
                "order_type": order_type,
                "price": price,
                "product_type": product_type
            }, "order_response": dh_res, "marketNotice": market_notice}
        else:
            msg = dh_res.get("remarks") or dh_res.get("message") or str(dh_res)
            return {"status": "failed", "message": msg, "preview": {
                "symbol": symbol,
                "securityId": security_id
            }, "marketNotice": market_notice}
    except Exception as e:
        logger.exception("Place order error")
        return {"status": "failed", "message": str(e)}

@app.post("/order/cancel")
def api_cancel_order(order_id: str = Query(..., description="Order ID to cancel")):
    try:
        res = cancel_order(order_id)
        logger.info("Cancel result: %s", res)
        if res.get("status") and res.get("status").lower() == "success":
            return {"status": "success", "cancel": res}
        else:
            return {"status": "failed", "message": res.get("remarks") or res.get("message") or str(res)}
    except Exception as e:
        logger.exception("Cancel order error")
        return {"status": "failed", "message": str(e)}
