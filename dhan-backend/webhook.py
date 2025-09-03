# webhook.py
import logging
import sqlite3
from datetime import datetime
from fastapi import APIRouter, Request
from dateutil import parser

from orders import broker_ready, place_order_via_broker, normalize_response
from scheduler import ensure_fresh_db
from config import SQLITE_PATH

LOG = logging.getLogger("webhook")
router = APIRouter(prefix="/webhook", tags=["webhook"])


def parse_expiry(exp_str: str):
    """Robust expiry date parsing."""
    if not exp_str or exp_str == "0001-01-01":
        return None
    try:
        return datetime.strptime(exp_str, "%Y-%m-%d").date()
    except Exception:
        try:
            return parser.parse(exp_str, fuzzy=True).date()
        except Exception:
            return None


def infer_segment_from_symbol(symbol: str) -> str:
    """Infer segment from trading symbol when database segment is None."""
    s = symbol.upper()
    if any(idx in s for idx in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY")):
        return "NSE_FNO"
    if "MCX" in s:
        return "MCX"
    if "BSE" in s:
        return "BSE_EQ"
    if "NSE" in s:
        return "NSE_EQ"
    return "NSE_FNO"  # safe default


def find_instrument(db_path: str, index_symbol: str, strike: int, option_type: str) -> dict:
    """Find instrument based on actual DB format."""
    conn = sqlite3.connect(db_path)
    try:
        sql = """
            SELECT securityId, tradingSymbol, segment, lotSize, expiry
            FROM instruments
            WHERE UPPER(tradingSymbol) LIKE ?
              AND UPPER(tradingSymbol) LIKE ?
        """
        like_index = f"{index_symbol.upper()}%"
        like_suffix = f"%-{strike}-{option_type.upper()}"
        rows = conn.execute(sql, (like_index, like_suffix)).fetchall()

        if not rows:
            return {}

        cols = ["securityId", "tradingSymbol", "segment", "lotSize", "expiry"]
        today = datetime.now().date()

        valid = []
        for r in rows:
            rec = dict(zip(cols, r))
            expd = parse_expiry(rec.get("expiry", ""))
            if expd and expd >= today:
                valid.append((expd, rec))

        if not valid:
            return {}

        valid.sort(key=lambda x: x[0])
        return valid[0][1]

    finally:
        conn.close()


@router.post("/trade")
async def webhook_trade(req: Request):
    """Webhook endpoint for option trades."""
    body = await req.json()
    LOG.info("Webhook payload: %s", body)

    try:
        ensure_fresh_db(SQLITE_PATH)

        ok, why = broker_ready()
        if not ok:
            return {"status": "error", "message": f"Broker not ready: {why}"}

        index_symbol = str(body.get("index", "")).upper()
        strike = int(body.get("strike", 0))
        option_type = str(body.get("option_type", "")).upper()
        side = str(body.get("side", "BUY")).upper()
        qty = int(body.get("qty", 0))
        order_type = str(body.get("order_type", "MARKET")).upper()
        price = float(body.get("price") or 0)
        product_type = str(body.get("product_type", "INTRADAY")).upper()
        validity = str(body.get("validity", "DAY")).upper()

        if not index_symbol or strike <= 0 or option_type not in ("CE", "PE"):
            return {"status": "error", "message": "Invalid input"}

        inst = find_instrument(SQLITE_PATH, index_symbol, strike, option_type)
        if not inst:
            return {"status": "error", "message": f"No instrument found for {index_symbol} {strike}{option_type}"}

        lot = int(inst["lotSize"])
        if qty <= 0:
            qty = lot
        elif qty % lot != 0:
            return {"status": "error", "message": f"Qty {qty} not multiple of lot {lot}"}

        # Instrument validation
        if not inst.get("securityId"):
            return {"status": "error", "message": "Instrument missing securityId"}
        
        if not inst.get("lotSize"):
            return {"status": "error", "message": "Instrument missing lotSize"}

        # Segment fallback logic
        segment = inst.get("segment") or infer_segment_from_symbol(inst["tradingSymbol"])
        if not segment:
            return {"status": "error", "message": f"Could not infer segment for {inst}"}

        LOG.debug("Final instrument for order: %s", inst)
        LOG.debug("Using segment=%s, lotSize=%s, securityId=%s",
                  segment, inst.get("lotSize"), inst.get("securityId"))

        raw_res = place_order_via_broker(
            security_id=str(inst["securityId"]),
            segment=segment,
            side=side,
            qty=qty,
            order_type=order_type,
            price=None if order_type == "MARKET" else price,
            product_type=product_type,
            validity=validity,
            symbol=inst["tradingSymbol"],
            disclosed_qty=0,
        )
        return normalize_response(raw_res, success_msg="Order placed via webhook", error_msg="Webhook order failed")

    except Exception as e:
        LOG.exception("Webhook trade failed")
        return {"status": "error", "message": str(e)}
