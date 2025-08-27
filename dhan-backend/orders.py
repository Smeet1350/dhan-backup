from dhanhq import dhanhq
import sqlite3, os
from scheduler import DB_FILE

# 🔑 Add your real credentials here
DHAN_CLIENT_ID = "YOUR_CLIENT_ID"
DHAN_ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"

dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)

def resolve_symbol(symbol: str, segment_filter: str = None):
    """Lookup symbol in SQLite (downloaded daily)"""
    if not os.path.exists(DB_FILE):
        return {"status": "error", "message": "Instrument master not available."}

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    if segment_filter:
        cur.execute("""
            SELECT securityId, tradingSymbol, exchangeSegment, instrument, expiry, lotSize, underlying
            FROM instruments
            WHERE tradingSymbol = ? AND exchangeSegment = ?
        """, (symbol.upper(), segment_filter))
    else:
        cur.execute("""
            SELECT securityId, tradingSymbol, exchangeSegment, instrument, expiry, lotSize, underlying
            FROM instruments
            WHERE tradingSymbol = ?
        """, (symbol.upper(),))

    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"status": "error", "message": f"Symbol {symbol.upper()} not found"}

    results = []
    for row in rows:
        results.append({
            "securityId": row[0],
            "tradingSymbol": row[1],
            "exchangeSegment": row[2],
            "instrument": row[3],
            "expiry": row[4],
            "lotSize": row[5],
            "underlying": row[6]
        })

    return {"status": "success", "data": results}


def place_order(symbol: str, qty: int, side: str = "BUY", segment: str = "EQUITY"):
    """
    Place order in equity/F&O/other segment.
    side: BUY / SELL
    segment: EQUITY, FNO, FUT, etc.
    """
    # Step 1: Resolve symbol
    res = resolve_symbol(symbol, segment_filter=None)
    if res["status"] != "success":
        return res

    # Just pick the first match for now (later we can refine)
    instrument = res["data"][0]

    # Step 2: Place order
    try:
        order = dhan.place_order(
            tag="my_dhan_order",
            transaction_type=side,
            exchange_segment=instrument["exchangeSegment"],
            product="CNC",  # CNC for equity delivery, change for intraday/F&O
            order_type="MARKET",
            validity="DAY",
            security_id=instrument["securityId"],
            quantity=qty,
            disclosed_quantity=0,
            price=0,
            trigger_price=0,
            after_market_order=False,
            amo_time="OPEN"
        )
        return {"status": "success", "symbol": symbol, "qty": qty, "side": side, "details": order}

    except Exception as e:
        return {"status": "error", "message": str(e)}
