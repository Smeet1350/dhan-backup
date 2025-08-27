# main.py
import os
import io
import csv
import json
import sqlite3
import tempfile
import threading
import time as time_module
from datetime import datetime, date
from typing import List, Dict, Optional

import requests
import pytz
import asyncio
import logging

from fastapi import FastAPI, HTTPException, Query
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ------- CONFIG -------
INSTRUMENT_DB_FILE = os.getenv("INSTRUMENT_DB_FILE", "instruments.db")
INSTRUMENT_TMP_DB = INSTRUMENT_DB_FILE + ".tmp"
# Replace this with the actual Dhan provided URL or a local file path (file://...)
INSTRUMENT_MASTER_SOURCE = os.getenv("INSTRUMENT_MASTER_SOURCE", "https://images.dhan.co/api-data/api-scrip-master.csv")
# Time to download (24h clock) in IST
DAILY_DOWNLOAD_HOUR = int(os.getenv("DAILY_DOWNLOAD_HOUR", "8"))
DAILY_DOWNLOAD_MINUTE = int(os.getenv("DAILY_DOWNLOAD_MINUTE", "0"))
# Time to cleanup after market close (IST)
DAILY_CLEANUP_HOUR = int(os.getenv("DAILY_CLEANUP_HOUR", "16"))
DAILY_CLEANUP_MINUTE = int(os.getenv("DAILY_CLEANUP_MINUTE", "0"))

DOWNLOAD_RETRIES = 3
DOWNLOAD_TIMEOUT = 30  # seconds
MAX_SEARCH_RESULTS = 50

IST = pytz.timezone("Asia/Kolkata")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("instruments")

app = FastAPI(title="Dhan Instruments + Lookup")

# Lock to guard in-memory index
_index_lock = threading.Lock()

class InstrumentsManager:
    def __init__(self, db_path: str, source_url: str, use_memory_index: bool = True):
        self.db_path = db_path
        self.source_url = source_url
        self.use_memory = use_memory_index
        # in-memory index: tradingSymbol (upper) -> list of instrument dict
        self.instruments_by_symbol: Dict[str, List[Dict]] = {}
        # additional dict for securityId -> instrument
        self.instruments_by_id: Dict[str, Dict] = {}
        self.last_updated: Optional[str] = None  # YYYY-MM-DD
        self.lock = threading.Lock()

    # ---------- DB helpers ----------
    def _create_db(self, conn: sqlite3.Connection):
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS instruments")
        cur.execute("""
           CREATE TABLE instruments (
               securityId TEXT PRIMARY KEY,
               tradingSymbol TEXT,
               exchange TEXT,
               segment TEXT,
               expiry TEXT,
               lotSize INTEGER,
               raw JSON,
               last_updated DATE
           )
        """)
        cur.execute("DROP TABLE IF EXISTS meta")
        cur.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        conn.commit()

    def _insert_row(self, conn: sqlite3.Connection, inst: Dict):
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO instruments (securityId, tradingSymbol, exchange, segment, expiry, lotSize, raw, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                inst.get("securityId"),
                inst.get("tradingSymbol"),
                inst.get("exchange"),
                inst.get("segment"),
                inst.get("expiry"),
                inst.get("lotSize"),
                json.dumps(inst),
                inst.get("last_updated"),
            )
        )

    def _save_meta_date(self, conn: sqlite3.Connection, ymd: str):
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", ("date", ymd))
        conn.commit()

    def _read_meta_date(self, conn: sqlite3.Connection) -> Optional[str]:
        cur = conn.cursor()
        cur.execute("SELECT value FROM meta WHERE key = ?", ("date",))
        row = cur.fetchone()
        return row[0] if row else None

    # ---------- Parsing ----------
    def _normalize_row(self, row: Dict[str,str]) -> Optional[Dict]:
        # Robustly map columns with a few alias names
        lowermap = {k.lower(): k for k in row.keys()}
        def get(*names):
            for n in names:
                if n.lower() in lowermap:
                    return row[lowermap[n.lower()]].strip()
            return None

        securityId = get("securityId", "security_id", "id", "instrumenttoken", "token")
        tradingSymbol = get("tradingSymbol", "trading_symbol", "symbol", "tradingsymbol")
        exchange = get("exchange", "exch")
        segment = get("segment")
        expiry = get("expiry", "expiry_date", "expirydate")
        lotSize = get("lotSize", "lot_size", "lotsize", "lot")
        # best-effort conversions
        if not securityId or not tradingSymbol:
            return None
        try:
            lotSizeVal = int(float(lotSize)) if lotSize else 1
        except Exception:
            lotSizeVal = 1

        inst = {
            "securityId": str(securityId),
            "tradingSymbol": tradingSymbol,
            "exchange": exchange or "",
            "segment": segment or "",
            "expiry": expiry or "",
            "lotSize": lotSizeVal,
            "raw_row": row
        }
        return inst

    # ---------- Download & populate ----------
    def _download_text(self) -> str:
        # Support file:// local path too
        if self.source_url.startswith("file://"):
            local_path = self.source_url[7:]
            with open(local_path, "r", encoding="utf-8") as f:
                return f.read()

        last_exc = None
        for attempt in range(1, DOWNLOAD_RETRIES + 1):
            try:
                logger.info(f"Downloading instruments (attempt {attempt}) from {self.source_url}")
                r = requests.get(self.source_url, timeout=DOWNLOAD_TIMEOUT)
                r.raise_for_status()
                return r.text
            except Exception as e:
                last_exc = e
                backoff = 1 << attempt
                logger.warning(f"Download attempt {attempt} failed: {e}. Retrying in {backoff}s...")
                time_module.sleep(backoff)
        # after retries
        raise last_exc

    def download_and_populate(self) -> Dict:
        """Downloads master (CSV/JSON), writes to temp sqlite DB and atomically replaces main DB.
           Returns dict with status and last_updated date.
        """
        txt = self._download_text()
        # detect CSV vs JSON
        is_json = False
        stripped = txt.lstrip()
        if stripped.startswith("{") or stripped.startswith("["):
            is_json = True

        today = date.today().isoformat()

        # create temp DB file
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".db")
        os.close(tmp_fd)
        try:
            conn = sqlite3.connect(tmp_path)
            self._create_db(conn)
            # parse
            if is_json:
                parsed = json.loads(txt)
                # Expect either list or dict containing list
                rows = parsed if isinstance(parsed, list) else (parsed.get("data") or parsed.get("instruments") or [])
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    inst = self._normalize_row({k: str(v) for k, v in row.items()})
                    if inst:
                        inst["last_updated"] = today
                        self._insert_row(conn, inst)
            else:
                # CSV parse - robust to different column names
                csvfile = io.StringIO(txt)
                reader = csv.DictReader(csvfile)
                for row in reader:
                    inst = self._normalize_row(row)
                    if inst:
                        inst["last_updated"] = today
                        self._insert_row(conn, inst)

            # save date meta
            self._save_meta_date(conn, today)
            conn.commit()
            conn.close()

            # atomic replace
            os.replace(tmp_path, self.db_path)
            logger.info("Instruments DB updated and saved to %s", self.db_path)

            # reload into memory index
            self.load_index_from_db()
            return {"status": "success", "date": today}
        except Exception as e:
            logger.exception("Failed to write temp DB: %s", e)
            # cleanup temp
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            raise

    def load_index_from_db(self) -> bool:
        """Load instruments into memory index. Returns True on success."""
        if not os.path.exists(self.db_path):
            logger.warning("DB file %s not found", self.db_path)
            return False

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        # read meta date
        cur.execute("SELECT value FROM meta WHERE key = ?", ("date",))
        r = cur.fetchone()
        ymd = r[0] if r else None
        self.last_updated = ymd

        # build in-memory indexes
        tmp_by_symbol = {}
        tmp_by_id = {}

        cur.execute("SELECT securityId, raw FROM instruments")
        rows = cur.fetchall()
        for row in rows:
            secid = row["securityId"]
            raw = json.loads(row["raw"])
            symbol = raw.get("tradingSymbol", "").upper()
            if not symbol:
                continue
            inst = {
                "securityId": secid,
                "tradingSymbol": raw.get("tradingSymbol"),
                "exchange": raw.get("exchange"),
                "segment": raw.get("segment"),
                "expiry": raw.get("expiry"),
                "lotSize": raw.get("lotSize"),
                "raw": raw,
            }
            tmp_by_id[secid] = inst
            tmp_by_symbol.setdefault(symbol, []).append(inst)

        conn.close()
        with self.lock:
            self.instruments_by_symbol = tmp_by_symbol
            self.instruments_by_id = tmp_by_id
        logger.info("Loaded %d instruments into memory index (last updated=%s)", len(tmp_by_id), self.last_updated)
        return True

    def db_is_current(self) -> bool:
        if not os.path.exists(self.db_path):
            return False
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("SELECT value FROM meta WHERE key = ?", ("date",))
            row = cur.fetchone()
            conn.close()
            if not row:
                return False
            return row[0] == date.today().isoformat()
        except Exception:
            return False

    # ---------- Lookup APIs ----------
    def search(self, query: str, segment: Optional[str] = None, limit: int = 20) -> List[Dict]:
        q = query.strip().upper()
        if not q:
            return []
        results = []
        with self.lock:
            # direct symbol match
            for symbol, items in self.instruments_by_symbol.items():
                if q in symbol:
                    for inst in items:
                        if segment and segment.upper() not in (inst.get("segment","") or "").upper():
                            continue
                        results.append(inst)
                        if len(results) >= limit:
                            return results
        # fallback to sqlite LIKE if not enough results or memory disabled
        if len(results) < limit and os.path.exists(self.db_path):
            try:
                conn = sqlite3.connect(self.db_path)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                pattern = f"%{q}%"
                params = (pattern, pattern, pattern, limit)
                cur.execute("""
                    SELECT securityId, raw FROM instruments
                    WHERE UPPER(tradingSymbol) LIKE ? OR UPPER(securityId) LIKE ? OR UPPER(raw) LIKE ?
                    LIMIT ?
                """, params)
                for row in cur.fetchall():
                    inst = json.loads(row["raw"])
                    candidate = {
                        "securityId": row["securityId"],
                        "tradingSymbol": inst.get("tradingSymbol"),
                        "exchange": inst.get("exchange"),
                        "segment": inst.get("segment"),
                        "expiry": inst.get("expiry"),
                        "lotSize": inst.get("lotSize"),
                        "raw": inst,
                    }
                    # avoid duplicates
                    if candidate not in results:
                        results.append(candidate)
                        if len(results) >= limit:
                            break
                conn.close()
            except Exception:
                logger.exception("SQLite fallback search failed")
        return results[:limit]

    def get_by_symbol_exact(self, symbol: str) -> Optional[List[Dict]]:
        s = symbol.strip().upper()
        with self.lock:
            return self.instruments_by_symbol.get(s)

    def get_by_security_id(self, sid: str) -> Optional[Dict]:
        with self.lock:
            return self.instruments_by_id.get(str(sid))

    # ---------- Cleanup ----------
    def cleanup(self) -> Dict:
        """Deletes DB file and clears in-memory index."""
        with self.lock:
            self.instruments_by_symbol = {}
            self.instruments_by_id = {}
            self.last_updated = None
        try:
            if os.path.exists(self.db_path):
                os.remove(self.db_path)
                logger.info("Instrument DB %s removed", self.db_path)
            return {"status": "success"}
        except Exception as e:
            logger.exception("Failed to delete DB: %s", e)
            return {"status": "failed", "message": str(e)}

# instantiate manager
instruments_manager = InstrumentsManager(INSTRUMENT_DB_FILE, INSTRUMENT_MASTER_SOURCE, use_memory_index=True)


# ------ Scheduler & startup events ------
scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")

async def ensure_today_loaded_on_startup():
    """If today's DB present -> load it. Else try to download immediately (best-effort)."""
    try:
        if instruments_manager.db_is_current():
            logger.info("Today's instruments DB exists; loading into memory.")
            instruments_manager.load_index_from_db()
            return
        # If DB absent or stale, try immediate download (but don't crash on failure)
        logger.info("Today's instruments DB not present — attempting immediate download (startup).")
        # run blocking download in thread
        res = await asyncio.to_thread(instruments_manager.download_and_populate)
        logger.info("Startup download result: %s", res)
    except Exception as e:
        logger.exception("Startup load failed (non-fatal): %s", e)


def schedule_jobs():
    # schedule daily download at configured time IST
    scheduler.add_job(lambda: asyncio.create_task(asyncio.to_thread(instruments_manager.download_and_populate)),
                      "cron",
                      hour=DAILY_DOWNLOAD_HOUR,
                      minute=DAILY_DOWNLOAD_MINUTE,
                      id="daily_download",
                      replace_existing=True,
                      timezone="Asia/Kolkata")
    # schedule daily cleanup at configured time
    scheduler.add_job(lambda: asyncio.create_task(asyncio.to_thread(instruments_manager.cleanup)),
                      "cron",
                      hour=DAILY_CLEANUP_HOUR,
                      minute=DAILY_CLEANUP_MINUTE,
                      id="daily_cleanup",
                      replace_existing=True,
                      timezone="Asia/Kolkata")
    scheduler.start()
    logger.info("Scheduler started (download at %02d:%02d IST, cleanup at %02d:%02d IST)",
                DAILY_DOWNLOAD_HOUR, DAILY_DOWNLOAD_MINUTE, DAILY_CLEANUP_HOUR, DAILY_CLEANUP_MINUTE)


@app.on_event("startup")
async def app_startup():
    logger.info("App startup: ensuring instruments are loaded and scheduler is started.")
    schedule_jobs()
    await ensure_today_loaded_on_startup()

# --------- FastAPI endpoints for instrument lookup & management ----------
@app.get("/instruments/status")
def instruments_status():
    return {
        "status": "success",
        "last_updated": instruments_manager.last_updated,
        "in_memory_count": len(instruments_manager.instruments_by_id),
        "db_exists": os.path.exists(INSTRUMENT_DB_FILE)
    }

@app.post("/instruments/refresh")
def instruments_refresh():
    try:
        # trigger immediate refresh (blocking)
        res = instruments_manager.download_and_populate()
        return {"status": "success", "result": res}
    except Exception as e:
        logger.exception("Manual refresh failed")
        return {"status": "failed", "message": str(e)}

@app.post("/instruments/cleanup")
def instruments_cleanup():
    res = instruments_manager.cleanup()
    if res.get("status") == "success":
        return {"status": "success", "message": "Instruments removed"}
    return {"status": "failed", "message": res.get("message")}

@app.get("/symbol-search")
def symbol_search(query: str = Query(..., min_length=1), segment: Optional[str] = None, limit: int = Query(20, ge=1, le=MAX_SEARCH_RESULTS)):
    try:
        results = instruments_manager.search(query, segment, limit)
        return {"status": "success", "results": results}
    except Exception as e:
        logger.exception("Search failed")
        return {"status": "failed", "message": str(e)}

@app.get("/symbol/{symbol}")
def symbol_lookup(symbol: str):
    try:
        items = instruments_manager.get_by_symbol_exact(symbol)
        if not items:
            return {"status": "failed", "message": "Symbol not found"}
        # return all matches (same tradingSymbol may map to multiple contracts)
        return {"status": "success", "results": items}
    except Exception as e:
        logger.exception("Lookup failed")
        return {"status": "failed", "message": str(e)}

@app.get("/security/{security_id}")
def security_lookup(security_id: str):
    try:
        item = instruments_manager.get_by_security_id(security_id)
        if not item:
            return {"status": "failed", "message": "SecurityId not found"}
        return {"status": "success", "result": item}
    except Exception as e:
        logger.exception("Lookup failed")
        return {"status": "failed", "message": str(e)}

# Add Dhan trading functionality
from dhanhq import dhanhq
import sqlite3, os
from scheduler import DB_FILE

# 🔑 Add your real credentials here
DHAN_CLIENT_ID = "1107860004"
DHAN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU2ODM2NDA4LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwNzg2MDAwNCJ9.3cuzgiY0Qm2Id8wpMW0m90_ZxJ0TJRTV5fZ0tpAwWo3S1Mv5HbpcDNwXxXVepnOUHMRDck_AbArIoVOmlA68Dg"

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
