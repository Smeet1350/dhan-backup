# scheduler.py
import os
import io
import csv
import json
import tempfile
import sqlite3
import logging
import time as time_module
from datetime import date
from typing import Optional, List, Dict

import requests
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logger = logging.getLogger("instruments")
logger.setLevel(logging.INFO)

# CONFIG
INSTRUMENT_DB_FILE = os.getenv("INSTRUMENT_DB_FILE", "instruments.db")
INSTRUMENT_SOURCE_URL = os.getenv(
    "INSTRUMENT_MASTER_SOURCE",
    "https://images.dhan.co/api-data/api-scrip-master.csv"
)
DAILY_DOWNLOAD_HOUR = int(os.getenv("DAILY_DOWNLOAD_HOUR", "8"))
DAILY_DOWNLOAD_MINUTE = int(os.getenv("DAILY_DOWNLOAD_MINUTE", "0"))
DAILY_CLEANUP_HOUR = int(os.getenv("DAILY_CLEANUP_HOUR", "16"))
DAILY_CLEANUP_MINUTE = int(os.getenv("DAILY_CLEANUP_MINUTE", "0"))
DOWNLOAD_RETRIES = int(os.getenv("DOWNLOAD_RETRIES", "3"))
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "30"))
IST = pytz.timezone("Asia/Kolkata")

# in-memory indexes
instruments_by_symbol = {}   # symbol upper -> list of inst dicts
instruments_by_id = {}       # securityId -> inst dict
last_updated: Optional[str] = None
_index_lock = None

def _create_db(conn: sqlite3.Connection):
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
        raw TEXT,
        last_updated DATE
    )""")
    cur.execute("DROP TABLE IF EXISTS meta")
    cur.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()

def _insert_row(conn: sqlite3.Connection, inst: Dict):
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO instruments
        (securityId, tradingSymbol, exchange, segment, expiry, lotSize, raw, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        inst.get("securityId"),
        inst.get("tradingSymbol"),
        inst.get("exchange"),
        inst.get("segment"),
        inst.get("expiry"),
        inst.get("lotSize") or 1,
        json.dumps(inst.get("raw_row", inst)),
        inst.get("last_updated"),
    ))
    conn.commit()

def _normalize_row(row: Dict[str,str]) -> Optional[Dict]:
    # Normalize column names (robust to variations)
    def g(*names):
        for n in names:
            if n in row and row[n] is not None:
                return str(row[n]).strip()
            # try lowercase key
            ln = n.lower()
            for k in row:
                if k.lower() == ln:
                    return str(row[k]).strip()
        return None

    securityId = g("securityId", "security_id", "id", "instrumenttoken", "token")
    tradingSymbol = g("tradingSymbol", "trading_symbol", "symbol", "tradingsymbol")
    exchange = g("exchange", "exch", "exchange_segment")
    segment = g("segment", "instrument")
    expiry = g("expiry", "expiry_date", "expirydate")
    lot = g("lotSize", "lot_size", "lotsize", "lot")
    if not securityId or not tradingSymbol:
        return None
    try:
        lotVal = int(float(lot)) if lot else 1
    except Exception:
        lotVal = 1
    inst = {
        "securityId": str(securityId),
        "tradingSymbol": tradingSymbol,
        "exchange": exchange or "",
        "segment": segment or "",
        "expiry": expiry or "",
        "lotSize": lotVal,
        "raw_row": row
    }
    return inst

def _download_text() -> str:
    last_exc = None
    for attempt in range(1, DOWNLOAD_RETRIES+1):
        try:
            logger.info("Downloading instruments from %s (attempt %d)", INSTRUMENT_SOURCE_URL, attempt)
            r = requests.get(INSTRUMENT_SOURCE_URL, timeout=DOWNLOAD_TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_exc = e
            backoff = 1 << attempt
            logger.warning("Download attempt %d failed: %s — retrying in %ds", attempt, e, backoff)
            time_module.sleep(backoff)
    logger.error("All download attempts failed: %s", last_exc)
    raise last_exc

def download_and_populate() -> Dict:
    """Download master and write an atomic sqlite DB file. Returns status dict."""
    global last_updated, instruments_by_symbol, instruments_by_id
    txt = _download_text()
    today = date.today().isoformat()

    # write to temp DB
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".db")
    os.close(tmp_fd)
    try:
        conn = sqlite3.connect(tmp_path)
        _create_db(conn)
        # detect CSV vs JSON
        s = txt.lstrip()
        if s.startswith("{") or s.startswith("["):
            parsed = json.loads(txt)
            rows = parsed if isinstance(parsed, list) else (parsed.get("data") or parsed.get("instruments") or [])
            for r in rows:
                if not isinstance(r, dict): continue
                inst = _normalize_row({k: ("" if r[k] is None else str(r[k])) for k in r})
                if inst:
                    inst["last_updated"] = today
                    _insert_row(conn, inst)
        else:
            csvfile = io.StringIO(txt)
            reader = csv.DictReader(csvfile)
            for row in reader:
                inst = _normalize_row(row)
                if inst:
                    inst["last_updated"] = today
                    _insert_row(conn, inst)

        # save meta
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", ("date", today))
        conn.commit()
        conn.close()

        # atomically replace DB file
        os.replace(tmp_path, INSTRUMENT_DB_FILE)
        logger.info("Instruments DB saved to %s", INSTRUMENT_DB_FILE)

        # load into memory
        load_index_from_db()
        return {"status": "success", "date": today}
    except Exception as e:
        logger.exception("Failed to write instruments DB: %s", e)
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        raise

def load_index_from_db() -> bool:
    """Load instruments into memory. Returns True on success."""
    global instruments_by_symbol, instruments_by_id, last_updated
    if not os.path.exists(INSTRUMENT_DB_FILE):
        logger.warning("DB file not present: %s", INSTRUMENT_DB_FILE)
        return False
    conn = sqlite3.connect(INSTRUMENT_DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT value FROM meta WHERE key = ?", ("date",))
    row = cur.fetchone()
    last_updated = row[0] if row else None

    tmp_by_symbol = {}
    tmp_by_id = {}
    cur.execute("SELECT securityId, raw FROM instruments")
    rows = cur.fetchall()
    for r in rows:
        sid = r["securityId"]
        try:
            raw = json.loads(r["raw"])
        except Exception:
            try:
                raw = json.loads(r["raw"].replace("'", "\""))
            except Exception:
                raw = {}
        sym = str(raw.get("tradingSymbol","")).upper()
        inst = {
            "securityId": sid,
            "tradingSymbol": raw.get("tradingSymbol"),
            "exchange": raw.get("exchange"),
            "segment": raw.get("segment"),
            "expiry": raw.get("expiry"),
            "lotSize": raw.get("lotSize"),
            "raw": raw
        }
        tmp_by_id[sid] = inst
        if sym:
            tmp_by_symbol.setdefault(sym, []).append(inst)
    conn.close()

    instruments_by_symbol = tmp_by_symbol
    instruments_by_id = tmp_by_id
    logger.info("Loaded %d instruments into memory (date=%s)", len(instruments_by_id), last_updated)
    return True

def db_is_current() -> bool:
    if not os.path.exists(INSTRUMENT_DB_FILE):
        return False
    try:
        conn = sqlite3.connect(INSTRUMENT_DB_FILE)
        cur = conn.cursor()
        cur.execute("SELECT value FROM meta WHERE key = ?", ("date",))
        row = cur.fetchone()
        conn.close()
        return bool(row and row[0] == date.today().isoformat())
    except Exception:
        return False

def search_instruments(query: str, segment: Optional[str]=None, limit: int=20) -> List[Dict]:
    q = query.strip().upper()
    if not q:
        return []
    results = []
    # in-memory search first
    for sym, items in instruments_by_symbol.items():
        if q in sym:
            for inst in items:
                if segment and segment.upper() not in (inst.get("segment") or "").upper():
                    continue
                results.append(inst)
                if len(results) >= limit:
                    return results
    # fallback to sqlite
    if os.path.exists(INSTRUMENT_DB_FILE):
        try:
            conn = sqlite3.connect(INSTRUMENT_DB_FILE)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            pattern = f"%{q}%"
            cur.execute("""
                SELECT securityId, raw FROM instruments
                WHERE UPPER(tradingSymbol) LIKE ? OR UPPER(securityId) LIKE ?
                LIMIT ?
            """, (pattern, pattern, limit))
            for r in cur.fetchall():
                inst = json.loads(r["raw"])
                candidate = {
                    "securityId": r["securityId"],
                    "tradingSymbol": inst.get("tradingSymbol"),
                    "exchange": inst.get("exchange"),
                    "segment": inst.get("segment"),
                    "expiry": inst.get("expiry"),
                    "lotSize": inst.get("lotSize"),
                    "raw": inst
                }
                if candidate not in results:
                    results.append(candidate)
                    if len(results) >= limit:
                        break
            conn.close()
        except Exception:
            logger.exception("SQLite fallback search failed")
    return results[:limit]

def cleanup_instruments() -> Dict:
    global instruments_by_symbol, instruments_by_id, last_updated
    instruments_by_symbol = {}
    instruments_by_id = {}
    last_updated = None
    try:
        if os.path.exists(INSTRUMENT_DB_FILE):
            os.remove(INSTRUMENT_DB_FILE)
            logger.info("Instruments DB removed: %s", INSTRUMENT_DB_FILE)
        return {"status": "success"}
    except Exception as e:
        logger.exception("Failed cleanup")
        return {"status": "failed", "message": str(e)}

# Scheduler
_scheduler: Optional[AsyncIOScheduler] = None

def start_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        logger.info("Scheduler already running")
        return
    _scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
    # Download job at configured time
    _scheduler.add_job(lambda: _run_download_job(), "cron",
                       hour=DAILY_DOWNLOAD_HOUR, minute=DAILY_DOWNLOAD_MINUTE,
                       id="download_instruments", replace_existing=True)
    # Cleanup job
    _scheduler.add_job(lambda: _run_cleanup_job(), "cron",
                       hour=DAILY_CLEANUP_HOUR, minute=DAILY_CLEANUP_MINUTE,
                       id="cleanup_instruments", replace_existing=True)
    _scheduler.start()
    logger.info("Scheduler started (download at %02d:%02d IST, cleanup at %02d:%02d IST)",
                DAILY_DOWNLOAD_HOUR, DAILY_DOWNLOAD_MINUTE, DAILY_CLEANUP_HOUR, DAILY_CLEANUP_MINUTE)
    # On startup: if DB current, load; else try immediate non-fatal download
    if db_is_current():
        logger.info("Today's DB exists; loading into memory")
        load_index_from_db()
    else:
        # best-effort immediate download but don't crash if fails
        try:
            logger.info("Today's DB not present; attempting immediate download")
            download_and_populate()
        except Exception as e:
            logger.exception("Startup download failed (non-fatal)")

def _run_download_job():
    try:
        download_and_populate()
        logger.info("Download job finished")
    except Exception:
        logger.exception("Download job failed")

def _run_cleanup_job():
    try:
        cleanup_instruments()
        logger.info("Cleanup job finished")
    except Exception:
        logger.exception("Cleanup job failed")
