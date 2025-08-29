# scheduler.py
from __future__ import annotations

import io
import logging
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests
from apscheduler.schedulers.background import BackgroundScheduler

LOG = logging.getLogger("scheduler")

# Defaults (can be overridden by env)
DEFAULT_DB = os.getenv("INSTRUMENTS_DB", "instruments.db")
MASTER_URL = os.getenv(
    "DHAN_MASTER_URL",
    "https://images.dhan.co/api-data/api-scrip-master.csv",
)

# ------------- SQLite helpers -------------
def _connect(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path, timeout=30, isolation_level=None)

def _ensure_indexes(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    # Create table if missing (we'll replace with the CSV later)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS instruments (
            securityId TEXT,
            tradingSymbol TEXT,
            segment TEXT,
            lotSize INTEGER,
            expiry TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tradingsymbol ON instruments(tradingSymbol)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_segment ON instruments(segment)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_securityid ON instruments(securityId)")
    conn.commit()

# ------------- Instrument master ops -------------
def download_and_populate(db_path: Optional[str] = None) -> Dict[str, str | int]:
    """Download the Dhan scrip master CSV and (re)build the instruments table."""
    db_path = db_path or DEFAULT_DB
    LOG.info("⏬ Downloading scrip master from %s", MASTER_URL)
    try:
        # Download + validate size & rows
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        r = requests.get(url, timeout=60)
        r.raise_for_status()

        raw = r.content                       # bytes
        dl_bytes = len(raw)
        if dl_bytes < 10 * 1024 * 1024:       # 10 MB sanity threshold
            raise RuntimeError(f"CSV too small: {dl_bytes} bytes — aborting to avoid corrupt DB")

        df = pd.read_csv(io.BytesIO(raw))
        if len(df) < 50000:                   # Dhan master usually has many more rows
            raise RuntimeError(f"Too few rows: {len(df)} — aborting to avoid corrupt DB")

        # Normalize column names (case-insensitive match)
        cols = {c.lower(): c for c in df.columns}
        def pick(name: str) -> str:
            # return the real column name in df for a lowercase key
            return cols.get(name.lower(), name)

        # Build a minimal, stable schema the app needs
        needed = {
            "securityId": pick("securityId"),
            "tradingSymbol": pick("tradingSymbol"),
            "segment": pick("segment"),
            "lotSize": pick("lotSize"),
            "expiry": pick("expiry"),
        }

        for want, real in list(needed.items()):
            if real not in df.columns:
                # If missing, create empty column
                df[want] = None
                needed[want] = want

        df_norm = df[[needed["securityId"], needed["tradingSymbol"], needed["segment"], needed["lotSize"], needed["expiry"]]].copy()
        df_norm.columns = ["securityId", "tradingSymbol", "segment", "lotSize", "expiry"]

        # Write atomically using a temp DB (so you never end up with 20 KB half-writes)
        tmp_path = db_path + ".tmp"
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

        conn = sqlite3.connect(tmp_path)
        cur = conn.cursor()

        # keep your current CREATE TABLE/INDEX logic here, then:
        df_norm.to_sql("instruments", conn, if_exists="replace", index=False)

        # indexes (keep yours; add if missing)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tradingsymbol ON instruments(tradingSymbol)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_securityid ON instruments(securityId)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_segment ON instruments(segment)")

        # optional: Light vacuum to compact temp DB before swap
        cur.execute("VACUUM")
        conn.commit()
        conn.close()

        # Atomic replace so readers never see a broken DB
        os.replace(tmp_path, db_path)

        LOG.info("✅ instruments saved to %s (rows=%d)", db_path, len(df_norm))
        return {"status": "success", "rows": int(len(df_norm))}
    except Exception as e:
        LOG.exception("❌ download_and_populate failed")
        return {"status": "error", "message": str(e)}

def cleanup_instruments(db_path: Optional[str] = None) -> Dict[str, str]:
    """Delete the instruments DB if present."""
    db_path = db_path or DEFAULT_DB
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
            LOG.info("🗑 Deleted %s", db_path)
            return {"message": "DB removed"}
        LOG.info("ℹ️ %s not present; nothing to delete", db_path)
        return {"message": "DB not present"}
    except Exception as e:
        LOG.exception("❌ cleanup_instruments failed")
        return {"message": f"cleanup failed: {e}"}

def db_is_current(db_path: Optional[str] = None) -> bool:
    """True if instruments DB exists and was modified *today* (local date)."""
    db_path = db_path or DEFAULT_DB
    if not os.path.exists(db_path):
        return False
    try:
        mtime = datetime.fromtimestamp(os.path.getmtime(db_path))
        return mtime.date() == datetime.now().date()
    except Exception:
        return False

# ------------- Query helpers -------------
def symbol_search(db_path: Optional[str], query: str, segment: str, limit: int = 30) -> List[Dict[str, str | int]]:
    """LIKE search by tradingSymbol + exact segment."""
    db_path = db_path or DEFAULT_DB
    if not query or not segment:
        return []
    if not os.path.exists(db_path):
        return []

    sql = """
        SELECT securityId, tradingSymbol, segment, lotSize, expiry
        FROM instruments
        WHERE tradingSymbol LIKE ? AND segment = ?
        LIMIT ?
    """
    conn = _connect(db_path)
    try:
        rows = conn.execute(sql, (f"%{query.strip()}%", segment.strip(), int(limit))).fetchall()
        cols = ["securityId", "tradingSymbol", "segment", "lotSize", "expiry"]
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()

def resolve_symbol(db_path: Optional[str], symbol: str, segment: str) -> Optional[Dict[str, str | int]]:
    """Exact tradingSymbol + segment → 1 record with securityId, lotSize, expiry."""
    db_path = db_path or DEFAULT_DB
    if not symbol or not segment or not os.path.exists(db_path):
        return None
    sql = """
        SELECT securityId, tradingSymbol, segment, lotSize, expiry
        FROM instruments
        WHERE LOWER(tradingSymbol) = LOWER(?) AND segment = ?
        LIMIT 1
    """
    conn = _connect(db_path)
    try:
        row = conn.execute(sql, (symbol.strip(), segment.strip())).fetchone()
        if not row:
            return None
        cols = ["securityId", "tradingSymbol", "segment", "lotSize", "expiry"]
        return dict(zip(cols, row))
    finally:
        conn.close()

# ------------- Scheduler -------------
_sched: Optional[BackgroundScheduler] = None

def ensure_fresh_db(db_path: str):
    # If DB missing or suspiciously small, force a download now
    if (not os.path.exists(db_path)) or (os.path.getsize(db_path) < 10 * 1024 * 1024):
        download_and_populate(db_path)

def start_scheduler(db_path: Optional[str] = None) -> BackgroundScheduler:
    """
    Start cron jobs:
      - 08:00 Asia/Kolkata: download_and_populate(db_path)
      - 15:45 Asia/Kolkata: cleanup_instruments(db_path)
    """
    global _sched
    if _sched:
        return _sched

    db_path = db_path or DEFAULT_DB
    LOG.info("⏳ Starting scheduler (IST): 08:00 download, 15:45 cleanup | db=%s", db_path)

    _sched = BackgroundScheduler(timezone="Asia/Kolkata")
    _sched.add_job(lambda: download_and_populate(db_path), "cron", hour=8, minute=0, id="download_job")
    _sched.add_job(lambda: cleanup_instruments(db_path), "cron", hour=15, minute=45, id="cleanup_job")
    _sched.start()

    LOG.info("✅ Scheduler started")
    return _sched
