# scheduler.py
from __future__ import annotations

import io
import logging
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import numpy as np
import requests
from apscheduler.schedulers.background import BackgroundScheduler

LOG = logging.getLogger("scheduler")

DEFAULT_DB = os.getenv("INSTRUMENTS_DB", "instruments.db")
MASTER_URL = os.getenv("DHAN_MASTER_URL", "https://images.dhan.co/api-data/api-scrip-master.csv")

# ---- helpers ----
def _connect(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path, timeout=30, isolation_level=None)

def _ensure_indexes(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
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

# Column candidates from Dhan CSV (actual headers)
CANDIDATES = {
    "securityId": ["SEM_SMST_SECURITY_ID"],
    "tradingSymbol": ["SEM_TRADING_SYMBOL", "SM_SYMBOL_NAME"],
    "segment_code": ["SEM_EXM_EXCH_ID"],
    "segment_text": ["SEM_SEGMENT"],
    "lotSize": ["SEM_LOT_UNITS"],
    "expiry": ["SEM_EXPIRY_DATE"],
}

SEG_MAP_CODE = {"1": "NSE_EQ", "2": "BSE_EQ", "13": "NSE_FNO", "50": "MCX"}

def _pick(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    for n in names:
        if n in df.columns:
            return n
    return None

def _norm_segment(code, txt, trading_symbol: str = "") -> Optional[str]:
    # Check trading symbol first for options
    t = str(trading_symbol).upper()
    if any(idx in t for idx in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY")):
        return "NSE_FNO"
    
    if pd.notna(code):
        if isinstance(code, (int, float, np.integer, np.floating)) and not pd.isna(code):
            s = str(int(code))
        else:
            s = str(code).strip()
        if s in SEG_MAP_CODE:
            return SEG_MAP_CODE[s]
    if pd.notna(txt):
        t = str(txt).upper()
        if "FNO" in t or "DERIV" in t:
            return "NSE_FNO"
        if "MCX" in t:
            return "MCX"
        if "BSE" in t:
            return "BSE_EQ"
        if "NSE" in t:
            return "NSE_EQ"
        if "EQ" in t:
            return "NSE_EQ"
    return None

# ------------- Instrument master ops -------------
def download_and_populate(db_path: Optional[str] = None) -> Dict[str, str | int]:
    """Download Dhan scrip master and rebuild instruments with real columns."""
    db_path = db_path or DEFAULT_DB
    LOG.info("⏬ Downloading scrip master from %s", MASTER_URL)
    try:
        r = requests.get(MASTER_URL, timeout=60)
        r.raise_for_status()
        raw = r.content
        if len(raw) < 10 * 1024 * 1024:
            raise RuntimeError(f"CSV too small: {len(raw)} bytes — aborting")
        df = pd.read_csv(io.BytesIO(raw), low_memory=False)
        if len(df) < 50000:
            raise RuntimeError(f"Too few rows: {len(df)} — aborting")

        sec_col = _pick(df, CANDIDATES["securityId"])
        ts_col  = _pick(df, CANDIDATES["tradingSymbol"])
        segc    = _pick(df, CANDIDATES["segment_code"])
        segt    = _pick(df, CANDIDATES["segment_text"])
        lot_col = _pick(df, CANDIDATES["lotSize"])
        exp_col = _pick(df, CANDIDATES["expiry"])

        if not sec_col or not ts_col:
            raise RuntimeError("Required columns missing (securityId/tradingSymbol). Dhan CSV format changed?")

        out = pd.DataFrame()
        out["securityId"] = df[sec_col].astype(str)
        out["tradingSymbol"] = df[ts_col].astype(str)
        out["segment"] = [
            _norm_segment(df[segc].iloc[i] if segc else None, df[segt].iloc[i] if segt else None, df[ts_col].iloc[i])
            for i in range(len(df))
        ]
        out["lotSize"] = pd.to_numeric(df[lot_col], errors="coerce").fillna(1).astype(int) if lot_col else 1
        out["expiry"] = df[exp_col].astype(str) if exp_col else None

        ok_rows = (
            out["securityId"].notna() & (out["securityId"].str.strip() != "") &
            out["tradingSymbol"].notna() & (out["tradingSymbol"].str.strip() != "")
        )
        out = out[ok_rows]

        if out.empty or out["tradingSymbol"].nunique() < 50000:
            raise RuntimeError("Validation failed: too few usable rows after normalization")

        tmp_path = db_path + ".tmp"
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

        conn = sqlite3.connect(tmp_path)
        out.to_sql("instruments", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tradingsymbol ON instruments(tradingSymbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_securityid ON instruments(securityId)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_segment ON instruments(segment)")
        conn.execute("VACUUM")
        conn.commit()
        conn.close()

        os.replace(tmp_path, db_path)
        LOG.info("✅ instruments saved to %s (rows=%d)", db_path, len(out))
        return {"status": "success", "rows": int(len(out))}
    except Exception as e:
        LOG.exception("❌ download_and_populate failed")
        return {"status": "error", "message": str(e)}

def cleanup_instruments(db_path: Optional[str] = None) -> Dict[str, str]:
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
    if not query or not os.path.exists(db_path):
        return []
    with sqlite3.connect(db_path) as conn:
        sql = """
          SELECT securityId, tradingSymbol, segment, lotSize, expiry
          FROM instruments
          WHERE tradingSymbol LIKE ?
            AND (
              UPPER(segment) = UPPER(?)
              OR (segment IS NULL OR segment = '')
            )
          ORDER BY tradingSymbol
          LIMIT ?
        """
        rows = conn.execute(sql, (f"%{query.strip()}%", (segment or "").strip(), int(limit))).fetchall()
        cols = ["securityId", "tradingSymbol", "segment", "lotSize", "expiry"]
        return [dict(zip(cols, r)) for r in rows]

def resolve_symbol(db_path: Optional[str], symbol: str, segment: str) -> Optional[Dict[str, str | int]]:
    if not symbol or not os.path.exists(db_path):
        return None
    with sqlite3.connect(db_path) as conn:
        sql = """
          SELECT securityId, tradingSymbol, segment, lotSize, expiry
          FROM instruments
          WHERE LOWER(tradingSymbol) = LOWER(?)
            AND UPPER(segment) = UPPER(?)
          LIMIT 1
        """
        row = conn.execute(sql, (symbol.strip(), (segment or "").strip())).fetchone()
        if not row:
            return None
        cols = ["securityId", "tradingSymbol", "segment", "lotSize", "expiry"]
        return dict(zip(cols, row))

# ------------- Scheduler -------------
_sched: Optional[BackgroundScheduler] = None

def ensure_fresh_db(db_path: str) -> bool:
    """
    Ensure instruments DB exists and is from today.
    Returns True if a fresh download was triggered.
    """
    from datetime import datetime
    if not os.path.exists(db_path):
        download_and_populate(db_path)
        return True
    try:
        mtime = datetime.fromtimestamp(os.path.getmtime(db_path))
        if mtime.date() != datetime.now().date():
            download_and_populate(db_path)
            return True
    except Exception:
        download_and_populate(db_path)
        return True
    return False

def start_scheduler(db_path: Optional[str] = None) -> BackgroundScheduler:
    global _sched
    
    # Optional: gate with environment variable
    if os.getenv("RUN_SCHEDULER", "true").lower() not in ("true", "1", "yes"):
        LOG.info("Scheduler disabled by RUN_SCHEDULER env var")
        return None
    
    if _sched:
        return _sched

    db_path = db_path or DEFAULT_DB
    LOG.info("⏳ Starting scheduler (IST): 08:00 download, 15:45 cleanup | db=%s", db_path)

    _sched = BackgroundScheduler(timezone="Asia/Kolkata")
    _sched.add_job(
        lambda: download_and_populate(db_path), 
        "cron", 
        hour=8, 
        minute=0, 
        id="download_job", 
        replace_existing=True,
        misfire_grace_time=300,  # 5 minutes
        max_instances=1
    )
    _sched.add_job(
        lambda: cleanup_instruments(db_path), 
        "cron", 
        hour=15, 
        minute=45, 
        id="cleanup_job", 
        replace_existing=True,
        misfire_grace_time=300,  # 5 minutes
        max_instances=1
    )
    try:
        _sched.start()
        LOG.info("✅ Scheduler started")
    except Exception:
        LOG.exception("Failed to start APScheduler (maybe already running in this process)")

    return _sched
