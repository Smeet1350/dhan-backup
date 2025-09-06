# scheduler.py
import os
import sqlite3
import tempfile
import threading
import logging
from pathlib import Path
import requests
import pandas as pd

LOG = logging.getLogger(__name__)

MASTER_URL_COMPACT = "https://images.dhan.co/api-data/api-scrip-master.csv"
MASTER_URL_DETAILED = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"


def _ensure_dir(path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def download_and_populate(db_path: str, master_url: str = MASTER_URL_COMPACT, chunksize: int = 200000):
    """Download the instrument master CSV as a stream and populate sqlite in a memory friendly, chunked way.

    Writes the remote CSV to a temp file, reads in pandas chunks, writes to a temp sqlite DB,
    then atomically swaps the DB to avoid leaving partial state.
    """
    LOG.info("Starting download_and_populate db=%s", db_path)
    _ensure_dir(db_path)

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv")
    os.close(tmp_fd)
    try:
        with requests.get(master_url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(tmp_path, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        fh.write(chunk)

        reader = pd.read_csv(tmp_path, chunksize=chunksize, low_memory=True)

        first = True
        tmp_db_fd, tmp_db_path = tempfile.mkstemp(suffix=".db")
        os.close(tmp_db_fd)
        try:
            conn = sqlite3.connect(tmp_db_path)
            with conn:
                for chunk in reader:
                    # Optionally filter or rename columns here to reduce DB size
                    if first:
                        chunk.to_sql("instruments", conn, if_exists="replace", index=False)
                        first = False
                    else:
                        chunk.to_sql("instruments", conn, if_exists="append", index=False)
            conn.close()

            # Atomic replace of DB file
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            backup_path = f"{db_path}.bak"
            try:
                if Path(db_path).exists():
                    Path(db_path).replace(backup_path)
                Path(tmp_db_path).replace(db_path)
                LOG.info("Instrument DB updated: %s", db_path)
                if Path(backup_path).exists():
                    Path(backup_path).unlink(missing_ok=True)
            except Exception:
                LOG.exception("Failed to replace DB atomically; rolling back")
                if Path(backup_path).exists():
                    Path(backup_path).replace(db_path)
                raise
        finally:
            try:
                Path(tmp_db_path).unlink(missing_ok=True)
            except Exception:
                pass
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except Exception:
            pass


def find_instrument(db_path: str, trading_symbol: str):
    """Find a single instrument by trading symbol. Closes connection reliably."""
    p = Path(db_path)
    if not p.exists():
        return None
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM instruments WHERE tradingSymbol = ? LIMIT 1", (trading_symbol,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [c[0] for c in cur.description]
        return dict(zip(cols, row))
    finally:
        conn.close()


def resolve_symbol(db_path: str, symbol: str, segment_hint: str = "NSE_EQ"):
    """Basic resolution wrapper; expand normalization logic as needed."""
    return find_instrument(db_path, symbol)


def schedule_periodic_download(db_path: str, master_url: str = MASTER_URL_COMPACT, hour: int = 8):
    """Start a simple background thread to periodically refresh the DB (24h interval)."""
    def runner():
        import time
        while True:
            try:
                download_and_populate(db_path, master_url=master_url)
            except Exception:
                LOG.exception("Periodic download failed")
            time.sleep(24 * 3600)

    t = threading.Thread(target=runner, daemon=True)
    t.start()


# Legacy compatibility functions for existing code
def ensure_fresh_db(db_path: str) -> bool:
    """Ensure instruments DB exists and is from today.
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


def db_is_current(db_path: str) -> bool:
    """Check if database is current (from today)."""
    from datetime import datetime
    if not os.path.exists(db_path):
        return False
    try:
        mtime = datetime.fromtimestamp(os.path.getmtime(db_path))
        return mtime.date() == datetime.now().date()
    except Exception:
        return False


def symbol_search(db_path: str, query: str, segment: str, limit: int = 30):
    """Search for symbols in the database."""
    if not query or not os.path.exists(db_path):
        return []
    conn = sqlite3.connect(db_path)
    try:
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
    finally:
        conn.close()


def cleanup_instruments(db_path: str):
    """Clean up old instrument database."""
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


def start_scheduler(db_path: str = None):
    """Start the background scheduler for periodic downloads."""
    if db_path is None:
        db_path = os.getenv("INSTRUMENTS_DB", "instruments.db")
    schedule_periodic_download(db_path)
    LOG.info("✅ Scheduler started")