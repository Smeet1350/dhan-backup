from apscheduler.schedulers.background import BackgroundScheduler
import requests, sqlite3, os, csv
from datetime import datetime

DB_FILE = "instruments.db"
INSTRUMENTS_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

def download_instruments():
    today = datetime.now().strftime("%Y-%m-%d")

    # Check if today's DB already exists
    if os.path.exists(DB_FILE):
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute("SELECT date('now') == date(created_at) FROM meta")
        is_today = cur.fetchone()[0]
        conn.close()
        if is_today == 1:
            print(f"[{datetime.now()}] ✅ Instruments already downloaded today.")
            return

    print(f"[{datetime.now()}] ⬇️ Downloading instruments...")
    try:
        response = requests.get(INSTRUMENTS_URL)
        response.raise_for_status()

        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()

        # Drop old tables
        cur.execute("DROP TABLE IF EXISTS instruments")
        cur.execute("DROP TABLE IF EXISTS meta")

        # Create instruments table
        cur.execute("""
            CREATE TABLE instruments (
                securityId TEXT,
                tradingSymbol TEXT,
                exchangeSegment TEXT,
                instrument TEXT,
                expiry TEXT,
                lotSize INTEGER,
                underlying TEXT
            )
        """)

        # Create metadata table to track when DB created
        cur.execute("""
            CREATE TABLE meta (
                created_at TEXT
            )
        """)
        cur.execute("INSERT INTO meta VALUES (datetime('now'))")

        # Parse CSV
        reader = csv.DictReader(response.text.splitlines())
        for row in reader:
            cur.execute("""
                INSERT INTO instruments
                (securityId, tradingSymbol, exchangeSegment, instrument, expiry, lotSize, underlying)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get("security_id"),
                row.get("trading_symbol"),
                row.get("exchange_segment"),
                row.get("instrument"),
                row.get("expiry"),
                int(row.get("lot_size") or 1),
                row.get("underlying_symbol")
            ))

        conn.commit()
        conn.close()
        print("✅ Instruments updated successfully.")

    except Exception as e:
        print("❌ Failed to download instruments:", str(e))


def cleanup_instruments():
    print(f"[{datetime.now()}] 🧹 Cleaning up instruments...")
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print("✅ Instruments deleted after market close.")
    else:
        print("⚠️ No instrument file found to delete.")


def start_scheduler():
    scheduler = BackgroundScheduler()
    # Download every day at 8:00 AM
    scheduler.add_job(download_instruments, "cron", hour=8, minute=0)
    # Cleanup every day at 4:00 PM
    scheduler.add_job(cleanup_instruments, "cron", hour=16, minute=0)
    scheduler.start()

    # Safety: if backend starts during market hours and DB not present, download immediately
    now = datetime.now()
    if 8 <= now.hour < 16 and not os.path.exists(DB_FILE):
        download_instruments()
