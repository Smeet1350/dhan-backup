# config.py
import os
from pathlib import Path

BASE_DIR = Path(__file__).parent

# Instruments DB under backend/data (create dir if missing)
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
SQLITE_PATH = os.getenv("INSTRUMENTS_DB", str(DATA_DIR / "instruments.db"))

# Alerts log under backend/logs (create dir if missing)
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
ALERTS_LOG_PATH = os.getenv("ALERTS_LOG_PATH", str(LOG_DIR / "alerts.log"))
