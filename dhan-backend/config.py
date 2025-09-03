# config.py
import os

SQLITE_PATH = os.getenv("INSTRUMENTS_DB", "instruments.db")
