import os
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5432),
    "main_dbname": os.getenv("MAIN_DB_NAME"),
    "created_dbname": os.getenv("CREATED_DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

ROOT_DIR = Path(__file__).resolve().parent.parent

RAW_DIR = ROOT_DIR / "data" / "01_raw"
PROCESSED_DIR = ROOT_DIR / "data" / "02_processed"
QUARANTINE_DIR = ROOT_DIR / "data" / "02_quarantine"
LOG_FILE = ROOT_DIR / "pipeline.log"

CURRENT_YEAR = pd.Timestamp.now().year
ISBN_REGEX = r"[\dX\-]{8,13}"
ASIN_REGEX = r"B[\dA-Z]{9}"

DOWNLOAD_PATH = "arashnic/book-recommendation-dataset"