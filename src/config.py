import os
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
from datetime import datetime
from sqlalchemy.engine import URL

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5432),
    "main_dbname": os.getenv("MAIN_DB_NAME"),
    "created_dbname": os.getenv("CREATED_DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

def make_url(database):
    return URL.create(
        drivername="postgresql+psycopg2",
        username=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=database,
    )

DB_MAIN = make_url(DB_CONFIG["main_dbname"])
DB_CREATED = make_url(DB_CONFIG["created_dbname"])

ROOT_DIR = Path(__file__).resolve().parent.parent

RAW_DIR = ROOT_DIR / "data" / "01_raw"
PROCESSED_DIR = ROOT_DIR / "data" / "02_processed"
QUARANTINE_DIR = ROOT_DIR / "data" / "02_quarantine"
LOG_FILE = ROOT_DIR / "logs" / f"pipeline_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"

SQL_LOAD     = ROOT_DIR / "sql" / "sql_load"
SQL_CREATE   = ROOT_DIR / "sql" / "sql_create"
SQL_INDEXES  = ROOT_DIR / "sql" / "sql_indexes"
SQL_TRIGGERS = ROOT_DIR / "sql" / "sql_triggers"
SQL_VIEWS    = ROOT_DIR / "sql" / "sql_views"

CURRENT_YEAR = pd.Timestamp.now().year
ISBN_REGEX = r"[\dX\-]{8,13}"
ASIN_REGEX = r"B[\dA-Z]{9}"

DOWNLOAD_PATH = "arashnic/book-recommendation-dataset"