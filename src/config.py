import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5432),
    "main_dbname": os.getenv("MAIN_DB_NAME"),
    "created_dbname": os.getenv("CREATED_DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

DATA_DIR = "data/"
RAW_DIR  = "data/raw/"