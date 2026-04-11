from src.config import (DB_CREATED, DB_MAIN, SQL_CREATE, SQL_INDEXES, SQL_TRIGGERS)

from sqlalchemy import text
from src.db import execute_sql_file
from src.logger import get_logger
from sqlalchemy import create_engine

logger = get_logger(__name__)


def create_database():
    engine = create_engine(DB_MAIN)
    with engine.connect() as conn:
        conn.execute(text("commit"))
        result = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": DB_CREATED.database}  # ← .database on URL object
        )
        if not result.fetchone():
            conn.execute(text(f'CREATE DATABASE "{DB_CREATED.database}"'))
            logger.info(f"Database '{DB_CREATED.database}' created.")
        else:
            logger.info(f"Database '{DB_CREATED.database}' already exists.")

def create_tables():
    engine = create_engine(DB_CREATED)
    with engine.connect() as conn:

        for script in ["03_schema.sql", "04_staging_schema.sql", "05_quarantine_schema.sql"]:
            logger.info(f"Running {script}...")
            execute_sql_file(conn, SQL_CREATE / script)

        for script in sorted(SQL_INDEXES.glob("*.sql")):
            logger.info(f"Creating index: {script.name}")
            execute_sql_file(conn, script)

        for script in sorted(SQL_TRIGGERS.glob("*.sql")):
            logger.info(f"Creating trigger: {script.name}")
            execute_sql_file(conn, script)

        conn.commit()
        logger.info("=== Schema setup complete ===")

if __name__ == "__main__":
    create_database()
    create_tables()