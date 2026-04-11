from pathlib import Path
from src.db import execute_sql_file
from src.validate import run
from src.download_data import download_data
from src.logger import get_logger
from sqlalchemy import create_engine
from src.config import (DB_CREATED, DOWNLOAD_PATH, SQL_LOAD, SQL_CREATE, ROOT_DIR)

logger = get_logger(__name__)

def run_pipeline():
    logger.info("=== Pipeline started ===")

    logger.info("Downloading data...")
    logger.info(download_data(DOWNLOAD_PATH))

    logger.info("Validating data...")
    books_c, users_c, ratings_c, books_b, users_b, ratings_b = run()

    engine = create_engine(DB_CREATED)

    with engine.connect() as conn:
        try:
            logger.info("Loading to staging...")
            execute_sql_file(conn, ROOT_DIR / SQL_LOAD / "01_load_staging.sql")

            logger.info("Running data integrity check...")
            execute_sql_file(conn, ROOT_DIR / SQL_LOAD / "02_data_integrity_check.sql")

            for name, file in [
                ("authors",    "03_load_authors.sql"),
                ("publishers", "04_load_publishers.sql"),
                ("books",      "05_load_books.sql"),
                ("users",      "06_load_users.sql"),
                ("ratings",    "07_load_ratings.sql"),
            ]:
                logger.info(f"Loading {name}...")
                execute_sql_file(conn, ROOT_DIR / SQL_LOAD / file)

            logger.info("Cleaning up staging...")
            execute_sql_file(conn, ROOT_DIR / SQL_CREATE / "06_cleanup_staging.sql")

            conn.commit()
            logger.info("=== Pipeline completed successfully ===")

        except Exception as e:
            conn.rollback()
            logger.exception(f"Pipeline failed: {e}")
            raise

if __name__ == "__main__":
    run_pipeline()