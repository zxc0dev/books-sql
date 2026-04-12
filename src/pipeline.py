from src.sql_execute import execute_sql_file
from src.validate import run
from src.download_data import download_data
from src.logger import get_logger
from sqlalchemy import create_engine
from src.config.config import DB_CREATED
from src.config.sql import SQL_LOAD, SQL_CLEANUP
from src.config.constants import DOWNLOAD_PATH

logger = get_logger(__name__)

def _sql_files(directory):
    return sorted(
        (p for p in directory.iterdir() if p.name.endswith(".sql")),
        key=lambda p: p.name,
    )

def run_pipeline():
    logger.info("=== Pipeline started ===")

    logger.info("Downloading data...")
    logger.info(download_data(DOWNLOAD_PATH))

    logger.info("Validating data...")
    books_c, users_c, ratings_c, books_b, users_b, ratings_b = run()

    engine = create_engine(DB_CREATED)

    with engine.connect() as conn:
        try:
            for script in _sql_files(SQL_LOAD):
                logger.info(f"Executing loading script: {script.name}")
                execute_sql_file(conn, script)

            logger.info("Cleaning up staging...")
            execute_sql_file(conn, SQL_CLEANUP / "cleanup_staging.sql")

            conn.commit()
            logger.info("=== Pipeline completed successfully ===")

        except Exception as e:
            conn.rollback()
            logger.exception(f"Pipeline failed: {e}")
            raise

if __name__ == "__main__":
    run_pipeline()