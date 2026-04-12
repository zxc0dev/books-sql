from sqlalchemy import text
from prefect import task
from prefect.cache_policies import NO_CACHE
from src.utils.logger import get_logger
from src.config.paths import PROCESSED_DIR, QUARANTINE_DIR

logger = get_logger(__name__)

def _copy_csv(raw_conn, table: str, csv_path):
    cursor = raw_conn.cursor()
    with open(csv_path, "r", encoding="utf-8") as f:
        cursor.execute(f"TRUNCATE TABLE {table}")
        cursor.copy_expert(
            f"COPY {table} FROM STDIN WITH CSV HEADER DELIMITER ','",
            f
        )
    logger.info(f"Loaded {table} from {csv_path.name}")

@task(name="load-staging", cache_policy=NO_CACHE)
def load_staging(engine):
    logger.info("Loading CSVs into staging...")
    raw_conn = engine.raw_connection()
    try:
        _copy_csv(raw_conn, "raw_books",    PROCESSED_DIR / "books_good.csv")
        _copy_csv(raw_conn, "raw_users",    PROCESSED_DIR / "users_good.csv")
        _copy_csv(raw_conn, "raw_ratings",  PROCESSED_DIR / "ratings_good.csv")
        raw_conn.commit()
        logger.info("Staging loaded.")
    except Exception as e:
        raw_conn.rollback()
        logger.exception(f"Staging load failed: {e}")
        raise
    finally:
        raw_conn.close()

@task(name="load-quarantine", cache_policy=NO_CACHE)
def load_quarantine(engine):
    logger.info("Loading CSVs into quarantine...")
    raw_conn = engine.raw_connection()
    try:
        _copy_csv(raw_conn, "quarantine_books",    QUARANTINE_DIR / "books_quarantine.csv")
        _copy_csv(raw_conn, "quarantine_users",    QUARANTINE_DIR / "users_quarantine.csv")
        _copy_csv(raw_conn, "quarantine_ratings",  QUARANTINE_DIR / "ratings_quarantine.csv")
        raw_conn.commit()
        logger.info("Quarantine loaded.")
    except Exception as e:
        raw_conn.rollback()
        logger.exception(f"Quarantine load failed: {e}")
        raise
    finally:
        raw_conn.close()


@task(name="cleanup-raw", cache_policy=NO_CACHE)
def cleanup(engine):
    logger.info("Cleaning up raw...")
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        cursor.execute("TRUNCATE TABLE raw_books, raw_users, raw_ratings")
        raw_conn.commit()
    finally:
        raw_conn.close()