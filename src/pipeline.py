from prefect import flow
from sqlalchemy import create_engine
from src.config.config import DB_CREATED
from src.config.constants import DOWNLOAD_PATH
from src.tasks.download import download
from src.tasks.validate import validate
from src.tasks.load import load, cleanup
from src.utils.logger import get_logger

logger = get_logger(__name__)

@flow(name="books-pipeline")
def run_pipeline():
    logger.info("=== Pipeline started ===")

    download(DOWNLOAD_PATH)
    validate()

    engine = create_engine(DB_CREATED)
    with engine.connect() as conn:
        try:
            load(conn)
            cleanup(conn)
            conn.commit()
            logger.info("=== Pipeline completed successfully ===")
        except Exception as e:
            conn.rollback()
            logger.exception(f"Pipeline failed: {e}")
            raise

if __name__ == "__main__":
    run_pipeline()