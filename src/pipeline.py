from prefect import flow
from sqlalchemy import create_engine
from src.config.config import DB_CREATED
from src.config.constants import DOWNLOAD_PATH
from src.tasks.download import download
from src.tasks.validate import validate
from src.tasks.load import load_staging, cleanup, load_quarantine
from src.tasks.dbt import dbt_deps, dbt_run, dbt_test
from src.utils.logger import get_logger

logger = get_logger(__name__)

@flow(name="books-pipeline")
def run_pipeline():
    logger.info("=== Pipeline started ===")

    download(DOWNLOAD_PATH)

    engine = create_engine(DB_CREATED)
    validate(engine)
    load_staging(engine)
    load_quarantine(engine)
    dbt_deps()
    dbt_run()
    dbt_test()
    cleanup(engine)

    logger.info("=== Pipeline completed successfully ===")