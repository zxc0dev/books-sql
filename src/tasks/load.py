from prefect import task
from prefect.cache_policies import NO_CACHE
from src.utils.sql_execute import execute_sql_file
from src.utils.logger import get_logger
from src.config.sql import SQL_LOAD, SQL_CLEANUP

logger = get_logger(__name__)

def _sql_files(directory):
    return sorted(
        (p for p in directory.iterdir() if p.name.endswith(".sql")),
        key=lambda p: p.name,
    )

@task(name="load-sql", cache_policy=NO_CACHE)
def load(conn):
    for script in _sql_files(SQL_LOAD):
        logger.info(f"Executing loading script: {script.name}")
        execute_sql_file(conn, script)

@task(name="cleanup-staging", cache_policy=NO_CACHE)
def cleanup(conn):
    logger.info("Cleaning up staging...")
    execute_sql_file(conn, SQL_CLEANUP / "cleanup_staging.sql")