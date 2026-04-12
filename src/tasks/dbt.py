import subprocess
from prefect import task
from prefect.cache_policies import NO_CACHE
from src.utils.logger import get_logger
from src.config.paths import ROOT_DIR

logger = get_logger(__name__)

DBT_DIR = str(ROOT_DIR / "dbt")

def _run_dbt(command: str):
    result = subprocess.run(
        ["dbt", command, "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
        capture_output=True,
        text=True
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt {command} failed")

@task(name="dbt-run", cache_policy=NO_CACHE)
def dbt_run():
    logger.info("Running dbt models...")
    _run_dbt("run")

@task(name="dbt-test", cache_policy=NO_CACHE)
def dbt_test():
    logger.info("Running dbt tests...")
    _run_dbt("test")