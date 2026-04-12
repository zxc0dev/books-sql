from src.config.config import DB_CREATED, DB_MAIN
from src.config.sql import SQL_CREATE,  SQL_INDEXES, SQL_TRIGGERS

from sqlalchemy import text, create_engine
from src.utils.sql_execute import execute_sql_file
from src.utils.logger import get_logger

logger = get_logger(__name__)


def create_database():
    engine = create_engine(DB_MAIN)
    with engine.connect() as conn:
        conn.execute(text("commit"))
        result = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": DB_CREATED.database}
        )
        if not result.fetchone():
            conn.execute(text(f'CREATE DATABASE "{DB_CREATED.database}"'))
            logger.info(f"Database '{DB_CREATED.database}' created.")
        else:
            logger.info(f"Database '{DB_CREATED.database}' already exists.")


def create_tables():
    engine = create_engine(DB_CREATED)
    with engine.connect() as conn:
        for script in ["01_schema.sql", "02_raw_schema.sql", "03_quarantine_schema.sql"]:
            logger.info(f"Running {script}...")
            execute_sql_file(conn, SQL_CREATE / script)

        for script in sorted(p for p in SQL_INDEXES.iterdir() if p.name.endswith(".sql")):
            logger.info(f"Creating index: {script.name}")
            execute_sql_file(conn, script)

        for script in sorted(p for p in SQL_TRIGGERS.iterdir() if p.name.endswith(".sql")):
            logger.info(f"Creating trigger: {script.name}")
            execute_sql_file(conn, script)

        conn.commit()
        logger.info("=== Schema setup complete ===")

if __name__ == "__main__":
    create_database()
    create_tables()