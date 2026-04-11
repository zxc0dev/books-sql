from sqlalchemy import create_engine, text
from src.logger import get_logger

logger = get_logger(__name__)

def execute_sql_file(conn, path, params=None):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()

    # split into individual statements, skip empty ones
    statements = [s.strip() for s in sql.split(";")]
    statements = [s for s in statements if s and not s.startswith("--")]

    for statement in statements:
        conn.execute(text(statement), params or {})

    logger.info(f"Executed: {path}")

def execute_sql(conn, sql: str, params=None):
    conn.execute(text(sql), params or {})