"""
db.py - DuckDB Connection Management
======================================
Centralized database access for the backtesting system.
DuckDB is an embedded analytical database — no server required.
The database is a single file on disk.
"""

import os
import logging

import duckdb

logger = logging.getLogger("db")

_SCHEMA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")


def get_db_path() -> str:
    """Resolve the database file path from config or default."""
    try:
        from config import DB_PATH
        return DB_PATH
    except ImportError:
        return os.getenv("DB_PATH", "data/trading.duckdb")


def get_connection(db_path: str = None) -> duckdb.DuckDBPyConnection:
    """
    Get a DuckDB connection. Creates the database file and parent
    directories if they don't exist.
    """
    if db_path is None:
        db_path = get_db_path()

    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    return duckdb.connect(db_path)


def init_schema(db_path: str = None):
    """
    Initialize the database schema from schema.sql.
    Safe to call multiple times (uses IF NOT EXISTS).
    """
    conn = get_connection(db_path)
    try:
        with open(_SCHEMA_FILE) as f:
            schema_sql = f.read()

        # Split and execute each statement for robustness
        for statement in schema_sql.split(";"):
            cleaned = statement.strip()
            # Skip empty or comment-only blocks
            code_lines = [
                l for l in cleaned.split("\n")
                if l.strip() and not l.strip().startswith("--")
            ]
            if code_lines:
                conn.execute(cleaned)

        logger.info(f"Schema initialized from {_SCHEMA_FILE}")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_schema()
    print("Schema initialized successfully.")
