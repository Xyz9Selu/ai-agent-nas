"""Database module for document_rows persistence."""

import json
import logging
import os
from typing import Any

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)

def _get_table_name() -> str:
    name = os.getenv("TABLE_NAME", "document_rows")
    if not name.replace("_", "").isalnum():
        raise ValueError("TABLE_NAME must be alphanumeric with underscores only")
    return name


def get_db_connection() -> psycopg.Connection | None:
    """Build PostgreSQL connection from environment variables.

    Returns:
        Connection if all required env vars are set, None otherwise.
    """
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    if not all([host, dbname, user, password]):
        logger.warning(
            "Database connection skipped: missing POSTGRES_HOST, POSTGRES_DB, "
            "POSTGRES_USER, or POSTGRES_PASSWORD"
        )
        return None

    try:
        conn = psycopg.connect(
            host=host,
            port=int(port),
            dbname=dbname,
            user=user,
            password=password,
            row_factory=dict_row,
        )
        return conn
    except Exception as e:
        logger.error("Failed to connect to database: %s", e)
        raise


def ensure_document_rows_table(conn: psycopg.Connection) -> None:
    """Create document_rows table if it does not exist."""
    table = _get_table_name()
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                dataset_id TEXT,
                row_data JSONB
            )
            """
        )
    conn.commit()


def insert_rows_batch(
    conn: psycopg.Connection,
    dataset_id: str,
    rows: list[dict[str, Any]],
) -> int:
    """Bulk insert rows into document_rows.

    Args:
        conn: Database connection.
        dataset_id: Dataset identifier for the rows.
        rows: List of record dicts to insert (will be stored as JSONB).

    Returns:
        Number of rows inserted.
    """
    if not rows:
        return 0

    table = _get_table_name()
    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO {table} (dataset_id, row_data)
            VALUES (%s, %s)
            """,
            [(dataset_id, json.dumps(row, ensure_ascii=False)) for row in rows],
        )
    conn.commit()
    return len(rows)
