"""Database module for document_rows persistence and import tables."""

import json
import logging
import os
import re
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)

IMP_TABLE_PREFIX = "imp_"
MAX_IDENTIFIER_LEN = 63


def _sanitize_identifier_part(s: str) -> str:
    """Normalize a string to a valid PostgreSQL identifier fragment (lowercase, alphanumeric + underscore)."""
    if not s or not isinstance(s, str):
        return ""
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _sanitize_prefix(prefix: str | None) -> str:
    """Normalize prefix to a valid PostgreSQL identifier (lowercase, alphanumeric + underscore)."""
    if not prefix or not isinstance(prefix, str):
        return ""
    s = prefix.lower().strip()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def sanitize_table_name(
    document_name: str,
    sheet_name: str,
    prefix: str | None = None,
) -> str:
    """Build base table name from document and sheet; optionally add prefix; truncate to 63 chars.
    If prefix is not provided, the table name has no prefix (base only).
    """
    doc = _sanitize_identifier_part(document_name or "")
    sheet = _sanitize_identifier_part(sheet_name or "")
    parts = [p for p in (doc, sheet) if p]
    base = "_".join(parts) if parts else "unnamed"
    prefix_clean = _sanitize_prefix(prefix)
    if prefix_clean:
        full = prefix_clean + "_" + base
    else:
        full = base
    if len(full) > MAX_IDENTIFIER_LEN:
        full = full[:MAX_IDENTIFIER_LEN]
    return full


def sanitize_column_names(column_names: list[str]) -> list[str]:
    """Sanitize and de-duplicate column names for SQL. Returns list of valid identifiers."""
    used: set[str] = set()
    result: list[str] = []
    for name in column_names:
        raw = _sanitize_identifier_part(str(name).strip() if name else "")
        if not raw or raw[0].isdigit():
            raw = "col_" + raw if raw else "col"
        base = raw[:MAX_IDENTIFIER_LEN]
        if base in used:
            k = 1
            while f"{base}_{k}" in used:
                k += 1
            base = f"{base}_{k}"[:MAX_IDENTIFIER_LEN]
        used.add(base)
        result.append(base)
    return result


def table_exists(conn: psycopg.Connection, table_name: str) -> bool:
    """Return True if a table with the given name exists in the current schema."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = current_schema() AND table_name = %s
            """,
            (table_name,),
        )
        return cur.fetchone() is not None


def get_table_columns(conn: psycopg.Connection, table_name: str) -> list[str]:
    """Return column names for the table in ordinal order (excluding system columns like id)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        rows = cur.fetchall()
    return [r["column_name"] for r in rows if r["column_name"] != "id"]


def index_exists(conn: psycopg.Connection, table_name: str, index_name: str) -> bool:
    """Return True if an index with the given name exists on the table in the current schema."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM pg_indexes
            WHERE schemaname = current_schema()
              AND tablename = %s
              AND indexname = %s
            """,
            (table_name, index_name),
        )
        return cur.fetchone() is not None


def rename_table(conn: psycopg.Connection, old_name: str, new_name: str) -> None:
    """Rename a table. new_name must be valid and unique."""
    sql = psycopg.sql
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                sql.Identifier(old_name),
                sql.Identifier(new_name),
            )
        )
    conn.commit()


def create_table_from_schema(
    conn: psycopg.Connection,
    table_name: str,
    column_names: list[str],
) -> None:
    """Create a table with id SERIAL PRIMARY KEY and one TEXT column per name. Column names must be sanitized."""
    sql = psycopg.sql
    col_defs = sql.SQL(", ").join(
        [sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in column_names]
    )
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE TABLE {} (id SERIAL PRIMARY KEY, {})").format(
                sql.Identifier(table_name),
                col_defs,
            )
        )
    conn.commit()


def insert_rows_into_table(
    conn: psycopg.Connection,
    table_name: str,
    column_names: list[str],
    rows: list[dict[str, Any]],
) -> int:
    """Insert rows into an existing table. Keys in rows must match column_names (after schema mapping)."""
    if not rows or not column_names:
        return 0
    sql = psycopg.sql
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(c) for c in column_names),
        sql.SQL(", ").join(sql.Placeholder() * len(column_names)),
    )
    with conn.cursor() as cur:
        for row in rows:
            values = [row.get(c, "") for c in column_names]
            cur.execute(query, values)
    conn.commit()
    return len(rows)


def ensure_import_table_and_insert(
    conn: psycopg.Connection,
    table_name: str,
    column_names: list[str],
    rows_iter: Any,
    batch_size: int = 500,
) -> tuple[int, int]:
    """Ensure a table exists with the given schema, then insert all rows. Handles duplicate name: same schema -> truncate+insert; different schema -> rename old, create new, insert. column_names are source header names; records from rows_iter are dicts keyed by these names. Returns (total_rows, rows_inserted)."""
    sanitized_columns = sanitize_column_names(column_names)
    total_rows = 0
    rows_inserted = 0
    batch: list[dict[str, Any]] = []

    def flush_batch() -> None:
        nonlocal rows_inserted
        if batch:
            rows_inserted += insert_rows_into_table(
                conn, table_name, sanitized_columns, batch
            )
            batch.clear()

    if table_exists(conn, table_name):
        existing = get_table_columns(conn, table_name)
        if existing == sanitized_columns:
            with conn.cursor() as cur:
                cur.execute(
                    psycopg.sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY").format(
                        psycopg.sql.Identifier(table_name)
                    )
                )
            conn.commit()
        else:
            backup_name = (
                f"{table_name}_old_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            )
            if len(backup_name) > MAX_IDENTIFIER_LEN:
                backup_name = backup_name[:MAX_IDENTIFIER_LEN]
            rename_table(conn, table_name, backup_name)
            create_table_from_schema(conn, table_name, sanitized_columns)
    else:
        create_table_from_schema(conn, table_name, sanitized_columns)

    for record in rows_iter:
        total_rows += 1
        # Map record keys (source column names) to sanitized column names for DB.
        row_for_db = {
            sanitized_columns[i]: record.get(column_names[i], "")
            for i in range(len(column_names))
        }
        batch.append(row_for_db)
        if len(batch) >= batch_size:
            flush_batch()

    flush_batch()
    return (total_rows, rows_inserted)


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
    host = os.getenv("RAG_POSTGRES_HOST")
    port = os.getenv("RAG_POSTGRES_PORT", "5432")
    dbname = os.getenv("RAG_POSTGRES_DB")
    user = os.getenv("RAG_POSTGRES_USER")
    password = os.getenv("RAG_POSTGRES_PASSWORD")

    if not all([host, dbname, user, password]):
        logger.warning(
            "Database connection skipped: missing RAG_POSTGRES_HOST, RAG_POSTGRES_DB, "
            "RAG_POSTGRES_USER, or RAG_POSTGRES_PASSWORD"
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
