"""Tests for db module (import table helpers)."""

import pytest
from unittest.mock import MagicMock

import db


def test_sanitize_table_name_basic():
    """Document + sheet become lowercase with underscores; no prefix when prefix not provided."""
    assert db.sanitize_table_name("Order Info System", "Sheet1") == "order_info_system_sheet1"


def test_sanitize_table_name_with_prefix():
    """When prefix is provided, table name is prefix_base."""
    assert db.sanitize_table_name("Order Info System", "Sheet1", prefix="imp") == "imp_order_info_system_sheet1"
    assert db.sanitize_table_name("Doc", "Data", prefix="imp_") == "imp_doc_data"


def test_sanitize_table_name_special_chars():
    """Non-alphanumeric are replaced with single underscore."""
    assert db.sanitize_table_name("Order Info System - Order Headers", "Sheet1") == "order_info_system_order_headers_sheet1"


def test_sanitize_table_name_empty_sheet():
    """Empty sheet name still produces valid name."""
    name = db.sanitize_table_name("Doc", "")
    assert "doc" in name
    assert name == "doc"


def test_sanitize_table_name_truncate():
    """Total length is capped at 63."""
    long_doc = "a" * 100
    name = db.sanitize_table_name(long_doc, "x")
    assert len(name) <= 63
    assert name == (long_doc.lower() + "_x")[:63]


def test_sanitize_column_names_basic():
    """Column names are lowercased and de-duplicated."""
    out = db.sanitize_column_names(["Order", "Material", "Order"])
    assert out[0] == "order"
    assert out[1] == "material"
    assert out[2] == "order_1"


def test_sanitize_column_names_empty_and_digits():
    """Empty or digit-leading get col_ prefix; duplicates get _1, _2."""
    out = db.sanitize_column_names(["", "123", "Col"])
    assert out[0] == "col"
    assert out[1].startswith("col_")
    assert out[2] == "col_1"  # duplicate of "col"


def test_table_exists_true(monkeypatch):
    """table_exists returns True when fetchone returns a row."""
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = (1,)
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = None
    assert db.table_exists(conn, "imp_foo") is True
    cur.execute.assert_called_once()


def test_table_exists_false(monkeypatch):
    """table_exists returns False when fetchone returns None."""
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = None
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = None
    assert db.table_exists(conn, "imp_foo") is False


def test_get_table_columns_returns_names_in_order(monkeypatch):
    """get_table_columns returns column names excluding id."""
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = [
        {"column_name": "id"},
        {"column_name": "order"},
        {"column_name": "material"},
    ]
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = None
    assert db.get_table_columns(conn, "imp_foo") == ["order", "material"]


def test_rename_table_calls_alter(monkeypatch):
    """rename_table executes ALTER TABLE ... RENAME TO ..."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = None
    db.rename_table(conn, "old_name", "new_name")
    cur.execute.assert_called_once()
    conn.commit.assert_called_once()


def test_create_table_from_schema_calls_create(monkeypatch):
    """create_table_from_schema executes CREATE TABLE with id and columns."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = None
    db.create_table_from_schema(conn, "imp_test", ["col_a", "col_b"])
    cur.execute.assert_called_once()
    conn.commit.assert_called_once()


def test_insert_rows_into_table_empty_returns_zero():
    """insert_rows_into_table returns 0 when rows or column_names empty."""
    conn = MagicMock()
    assert db.insert_rows_into_table(conn, "t", ["a"], []) == 0
    assert db.insert_rows_into_table(conn, "t", [], [{"a": "1"}]) == 0


def test_ensure_import_table_and_insert_creates_and_inserts(monkeypatch):
    """When table does not exist, create is called and rows inserted."""
    conn = MagicMock()
    create_calls = []
    insert_calls = []

    def fake_table_exists(c, name):
        return False

    def fake_create_table_from_schema(c, table_name, column_names):
        create_calls.append({"table_name": table_name, "column_names": column_names})

    def fake_insert_rows_into_table(c, table_name, column_names, rows):
        insert_calls.append({"rows": list(rows)})  # copy: real code clears batch after
        return len(rows)

    monkeypatch.setattr(db, "table_exists", fake_table_exists)
    monkeypatch.setattr(db, "create_table_from_schema", fake_create_table_from_schema)
    monkeypatch.setattr(db, "insert_rows_into_table", fake_insert_rows_into_table)

    records = [
        {"Order": "4010523731", "Material": "116G0-036415"},
        {"Order": "4010523912", "Material": "116G0-032048"},
    ]
    total, inserted = db.ensure_import_table_and_insert(
        conn, "imp_new_table", ["Order", "Material"], iter(records), batch_size=10
    )
    assert total == 2
    assert inserted == 2
    assert len(create_calls) == 1
    assert create_calls[0]["table_name"] == "imp_new_table"
    assert len(insert_calls) == 1
    assert len(insert_calls[0]["rows"]) == 2
