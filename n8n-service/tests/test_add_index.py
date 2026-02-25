"""Tests for add-index API and db index helpers."""

import pytest
import psycopg.sql

import main
import db


# ---- API tests (mocked) ----

def test_add_index_missing_auth_returns_401():
    client = main.app.test_client()
    resp = client.post(
        "/add-index",
        json={"table_name": "document_rows", "fields": [["dataset_id"]]},
    )
    assert resp.status_code == 401


def test_add_index_missing_table_name_returns_400():
    client = main.app.test_client()
    resp = client.post(
        "/add-index",
        headers={"Authorization": "Bearer token"},
        json={"fields": [["dataset_id"]]},
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "table_name" in data.get("error", "").lower()


def test_add_index_empty_fields_returns_400():
    client = main.app.test_client()
    resp = client.post(
        "/add-index",
        headers={"Authorization": "Bearer token"},
        json={"table_name": "document_rows", "fields": []},
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "fields" in data.get("error", "").lower()


def test_add_index_fields_not_a_list_returns_400():
    client = main.app.test_client()
    resp = client.post(
        "/add-index",
        headers={"Authorization": "Bearer token"},
        json={"table_name": "document_rows", "fields": "dataset_id"},
    )
    assert resp.status_code == 400


def test_add_index_fields_not_list_of_lists_returns_400():
    """Each element of fields must be a list (column group); mixed or non-list element returns 400."""
    client = main.app.test_client()
    resp = client.post(
        "/add-index",
        headers={"Authorization": "Bearer token"},
        json={"table_name": "document_rows", "fields": [["dataset_id"], "order"]},
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "list of lists" in data.get("error", "").lower() or "list" in data.get("error", "").lower()


def test_add_index_db_unavailable_returns_503(monkeypatch):
    monkeypatch.setattr(main.db_module, "get_db_connection", lambda: None)
    client = main.app.test_client()
    resp = client.post(
        "/add-index",
        headers={"Authorization": "Bearer token"},
        json={"table_name": "document_rows", "fields": [["dataset_id"]]},
    )
    assert resp.status_code == 503
    data = resp.get_json()
    assert "database" in data.get("error", "").lower() or "unavailable" in data.get("error", "").lower()


def test_add_index_success_created_true(monkeypatch):
    mock_conn = type("MockConn", (), {"close": lambda self: None})()

    def _fake_get_db_connection():
        return mock_conn

    def _fake_add_index_if_not_exists(conn, table_name, column_names):
        return {"index": "idx_document_rows_dataset_id", "created": True}

    monkeypatch.setattr(main.db_module, "get_db_connection", _fake_get_db_connection)
    monkeypatch.setattr(
        main.db_module, "add_index_if_not_exists", _fake_add_index_if_not_exists
    )

    client = main.app.test_client()
    resp = client.post(
        "/add-index",
        headers={"Authorization": "Bearer token"},
        json={"table_name": "document_rows", "fields": [["dataset_id"]]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert "indexes" in data
    assert len(data["indexes"]) == 1
    assert data["indexes"][0]["index"] == "idx_document_rows_dataset_id"
    assert data["indexes"][0]["created"] is True


def test_add_index_success_created_false(monkeypatch):
    mock_conn = type("MockConn", (), {"close": lambda self: None})()

    def _fake_get_db_connection():
        return mock_conn

    def _fake_add_index_if_not_exists(conn, table_name, column_names):
        return {"index": "idx_imp_foo_col_a", "created": False}

    monkeypatch.setattr(main.db_module, "get_db_connection", _fake_get_db_connection)
    monkeypatch.setattr(
        main.db_module, "add_index_if_not_exists", _fake_add_index_if_not_exists
    )

    client = main.app.test_client()
    resp = client.post(
        "/add-index",
        headers={"Authorization": "Bearer token"},
        json={"table_name": "imp_foo", "fields": [["col_a"]]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert "indexes" in data
    assert len(data["indexes"]) == 1
    assert data["indexes"][0]["index"] == "idx_imp_foo_col_a"
    assert data["indexes"][0]["created"] is False


def test_add_index_success_multiple_groups(monkeypatch):
    """Multiple index groups return multiple results in indexes array."""
    mock_conn = type("MockConn", (), {"close": lambda self: None})()

    def _fake_get_db_connection():
        return mock_conn

    def _fake_add_index_if_not_exists(conn, table_name, column_names):
        if column_names == ["order"]:
            return {"index": "idx_t_order", "created": True}
        if column_names == ["buyer", "time"]:
            return {"index": "idx_t_buyer_time", "created": True}
        raise ValueError("unexpected columns")

    monkeypatch.setattr(main.db_module, "get_db_connection", _fake_get_db_connection)
    monkeypatch.setattr(
        main.db_module, "add_index_if_not_exists", _fake_add_index_if_not_exists
    )

    client = main.app.test_client()
    resp = client.post(
        "/add-index",
        headers={"Authorization": "Bearer token"},
        json={"table_name": "t", "fields": [["order"], ["buyer", "time"]]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["indexes"] == [
        {"index": "idx_t_order", "created": True},
        {"index": "idx_t_buyer_time", "created": True},
    ]


def test_add_index_value_error_returns_400(monkeypatch):
    mock_conn = type("MockConn", (), {"close": lambda self: None})()

    def _fake_get_db_connection():
        return mock_conn

    def _fake_add_index_if_not_exists(conn, table_name, column_names):
        raise ValueError("Table 'nosuch' does not exist")

    monkeypatch.setattr(main.db_module, "get_db_connection", _fake_get_db_connection)
    monkeypatch.setattr(
        main.db_module, "add_index_if_not_exists", _fake_add_index_if_not_exists
    )

    client = main.app.test_client()
    resp = client.post(
        "/add-index",
        headers={"Authorization": "Bearer token"},
        json={"table_name": "nosuch", "fields": [["x"]]},
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data
    assert "does not exist" in data["error"]


# ---- DB layer unit tests (mocked) ----

def test_index_exists_true():
    from unittest.mock import MagicMock
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = (1,)
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = None
    assert db.index_exists(conn, "imp_foo", "idx_imp_foo_col_a") is True


def test_index_exists_false():
    from unittest.mock import MagicMock
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = None
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = None
    assert db.index_exists(conn, "imp_foo", "idx_imp_foo_col_a") is False


def test_add_index_if_not_exists_empty_fields_raises():
    from unittest.mock import MagicMock
    conn = MagicMock()
    with pytest.raises(ValueError, match="non-empty"):
        db.add_index_if_not_exists(conn, "t", [])


def test_add_index_if_not_exists_table_not_exists_raises(monkeypatch):
    from unittest.mock import MagicMock
    conn = MagicMock()
    monkeypatch.setattr(db, "table_exists", lambda c, t: False)
    with pytest.raises(ValueError, match="does not exist"):
        db.add_index_if_not_exists(conn, "nosuch", ["col_a"])


def test_add_index_if_not_exists_column_not_found_raises(monkeypatch):
    from unittest.mock import MagicMock
    conn = MagicMock()
    monkeypatch.setattr(db, "table_exists", lambda c, t: True)
    monkeypatch.setattr(db, "get_table_columns", lambda c, t: ["col_a", "col_b"])
    with pytest.raises(ValueError, match="Column 'col_c' not found"):
        db.add_index_if_not_exists(conn, "imp_foo", ["col_a", "col_c"])


# ---- Integration tests (real DB when available) ----

def test_add_index_integration_create_then_skip():
    """When index does not exist, created is True; when called again, created is False."""
    try:
        conn = db.get_db_connection()
    except Exception:
        pytest.skip("PostgreSQL not configured or not reachable")
    if conn is None:
        pytest.skip("PostgreSQL not configured (missing env)")

    table_name = "imp_add_index_test"
    try:
        if db.table_exists(conn, table_name):
            with conn.cursor() as cur:
                cur.execute(
                    psycopg.sql.SQL("DROP TABLE {}").format(
                        psycopg.sql.Identifier(table_name)
                    )
                )
            conn.commit()
        db.create_table_from_schema(conn, table_name, ["col_a", "col_b"])

        result1 = db.add_index_if_not_exists(conn, table_name, ["col_a"])
        assert result1["created"] is True
        assert result1["index"]
        assert db.index_exists(conn, table_name, result1["index"])

        result2 = db.add_index_if_not_exists(conn, table_name, ["col_a"])
        assert result2["created"] is False
        assert result2["index"] == result1["index"]
    finally:
        if db.table_exists(conn, table_name):
            with conn.cursor() as cur:
                cur.execute(
                    psycopg.sql.SQL("DROP TABLE {}").format(
                        psycopg.sql.Identifier(table_name)
                    )
                )
            conn.commit()
        conn.close()
