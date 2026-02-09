import main


def test_parse_sap_sheet_missing_auth_returns_401():
    client = main.app.test_client()
    resp = client.post("/parse-sap-sheet", json={"file_id": "abc"})
    assert resp.status_code == 401


def test_parse_sap_sheet_missing_file_id_returns_400():
    client = main.app.test_client()
    resp = client.post(
        "/parse-sap-sheet",
        headers={"Authorization": "Bearer token"},
        json={},
    )
    assert resp.status_code == 400


def test_parse_sap_sheet_success(monkeypatch):
    calls = []

    def _fake_write_sap_sheet_to_database(file_id: str, access_token: str, **kwargs):
        calls.append({"file_id": file_id, "access_token": access_token, **kwargs})
        return {
            "file_id": file_id,
            "name": "n",
            "mime_type": "text/csv",
            "total_rows": 1,
        }

    monkeypatch.setattr(
        main.sap_parser, "write_sap_sheet_to_database", _fake_write_sap_sheet_to_database
    )

    client = main.app.test_client()
    resp = client.post(
        "/parse-sap-sheet",
        headers={"Authorization": "Bearer token123"},
        json={"file_id": "file123"},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["file_id"] == "file123"
    assert data["total_rows"] == 1
    assert calls[0]["dataset_id"] is None


def test_parse_sap_sheet_uses_custom_dataset_id(monkeypatch):
    calls = []

    def _fake_write_sap_sheet_to_database(file_id: str, access_token: str, **kwargs):
        calls.append({"file_id": file_id, "access_token": access_token, **kwargs})
        return {
            "file_id": file_id,
            "name": "n",
            "mime_type": "text/csv",
            "total_rows": 1,
        }

    monkeypatch.setattr(
        main.sap_parser, "write_sap_sheet_to_database", _fake_write_sap_sheet_to_database
    )

    client = main.app.test_client()
    resp = client.post(
        "/parse-sap-sheet",
        headers={"Authorization": "Bearer token123"},
        json={"file_id": "file123", "dataset_id": "custom-dataset-456"},
    )
    assert resp.status_code == 200
    assert calls[0]["dataset_id"] == "custom-dataset-456"


def test_parse_sap_sheet_jsonl_success(monkeypatch):
    def _fake_write_sap_sheet_to_file(file_id: str, access_token: str):
        assert file_id == "file456"
        assert access_token == "token789"
        return {
            "file_id": file_id,
            "name": "sheet.jsonl",
            "mime_type": "text/csv",
            "output_file": "/tmp/file456_20250101120000.jsonl",
            "total_rows": 10,
        }

    monkeypatch.setattr(
        main.sap_parser, "write_sap_sheet_to_file", _fake_write_sap_sheet_to_file
    )

    client = main.app.test_client()
    resp = client.post(
        "/parse-sap-sheet-jsonl",
        headers={"Authorization": "Bearer token789"},
        json={"file_id": "file456"},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["file_id"] == "file456"
    assert data["output_file"] == "/tmp/file456_20250101120000.jsonl"
    assert data["total_rows"] == 10


def test_parse_sap_sheet_jsonl_missing_auth_returns_401():
    client = main.app.test_client()
    resp = client.post("/parse-sap-sheet-jsonl", json={"file_id": "abc"})
    assert resp.status_code == 401


def test_parse_sap_sheet_raises_when_db_unavailable(monkeypatch):
    """When DB connection fails, the API returns 500 with error message."""
    def _fake_get_db_connection():
        raise ConnectionError("Connection refused")

    monkeypatch.setattr(
        main.sap_parser.db, "get_db_connection", _fake_get_db_connection
    )
    # Mock Drive/Sheets so we reach the DB connection step
    def _fake_get_metadata(_, file_id):
        return {"mimeType": "application/vnd.google-apps.spreadsheet", "name": "x"}

    def _fake_iter_rows(*args, **kwargs):
        yield ["a", "b", "c", "d", "e", "f"]
        yield ["1", "2", "3", "4", "5", "6"]

    monkeypatch.setattr(main.sap_parser, "get_drive_file_metadata", _fake_get_metadata)
    monkeypatch.setattr(main.sap_parser, "iter_google_sheet_rows", _fake_iter_rows)
    # build_drive_service can return a dummy; get_drive_file_metadata ignores it
    monkeypatch.setattr(
        main.sap_parser, "build_drive_service",
        lambda _: object(),
    )
    monkeypatch.setattr(
        main.sap_parser, "build_sheets_service",
        lambda _: object(),
    )

    client = main.app.test_client()
    resp = client.post(
        "/parse-sap-sheet",
        headers={"Authorization": "Bearer token"},
        json={"file_id": "file123"},
    )
    assert resp.status_code == 500
    data = resp.get_json()
    assert "error" in data
    assert "Connection refused" in data.get("message", "")


def test_parse_sap_sheet_raises_when_db_env_missing(monkeypatch):
    """When DB env vars are missing, the API returns 500."""
    def _fake_get_db_connection():
        return None

    monkeypatch.setattr(
        main.sap_parser.db, "get_db_connection", _fake_get_db_connection
    )
    monkeypatch.setattr(
        main.sap_parser, "get_drive_file_metadata",
        lambda _, __: {"mimeType": "application/vnd.google-apps.spreadsheet", "name": "x"},
    )
    def _fake_iter():
        yield ["a", "b", "c", "d", "e", "f"]
        yield ["1", "2", "3", "4", "5", "6"]

    monkeypatch.setattr(
        main.sap_parser, "iter_google_sheet_rows",
        lambda *a, **k: _fake_iter(),
    )
    monkeypatch.setattr(main.sap_parser, "build_drive_service", lambda _: object())
    monkeypatch.setattr(main.sap_parser, "build_sheets_service", lambda _: object())

    client = main.app.test_client()
    resp = client.post(
        "/parse-sap-sheet",
        headers={"Authorization": "Bearer token"},
        json={"file_id": "file123"},
    )
    assert resp.status_code == 500
    data = resp.get_json()
    assert "error" in data
    assert "missing" in data.get("message", "").lower()


def test_parse_sap_sheet_jsonl_missing_file_id_returns_400():
    client = main.app.test_client()
    resp = client.post(
        "/parse-sap-sheet-jsonl",
        headers={"Authorization": "Bearer token"},
        json={},
    )
    assert resp.status_code == 400

