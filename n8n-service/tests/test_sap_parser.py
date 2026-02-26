"""Tests for sap_parser module."""

import pytest

import sap_parser


def test_sap_parser_module_imports():
    """Ensure sap_parser module has expected exports."""
    assert hasattr(sap_parser, "write_sap_sheet_to_database")
    assert hasattr(sap_parser, "write_sap_sheet_to_file")
    assert hasattr(sap_parser, "write_sap_sheet_to_table")
    assert hasattr(sap_parser, "iter_xlsx_rows")
    assert hasattr(sap_parser, "iter_google_sheet_rows")
    assert hasattr(sap_parser, "parse_txt_sap_report")


def _make_sheets_service_mock(meta, values_responses):
    """Build a mock sheets service: get().execute() -> meta, values().get().execute() -> values_responses[i]."""
    values_calls = []

    class ValuesGet:
        def execute(self):
            idx = len(values_calls)
            values_calls.append(1)
            return values_responses[idx] if idx < len(values_responses) else {"values": []}

    class Values:
        def get(self, **kwargs):
            return ValuesGet()

    class SpreadsheetsGet:
        def __init__(self, result):
            self._result = result

        def execute(self):
            return self._result

    class Spreadsheets:
        def get(self, **kwargs):
            return SpreadsheetsGet(meta)

        def values(self):
            return Values()

    class Service:
        def spreadsheets(self):
            return Spreadsheets()

    return Service()


def test_iter_google_sheet_rows_uses_first_sheet_when_no_params():
    """When neither sheet_id nor sheet_name is given, first sheet is used."""
    meta = {
        "sheets": [
            {
                "properties": {
                    "sheetId": 0,
                    "title": "First",
                    "gridProperties": {"rowCount": 2, "columnCount": 2},
                }
            }
        ]
    }
    values_responses = [{"values": [["A", "B"], ["1", "2"]]}]
    svc = _make_sheets_service_mock(meta, values_responses)
    rows = list(
        sap_parser.iter_google_sheet_rows(svc, "sid", batch_size=10)
    )
    assert rows == [["A", "B"], ["1", "2"]]


def test_iter_google_sheet_rows_sheet_id_only():
    """When sheet_id is given, that sheet is used."""
    meta = {
        "sheets": [
            {"properties": {"sheetId": 0, "title": "First", "gridProperties": {"rowCount": 0, "columnCount": 0}}},
            {
                "properties": {
                    "sheetId": 42,
                    "title": "Second",
                    "gridProperties": {"rowCount": 1, "columnCount": 1},
                }
            },
        ]
    }
    values_responses = [{"values": [["X"]]}]
    svc = _make_sheets_service_mock(meta, values_responses)
    rows = list(
        sap_parser.iter_google_sheet_rows(svc, "sid", batch_size=10, sheet_id=42)
    )
    assert rows == [["X"]]


def test_iter_google_sheet_rows_sheet_name_only():
    """When sheet_name is given, that sheet is used."""
    meta = {
        "sheets": [
            {"properties": {"sheetId": 0, "title": "First", "gridProperties": {"rowCount": 0, "columnCount": 0}}},
            {
                "properties": {
                    "sheetId": 1,
                    "title": "Data",
                    "gridProperties": {"rowCount": 1, "columnCount": 2},
                }
            },
        ]
    }
    values_responses = [{"values": [["A", "B"]]}]
    svc = _make_sheets_service_mock(meta, values_responses)
    rows = list(
        sap_parser.iter_google_sheet_rows(svc, "sid", batch_size=10, sheet_name="Data")
    )
    assert rows == [["A", "B"]]


def test_iter_google_sheet_rows_sheet_id_takes_precedence():
    """When both sheet_id and sheet_name are given, sheet_id wins (we read sheet 20, not 'ByName')."""
    meta = {
        "sheets": [
            {
                "properties": {
                    "sheetId": 10,
                    "title": "ByName",
                    "gridProperties": {"rowCount": 1, "columnCount": 1},
                }
            },
            {
                "properties": {
                    "sheetId": 20,
                    "title": "ById",
                    "gridProperties": {"rowCount": 1, "columnCount": 1},
                }
            },
        ]
    }
    # One values() request for the chosen sheet (id=20, title "ById").
    values_responses = [{"values": [["Y"]]}]
    svc = _make_sheets_service_mock(meta, values_responses)
    rows = list(
        sap_parser.iter_google_sheet_rows(
            svc, "sid", batch_size=10, sheet_id=20, sheet_name="ByName"
        )
    )
    assert rows == [["Y"]]


def test_iter_google_sheet_rows_sheet_id_not_found_raises():
    """When sheet_id is given but no sheet has that id, ValueError is raised."""
    meta = {
        "sheets": [
            {"properties": {"sheetId": 0, "title": "Only", "gridProperties": {}}}
        ]
    }
    svc = _make_sheets_service_mock(meta, [])
    with pytest.raises(ValueError, match=r"Sheet not found: sheet_id=999"):
        list(sap_parser.iter_google_sheet_rows(svc, "sid", sheet_id=999))


def test_iter_google_sheet_rows_sheet_name_not_found_raises():
    """When sheet_name is given but no sheet has that title, ValueError is raised."""
    meta = {
        "sheets": [
            {"properties": {"sheetId": 0, "title": "Only", "gridProperties": {}}}
        ]
    }
    svc = _make_sheets_service_mock(meta, [])
    with pytest.raises(ValueError, match=r"Sheet not found: sheet_name='Missing'"):
        list(sap_parser.iter_google_sheet_rows(svc, "sid", sheet_name="Missing"))


def test_parse_txt_sap_report_returns_header_and_rows():
    """TXT SAP report is parsed: header row and pipe-delimited data rows."""
    content = """
|Order     |Material       |Description    |
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
|4010523731|116G0-036415   |SEMI-FG,SM,    |
|4010523912|116G0-032048   |SEMI-FG,METAL  |
"""
    column_names, row_iter = sap_parser.parse_txt_sap_report(content)
    assert column_names == ["Order", "Material", "Description"]
    rows = list(row_iter)
    assert len(rows) == 2
    assert rows[0][0].strip() == "4010523731"
    assert rows[0][1].strip() == "116G0-036415"
    assert rows[1][0].strip() == "4010523912"


def test_parse_txt_sap_report_no_header_returns_empty():
    """When no header line is found, returns empty column names and no rows."""
    content = "just some text\nno pipes here\n"
    column_names, row_iter = sap_parser.parse_txt_sap_report(content)
    assert column_names == []
    assert list(row_iter) == []


def test_write_sap_sheet_to_table_success_returns_table_name_and_schema(monkeypatch):
    """write_sap_sheet_to_table returns table_name, schema, total_rows, rows_inserted."""
    def _fake_metadata(_, file_id):
        return {"mimeType": "application/vnd.google-apps.spreadsheet", "name": "My Report"}

    def _fake_get_title(svc, fid, **kwargs):
        return "Sheet1"

    def _fake_iter_rows(*args, **kwargs):
        yield ["Order", "Material", "Qty", "X", "Y", "Z"]
        yield ["1", "A", "10", "", "", ""]
        yield ["2", "B", "20", "", "", ""]

    def _fake_get_conn():
        return None
    monkeypatch.setattr(sap_parser, "get_drive_file_metadata", _fake_metadata)
    monkeypatch.setattr(sap_parser, "build_drive_service", lambda _: object())
    monkeypatch.setattr(sap_parser, "build_sheets_service", lambda _: object())
    monkeypatch.setattr(sap_parser, "get_google_sheet_chosen_title", _fake_get_title)
    monkeypatch.setattr(sap_parser, "iter_google_sheet_rows", lambda *a, **k: _fake_iter_rows())
    monkeypatch.setattr(sap_parser.db, "get_db_connection", _fake_get_conn)
    with pytest.raises(RuntimeError, match="Database connection failed"):
        sap_parser.write_sap_sheet_to_table("fid", "token")

    # With a mock conn and ensure_import_table_and_insert returning (2, 2)
    def _fake_get_conn_ok():
        from unittest.mock import MagicMock
        return MagicMock()

    def _fake_ensure(conn, table_name, column_names, rows_iter, batch_size=500):
        rows = list(rows_iter)
        return (len(rows), len(rows))

    monkeypatch.setattr(sap_parser.db, "get_db_connection", _fake_get_conn_ok)
    monkeypatch.setattr(sap_parser.db, "ensure_import_table_and_insert", _fake_ensure)

    result = sap_parser.write_sap_sheet_to_table("fid", "token")
    assert "table_name" in result
    assert result["table_name"] == "my_report_sheet1"
    assert "schema" in result
    assert len(result["schema"]) == 6
    assert result["schema"][0]["type"] == "text"
    assert result["total_rows"] == 2
    assert result["rows_inserted"] == 2
    assert result["file_id"] == "fid"
    assert result["name"] == "My Report"

    result_with_prefix = sap_parser.write_sap_sheet_to_table("fid", "token", table_prefix="imp")
    assert result_with_prefix["table_name"] == "imp_my_report_sheet1"
