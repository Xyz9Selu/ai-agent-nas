"""Integration tests for parse-sap-sheet DB writes."""

import os
import tempfile
from unittest.mock import MagicMock

import sap_parser


def test_stream_clean_and_write_jsonl_calls_insert_rows_batch(monkeypatch):
    """Verify insert_rows_batch is called with correct dataset_id and row data."""
    insert_calls = []

    def _fake_insert_rows_batch(conn, dataset_id, rows):
        insert_calls.append({"conn": conn, "dataset_id": dataset_id, "rows": rows})
        return len(rows)

    monkeypatch.setattr(sap_parser.db, "insert_rows_batch", _fake_insert_rows_batch)

    # Rows: header (6+ non-empty cells) + 2 data rows
    rows = [
        ["col1", "col2", "col3", "col4", "col5", "col6"],
        ["v1", "v2", "v3", "v4", "v5", "v6"],
        ["w1", "w2", "w3", "w4", "w5", "w6"],
    ]
    mock_conn = MagicMock()

    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        output_path = f.name

    try:
        total_rows, rows_inserted = sap_parser._stream_clean_and_write(
            rows,
            output_path=output_path,
            dataset_id="test-dataset-123",
            db_conn=mock_conn,
        )
        assert total_rows == 2
        assert rows_inserted == 2
        assert len(insert_calls) == 1
        assert insert_calls[0]["dataset_id"] == "test-dataset-123"
        assert insert_calls[0]["rows"] == [
            {
                "col1": "v1",
                "col2": "v2",
                "col3": "v3",
                "col4": "v4",
                "col5": "v5",
                "col6": "v6",
            },
            {
                "col1": "w1",
                "col2": "w2",
                "col3": "w3",
                "col4": "w4",
                "col5": "w5",
                "col6": "w6",
            },
        ]
    finally:
        os.unlink(output_path)
