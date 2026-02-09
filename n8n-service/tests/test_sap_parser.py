"""Tests for sap_parser module."""

import sap_parser


def test_sap_parser_module_imports():
    """Ensure sap_parser module has expected exports."""
    assert hasattr(sap_parser, "write_sap_sheet_to_database")
    assert hasattr(sap_parser, "write_sap_sheet_to_file")
    assert hasattr(sap_parser, "iter_xlsx_rows")
