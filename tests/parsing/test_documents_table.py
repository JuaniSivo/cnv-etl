"""
Tests for cnv_etl.parsing.documents_table.

Uses unittest.mock to simulate Selenium WebElements so no browser
is required.
"""

from unittest.mock import MagicMock
from cnv_etl.parsing.documents_table import DocumentsTableParser


# ---------------------------------------------------------------------------
# Helpers to build fake WebElements
# ---------------------------------------------------------------------------

def _make_cell(text: str) -> MagicMock:
    """A fake <td> whose .text returns the given string."""
    cell = MagicMock()
    cell.text = text
    return cell


def _make_link_cell(href: str) -> MagicMock:
    """A fake <td> containing an <a> whose href is the given URL."""
    anchor = MagicMock()
    anchor.get_attribute.return_value = href

    cell = MagicMock()
    cell.find_element.return_value = anchor
    return cell


def _make_header(names: list[str]) -> list[MagicMock]:
    """Build a fake header row from a list of column names."""
    headers = []
    for name in names:
        h = MagicMock()
        h.text = name
        headers.append(h)
    return headers


def _make_row(date: str, hour: str, desc: str, doc_id: str, href: str) -> MagicMock:
    """
    Build a fake table row with the five columns DocumentsTableParser expects:
    FECHA, HORA, DESCRIPCIÓN, DOCUMENTO, VER
    """
    cells = [
        _make_cell(date),
        _make_cell(hour),
        _make_cell(desc),
        _make_cell(doc_id),
        _make_link_cell(href),
    ]
    row = MagicMock()
    row.find_elements.return_value = cells
    return row


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

HEADERS = ["FECHA", "HORA", "DESCRIPCIÓN", "DOCUMENTO", "VER"]


class TestDocumentsTableParser:

    def _parse(self, rows, exclude=None):
        header = _make_header(HEADERS)
        # First row is treated as the header row by the parser (rows[1:])
        # so we prepend a dummy header row
        all_rows = [MagicMock()] + rows
        return DocumentsTableParser().parse(header, all_rows, exclude or [])

    def test_single_row_parsed_correctly(self):
        row = _make_row(
            date="13/03/2025",
            hour="14:20",
            desc="PERIODICIDAD: ANUAL - TIPO BALANCE: CONSOLIDADO - FECHA CIERRE: 2024-12-31",
            doc_id="100001",
            href="https://cnv.gob.ar/doc/100001",
        )
        docs = self._parse([row])

        assert len(docs) == 1
        assert docs[0].document_id == "100001"
        assert docs[0].submission_date == "13/03/2025 14:20"
        assert docs[0].document_link == "https://cnv.gob.ar/doc/100001"

    def test_multiple_rows_all_returned(self):
        rows = [
            _make_row("01/01/2025", "10:00", "DESC A", "100001", "https://cnv.gob.ar/1"),
            _make_row("02/01/2025", "11:00", "DESC B", "100002", "https://cnv.gob.ar/2"),
            _make_row("03/01/2025", "12:00", "DESC C", "100003", "https://cnv.gob.ar/3"),
        ]
        docs = self._parse(rows)
        assert len(docs) == 3

    def test_exclude_keyword_filters_row(self):
        rows = [
            _make_row("01/01/2025", "10:00", "BALANCE ANUAL CONSOLIDADO", "100001", "https://a"),
            _make_row("02/01/2025", "11:00", "NOTA ACLARATORIA ANUAL", "100002", "https://b"),
        ]
        docs = self._parse(rows, exclude=["NOTA ACLARATORIA"])
        assert len(docs) == 1
        assert docs[0].document_id == "100001"

    def test_exclude_is_case_insensitive(self):
        rows = [
            _make_row("01/01/2025", "10:00", "NOTA ACLARATORIA ANUAL", "100001", "https://a"),
        ]
        docs = self._parse(rows, exclude=["nota aclaratoria"])
        assert len(docs) == 0

    def test_empty_rows_returns_empty_list(self):
        docs = self._parse([])
        assert docs == []

    def test_multiple_exclude_keywords(self):
        rows = [
            _make_row("01/01/2025", "10:00", "BALANCE ANUAL", "100001", "https://a"),
            _make_row("02/01/2025", "11:00", "NOTA ACLARATORIA", "100002", "https://b"),
            _make_row("03/01/2025", "12:00", "INFORME AUDITORIA", "100003", "https://c"),
        ]
        docs = self._parse(rows, exclude=["NOTA ACLARATORIA", "INFORME AUDITORIA"])
        assert len(docs) == 1
        assert docs[0].document_id == "100001"

    def test_document_description_stored_correctly(self):
        desc = "PERIODICIDAD: ANUAL - TIPO BALANCE: CONSOLIDADO"
        row  = _make_row("01/01/2025", "10:00", desc, "100001", "https://a")
        docs = self._parse([row])
        assert docs[0].document_description == desc