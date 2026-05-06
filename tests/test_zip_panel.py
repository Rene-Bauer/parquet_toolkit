# tests/test_zip_panel.py
"""Smoke tests for ZipPanel."""
import sys
import pytest
from unittest.mock import patch, MagicMock

from PyQt6.QtWidgets import QApplication
_app = QApplication.instance() or QApplication(sys.argv)


@pytest.fixture()
def panel():
    from gui.zip_panel import ZipPanel
    p = ZipPanel()
    yield p
    p.close()


def test_panel_instantiates(panel):
    assert panel is not None


def test_convert_btn_exists(panel):
    assert hasattr(panel, "_convert_btn")


def test_cancel_btn_exists_and_disabled(panel):
    assert hasattr(panel, "_cancel_btn")
    assert not panel._cancel_btn.isEnabled()


def test_progress_bar_hidden_initially(panel):
    assert hasattr(panel, "_progress")
    assert not panel._progress.isVisible()


def test_log_is_empty_initially(panel):
    assert panel._log_entries == []


def test_resources_panel_exposed(panel):
    from gui.resources_panel import ResourcesPanel
    assert isinstance(panel.resources_panel, ResourcesPanel)


def test_delimiter_combo_has_common_options(panel):
    items = [panel._delimiter_combo.itemText(i)
             for i in range(panel._delimiter_combo.count())]
    assert any("," in it for it in items)
    assert any(";" in it for it in items)


def test_encoding_combo_has_utf8(panel):
    items = [panel._encoding_combo.itemText(i)
             for i in range(panel._encoding_combo.count())]
    assert any("utf-8" in it.lower() for it in items)


def test_convert_blocked_without_connection_string(panel):
    panel._conn_edit.clear()
    panel._source_edit.setText("zips/")
    panel._output_edit.setText("converted/")
    panel._convert_btn.click()
    assert not panel._cancel_btn.isEnabled()  # worker never started


def test_log_append_writes_entry(panel):
    panel._log_info("hello world")
    assert any("hello world" in t for t, _ in panel._log_entries)
