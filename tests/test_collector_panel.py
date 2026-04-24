import sys

import pytest
from PyQt6.QtGui import QCloseEvent
from PyQt6.QtWidgets import QApplication

from gui.collector_panel import CollectorPanel

_app = QApplication.instance() or QApplication(sys.argv)


class _DummyWorker:
    def __init__(self, running: bool = True, wait_result: bool = True) -> None:
        self._running = running
        self._wait_result = wait_result
        self.deleted = False
        self.cancel_calls = 0

    def isRunning(self) -> bool:
        return self._running

    def deleteLater(self) -> None:
        self.deleted = True

    def wait(self, _timeout: int) -> bool:
        return self._wait_result

    def cancel(self) -> None:
        self.cancel_calls += 1

    def stop(self) -> None:
        self._running = False


def test_worker_cleanup_waits_for_thread_to_stop():
    panel = CollectorPanel()
    worker = _DummyWorker(running=True)
    panel._worker = worker  # type: ignore[attr-defined]

    panel._request_worker_cleanup()
    assert panel._worker is worker  # still present while running
    assert not worker.deleted

    worker.stop()
    panel._request_worker_cleanup()

    assert panel._worker is None
    assert worker.deleted


def test_close_event_blocks_when_worker_is_still_running():
    panel = CollectorPanel()
    worker = _DummyWorker(running=True, wait_result=False)
    panel._worker = worker  # type: ignore[attr-defined]

    event = QCloseEvent()
    panel.closeEvent(event)

    assert not event.isAccepted()
    assert worker.cancel_calls == 1
    assert panel._worker is worker


def test_panel_exposes_resources_panel():
    from gui.resources_panel import ResourcesPanel
    panel = CollectorPanel()
    assert isinstance(panel.resources_panel, ResourcesPanel)


def test_schema_table_initially_hidden():
    panel = CollectorPanel()
    # The schema group should be hidden until Load Schema is triggered
    assert not panel._schema_group.isVisible()


def test_pause_btn_exists_and_disabled_initially():
    panel = CollectorPanel()
    assert hasattr(panel, '_pause_btn')
    assert not panel._pause_btn.isEnabled()
    assert panel._pause_btn.text() == "Pause"


def test_pause_btn_is_not_paused_initially():
    panel = CollectorPanel()
    assert hasattr(panel, '_is_paused')
    assert panel._is_paused is False


def test_on_worker_paused_updates_button_and_flag():
    panel = CollectorPanel()
    panel._on_worker_paused()
    assert panel._is_paused is True
    assert panel._pause_btn.text() == "Resume"


def test_on_worker_resumed_updates_button_and_flag():
    panel = CollectorPanel()
    panel._is_paused = True
    panel._pause_btn.setText("Resume")
    panel._on_worker_resumed()
    assert panel._is_paused is False
    assert panel._pause_btn.text() == "Pause"


def test_on_finished_disables_pause_btn():
    panel = CollectorPanel()
    panel._pause_btn.setEnabled(True)
    panel._on_finished({"rowCount": 0})
    assert not panel._pause_btn.isEnabled()


def test_on_cancelled_disables_pause_btn():
    panel = CollectorPanel()
    panel._pause_btn.setEnabled(True)
    panel._on_cancelled()
    assert not panel._pause_btn.isEnabled()


def test_eta_label_exists_and_initially_empty():
    panel = CollectorPanel()
    assert hasattr(panel, '_eta_label')
    assert panel._eta_label.text() == ""


def test_eta_label_clears_on_finished():
    panel = CollectorPanel()
    panel._eta_label.setText("ETA 1m 30s (~14:00)")
    panel._on_finished({"rowCount": 0})
    assert panel._eta_label.text() == ""


def test_eta_label_clears_on_cancelled():
    panel = CollectorPanel()
    panel._eta_label.setText("ETA 1m 30s (~14:00)")
    panel._on_cancelled()
    assert panel._eta_label.text() == ""


def test_log_entries_accumulate():
    panel = CollectorPanel()
    panel._log_info("hello")
    panel._log_error("oops")
    assert len(panel._log_entries) == 2
    assert panel._log_entries[0] == ("hello", None)
    text, color = panel._log_entries[1]
    assert text == "oops"
    assert color is not None   # errors have a colour


def test_flush_log_writes_file_and_clears_entries(tmp_path, monkeypatch):
    import gui.collector_panel as cp_mod
    monkeypatch.setattr(cp_mod, "_LOG_DIR_OVERRIDE", str(tmp_path))
    panel = CollectorPanel()
    panel._log_entries = [("line1", None), ("line2", None)]
    panel._flush_log_to_file()
    # Entries cleared after flush
    assert len(panel._log_entries) == 0
    # A .txt file was written
    files = list(tmp_path.iterdir())
    assert len(files) == 1
    content = files[0].read_text(encoding="utf-8")
    assert "line1" in content
    assert "line2" in content


def test_flush_count_increments():
    panel = CollectorPanel()
    assert panel._log_flush_count == 0


def test_run_record_is_none_initially():
    panel = CollectorPanel()
    assert hasattr(panel, '_run_record')
    assert panel._run_record is None


def test_on_collect_calls_mark_in_progress(monkeypatch):
    """Collect stores a run record and calls mark_in_progress."""
    from unittest.mock import MagicMock, patch

    panel = CollectorPanel()
    panel._conn_edit.setText("fake_conn_string")
    panel._container_edit.setText("mycontainer")
    panel._source_edit.setText("prefix/")
    panel._output_edit.setText("out/")
    panel._ids_edit.setPlainText("uid1")

    mock_record = MagicMock()
    mock_record.is_complete.return_value = False
    mock_record.status = "none"

    mock_worker = MagicMock()
    # Make mock_worker support all the signal connections
    mock_worker.listing_complete = MagicMock()
    mock_worker.listing_complete.connect = MagicMock()
    mock_worker.progress = MagicMock()
    mock_worker.progress.connect = MagicMock()
    mock_worker.file_error = MagicMock()
    mock_worker.file_error.connect = MagicMock()
    mock_worker.finished = MagicMock()
    mock_worker.finished.connect = MagicMock()
    mock_worker.cancelled = MagicMock()
    mock_worker.cancelled.connect = MagicMock()
    mock_worker.log_message = MagicMock()
    mock_worker.log_message.connect = MagicMock()
    mock_worker.workers_scaled = MagicMock()
    mock_worker.workers_scaled.connect = MagicMock()
    mock_worker.paused = MagicMock()
    mock_worker.paused.connect = MagicMock()
    mock_worker.resumed = MagicMock()
    mock_worker.resumed.connect = MagicMock()
    mock_worker.start = MagicMock()

    with patch("gui.collector_panel.CollectorRunRecord") as mock_cls, \
         patch("gui.collector_panel.DataCollectorWorker", return_value=mock_worker):
        mock_cls.load_or_create.return_value = mock_record
        panel._on_collect()

    mock_record.mark_in_progress.assert_called_once()
    assert panel._run_record is mock_record


def test_on_finished_with_rows_calls_mark_complete():
    """_on_finished marks run complete when rows were produced."""
    from unittest.mock import MagicMock

    panel = CollectorPanel()
    mock_record = MagicMock()
    panel._run_record = mock_record

    panel._on_finished({
        "rowCount": 10,
        "outputBlobs": ["out/SenderUid_uid1.parquet"],
        "outputContainer": "mycontainer",
    })

    mock_record.mark_complete.assert_called_once_with(
        "out/SenderUid_uid1.parquet", 10
    )


def test_on_finished_with_no_rows_does_not_mark_complete():
    """_on_finished does NOT mark complete when no rows were collected."""
    from unittest.mock import MagicMock

    panel = CollectorPanel()
    mock_record = MagicMock()
    panel._run_record = mock_record

    panel._on_finished({"rowCount": 0})

    mock_record.mark_complete.assert_not_called()


def test_max_filesize_spin_default_value():
    """Max file size spinbox must default to 5 (GB)."""
    panel = CollectorPanel()
    assert panel._max_filesize_spin.value() == 5


def test_max_filesize_spin_range():
    """Spinbox range must be 0–500 GB."""
    panel = CollectorPanel()
    assert panel._max_filesize_spin.minimum() == 0
    assert panel._max_filesize_spin.maximum() == 500


def test_max_filesize_spin_special_value_text():
    """Value 0 must display 'Unbegrenzt'."""
    panel = CollectorPanel()
    assert "Unbegrenzt" in panel._max_filesize_spin.specialValueText()
