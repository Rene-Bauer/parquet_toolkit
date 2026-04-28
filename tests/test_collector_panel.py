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


def test_flush_log_clears_entries_without_writing_file(tmp_path, monkeypatch):
    """_flush_log_to_file only clears the widget buffer; disk writes happen via the stream."""
    import gui.collector_panel as cp_mod
    monkeypatch.setattr(cp_mod, "_LOG_DIR_OVERRIDE", str(tmp_path))
    panel = CollectorPanel()
    panel._log_entries = [("line1", None), ("line2", None)]
    panel._flush_log_to_file()
    # Entries cleared after flush
    assert len(panel._log_entries) == 0
    # No file written by flush itself (streaming handles disk writes)
    assert list(tmp_path.iterdir()) == []


def test_flush_count_increments():
    panel = CollectorPanel()
    assert panel._log_flush_count == 0


def test_open_log_stream_creates_file(tmp_path, monkeypatch):
    """_open_log_stream opens a writable file and exposes the path."""
    import gui.collector_panel as cp_mod
    monkeypatch.setattr(cp_mod, "_LOG_DIR_OVERRIDE", str(tmp_path))
    panel = CollectorPanel()
    panel._open_log_stream()
    assert panel._log_stream is not None
    assert panel._log_file_path is not None
    assert panel._log_file_path.startswith(str(tmp_path))
    assert panel._log_file_path.endswith(".txt")
    panel._close_log_stream()


def test_append_log_writes_to_stream(tmp_path, monkeypatch):
    """Every log line is written to the stream file immediately."""
    import gui.collector_panel as cp_mod
    monkeypatch.setattr(cp_mod, "_LOG_DIR_OVERRIDE", str(tmp_path))
    panel = CollectorPanel()
    panel._open_log_stream()
    panel._log_info("hello stream")
    panel._log_error("an error")
    panel._close_log_stream()
    content = (tmp_path / panel._log_path_label.text().removeprefix("Streaming to: ")).read_text(encoding="utf-8")
    assert "hello stream" in content
    assert "an error" in content


def test_close_log_stream_is_idempotent():
    """Calling _close_log_stream twice must not raise."""
    panel = CollectorPanel()
    panel._close_log_stream()
    panel._close_log_stream()


def test_open_log_stream_closes_previous_stream(tmp_path, monkeypatch):
    """Opening a second stream closes the first one."""
    import gui.collector_panel as cp_mod
    monkeypatch.setattr(cp_mod, "_LOG_DIR_OVERRIDE", str(tmp_path))
    panel = CollectorPanel()
    panel._open_log_stream()
    first_stream = panel._log_stream
    panel._open_log_stream()
    assert first_stream.closed
    panel._close_log_stream()


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


import json
from unittest.mock import MagicMock, patch


def _fill_panel(panel) -> None:
    """Fill required fields with valid dummy values."""
    panel._conn_edit.setText("fake_conn")
    panel._container_edit.setText("mycontainer")
    panel._source_edit.setText("data/")
    panel._output_edit.setText("output/")
    panel._ids_edit.setPlainText("uid1")
    # SenderUid is the first item in the combo — already selected by default


def _fake_cp_json(done: list, in_progress) -> str:
    return json.dumps({
        "container": "mycontainer",
        "source_prefix": "data/",
        "filter_col": "SenderUid",
        "filter_values": ["uid1"],
        "output_prefix": "output/",
        "output_container": "mycontainer",
        "subfolders_done": done,
        "in_progress_subfolder": in_progress,
        "next_part": len(done) + 1,
        "total_rows": len(done) * 500,
        "created_at": "2026-01-01T00:00:00",
        "updated_at": "2026-01-01T00:00:00",
    })


def test_start_fresh_deletes_subfolder_checkpoint(tmp_path):
    """Clicking Start Fresh in the subfolder dialog removes the checkpoint file."""
    from parquet_transform.checkpoint import SubfolderCheckpoint

    panel = CollectorPanel()
    _fill_panel(panel)

    fake_cp_path = tmp_path / "fake__subfolder.json"
    fake_cp_path.write_text(
        _fake_cp_json(done=["26-01-2026", "26-02-2026"], in_progress="26-03-2026"),
        encoding="utf-8",
    )

    mock_rr = MagicMock()
    mock_rr.is_complete.return_value = False
    mock_rr.status = "none"

    with patch.object(SubfolderCheckpoint, "checkpoint_path", return_value=fake_cp_path), \
         patch("gui.collector_panel.CollectorRunRecord.load_or_create", return_value=mock_rr), \
         patch("gui.workers.DataCollectorWorker.start"), \
         patch("PyQt6.QtWidgets.QMessageBox") as MockMB:

        fresh_btn = MagicMock()

        def add_button(label, role):
            if label == "Start Fresh":
                return fresh_btn
            return MagicMock()

        mb_instance = MagicMock()
        MockMB.return_value = mb_instance
        mb_instance.addButton.side_effect = add_button
        mb_instance.clickedButton.return_value = fresh_btn

        panel._on_collect()

    assert not fake_cp_path.exists()


def test_resume_keeps_subfolder_checkpoint(tmp_path):
    """Clicking Resume in the subfolder dialog leaves the checkpoint file untouched."""
    from parquet_transform.checkpoint import SubfolderCheckpoint

    panel = CollectorPanel()
    _fill_panel(panel)

    fake_cp_path = tmp_path / "fake__subfolder.json"
    fake_cp_path.write_text(
        _fake_cp_json(done=["26-01-2026"], in_progress="26-02-2026"),
        encoding="utf-8",
    )

    mock_rr = MagicMock()
    mock_rr.is_complete.return_value = False
    mock_rr.status = "none"

    with patch.object(SubfolderCheckpoint, "checkpoint_path", return_value=fake_cp_path), \
         patch("gui.collector_panel.CollectorRunRecord.load_or_create", return_value=mock_rr) as mock_rr_ctor, \
         patch("gui.workers.DataCollectorWorker.start"), \
         patch("PyQt6.QtWidgets.QMessageBox") as MockMB:

        resume_btn = MagicMock()

        def add_button(label, role):
            if label == "Resume":
                return resume_btn
            return MagicMock()

        mb_instance = MagicMock()
        MockMB.return_value = mb_instance
        mb_instance.addButton.side_effect = add_button
        mb_instance.clickedButton.return_value = resume_btn

        panel._on_collect()

    assert fake_cp_path.exists()
    mock_rr_ctor.assert_called_once()  # collection proceeded past the dialog


def test_cancel_subfolder_dialog_aborts_collection(tmp_path):
    """Clicking Cancel in the subfolder dialog does not start a worker."""
    from parquet_transform.checkpoint import SubfolderCheckpoint

    panel = CollectorPanel()
    _fill_panel(panel)

    fake_cp_path = tmp_path / "fake__subfolder.json"
    fake_cp_path.write_text(
        _fake_cp_json(done=["26-01-2026"], in_progress="26-02-2026"),
        encoding="utf-8",
    )

    with patch.object(SubfolderCheckpoint, "checkpoint_path", return_value=fake_cp_path), \
         patch("gui.collector_panel.CollectorRunRecord.load_or_create") as mock_rr_ctor, \
         patch("PyQt6.QtWidgets.QMessageBox") as MockMB:

        cancel_btn = MagicMock()

        mb_instance = MagicMock()
        MockMB.return_value = mb_instance
        mb_instance.addButton.return_value = MagicMock()
        mb_instance.clickedButton.return_value = cancel_btn  # not resume or fresh

        panel._on_collect()

    # CollectorRunRecord was never loaded (cancelled before reaching that block)
    mock_rr_ctor.assert_not_called()
    assert panel._worker is None


def test_no_subfolder_checkpoint_uses_run_record_flow(tmp_path):
    """When no SubfolderCheckpoint exists, the normal CollectorRunRecord flow runs."""
    from parquet_transform.checkpoint import SubfolderCheckpoint

    panel = CollectorPanel()
    _fill_panel(panel)

    non_existent = tmp_path / "does_not_exist.json"

    mock_rr = MagicMock()
    mock_rr.is_complete.return_value = False
    mock_rr.status = "none"

    with patch.object(SubfolderCheckpoint, "checkpoint_path", return_value=non_existent), \
         patch("gui.collector_panel.CollectorRunRecord.load_or_create", return_value=mock_rr) as mock_rr_ctor, \
         patch("gui.workers.DataCollectorWorker.start"):

        panel._on_collect()

    mock_rr_ctor.assert_called_once()


def test_corrupt_subfolder_checkpoint_falls_through_to_run_record(tmp_path):
    """A corrupt SubfolderCheckpoint is non-fatal; the run-record flow still runs."""
    from parquet_transform.checkpoint import SubfolderCheckpoint

    panel = CollectorPanel()
    _fill_panel(panel)

    fake_cp_path = tmp_path / "fake__subfolder.json"
    fake_cp_path.write_text("not json", encoding="utf-8")

    mock_rr = MagicMock()
    mock_rr.is_complete.return_value = False
    mock_rr.status = "none"

    with patch.object(SubfolderCheckpoint, "checkpoint_path", return_value=fake_cp_path), \
         patch("gui.collector_panel.CollectorRunRecord.load_or_create", return_value=mock_rr) as mock_rr_ctor, \
         patch("gui.workers.DataCollectorWorker.start"):

        panel._on_collect()

    mock_rr_ctor.assert_called_once()
