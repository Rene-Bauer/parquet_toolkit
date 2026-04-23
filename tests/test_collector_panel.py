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
