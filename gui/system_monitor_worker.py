"""
Background QThread that polls system resources every second and emits a
``snapshot_ready`` signal with the latest ``SystemSnapshot``.

Start it once when the application launches and stop it on close — it runs
independently of any active transformation.
"""
from __future__ import annotations

import threading

from PyQt6.QtCore import QThread, pyqtSignal

from parquet_transform.system_monitor import SystemMonitor, SystemSnapshot


class SystemMonitorWorker(QThread):
    """Polls CPU / RAM / network once per second.

    Signals
    -------
    snapshot_ready(SystemSnapshot)
        Emitted on the Qt main thread after each 1-second poll.
    """

    snapshot_ready = pyqtSignal(object)  # SystemSnapshot

    def __init__(self, interval_ms: int = 1000, parent=None) -> None:
        super().__init__(parent)
        self._interval_ms = interval_ms
        self._stop_event = threading.Event()

    def stop(self) -> None:
        """Request the polling loop to exit.  Returns immediately."""
        self._stop_event.set()

    def run(self) -> None:
        monitor = SystemMonitor()
        while not self._stop_event.is_set():
            snap: SystemSnapshot = monitor.snapshot()
            self.snapshot_ready.emit(snap)
            # Use the event as a sleep so stop() wakes us up immediately
            self._stop_event.wait(timeout=self._interval_ms / 1000.0)
