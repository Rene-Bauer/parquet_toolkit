"""
SubfolderPanel — modal QDialog for the adaptive subfolder scanner.

Shows scan progress, colour-coded subfolder rows, quick-select buttons,
and a Run Selected button.  Communicates results back to MainWindow via
the run_requested signal.
"""
from __future__ import annotations

from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QColor
from PyQt6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QProgressBar,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from gui.subfolder_scan_worker import SubfolderScanResult, SubfolderScanWorker

# Colours for status indicators (text colour in the list)
_COLOR_RED    = QColor(210, 60, 60)
_COLOR_GREEN  = QColor(50, 170, 50)
_COLOR_YELLOW = QColor(190, 130, 0)   # dark amber — readable on both light/dark
_COLOR_GREY   = QColor(140, 140, 140)

_STATUS_LABEL = {
    "red":    "needs work",
    "green":  "done",
    "yellow": "mixed",
    "grey":   "no files",
}
_STATUS_COLOR = {
    "red":    _COLOR_RED,
    "green":  _COLOR_GREEN,
    "yellow": _COLOR_YELLOW,
    "grey":   _COLOR_GREY,
}


class SubfolderPanel(QDialog):
    """Modal dialog for the adaptive subfolder scanner.

    Signals:
        run_requested(list[SubfolderScanResult]): emitted when the user clicks
            "Run Selected", carrying only the checked results.
    """

    run_requested = pyqtSignal(list)

    def __init__(self, worker: SubfolderScanWorker, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Scan Subfolders")
        self.setMinimumSize(600, 420)
        self.setModal(True)

        self._worker = worker
        self._results: dict[str, SubfolderScanResult] = {}   # name → result
        self._scan_done = False

        self._build_ui()
        self._connect_worker()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        # Progress row
        progress_row = QHBoxLayout()
        self._status_label = QLabel("Scanning…")
        self._progress = QProgressBar()
        self._progress.setMinimum(0)
        self._progress.setValue(0)
        progress_row.addWidget(self._status_label)
        progress_row.addWidget(self._progress, stretch=1)
        layout.addLayout(progress_row)

        # Quick-select buttons
        sel_row = QHBoxLayout()
        self._btn_select_untransformed = QPushButton("Select Untransformed")
        self._btn_select_untransformed.setToolTip("Check all red and yellow subfolders")
        self._btn_select_untransformed.clicked.connect(self._select_untransformed)

        self._btn_select_all = QPushButton("Select All")
        self._btn_select_all.clicked.connect(self._select_all)

        self._btn_clear = QPushButton("Deselect All")
        self._btn_clear.clicked.connect(self._deselect_all)

        sel_row.addWidget(self._btn_select_untransformed)
        sel_row.addWidget(self._btn_select_all)
        sel_row.addWidget(self._btn_clear)
        sel_row.addStretch()
        layout.addLayout(sel_row)

        # Subfolder list
        self._list = QListWidget()
        self._list.setSelectionMode(QListWidget.SelectionMode.NoSelection)
        self._list.itemChanged.connect(self._on_item_changed)
        layout.addWidget(self._list, stretch=1)

        # Summary label
        self._summary_label = QLabel("")
        layout.addWidget(self._summary_label)

        # Bottom buttons
        btn_row = QHBoxLayout()
        self._cancel_scan_btn = QPushButton("Cancel Scan")
        self._cancel_scan_btn.clicked.connect(self._on_cancel_scan)

        self._run_btn = QPushButton("▶  Run Selected Subfolders")
        self._run_btn.setEnabled(False)
        self._run_btn.clicked.connect(self._on_run)

        btn_row.addWidget(self._cancel_scan_btn)
        btn_row.addStretch()
        btn_row.addWidget(self._run_btn)
        layout.addLayout(btn_row)

    # ------------------------------------------------------------------
    # Worker wiring
    # ------------------------------------------------------------------

    def _connect_worker(self) -> None:
        self._worker.subfolder_scanned.connect(self._on_subfolder_scanned)
        self._worker.progress.connect(self._on_progress)
        self._worker.scan_complete.connect(self._on_scan_complete)
        self._worker.log_message.connect(lambda msg: None)  # swallow; main window logs
        self._worker.error.connect(self._on_error)

    def closeEvent(self, event) -> None:  # noqa: N802
        """Disconnect worker signals before closing to prevent slots firing on a dead dialog."""
        try:
            self._worker.subfolder_scanned.disconnect(self._on_subfolder_scanned)
            self._worker.progress.disconnect(self._on_progress)
            self._worker.scan_complete.disconnect(self._on_scan_complete)
            self._worker.error.disconnect(self._on_error)
        except RuntimeError:
            pass  # already disconnected
        super().closeEvent(event)

    # ------------------------------------------------------------------
    # Slots
    # ------------------------------------------------------------------

    def _on_progress(self, scanned: int, total: int) -> None:
        self._progress.setMaximum(total)
        self._progress.setValue(scanned)
        self._status_label.setText(f"Scanning… {scanned}/{total}")

    def _on_subfolder_scanned(self, result: SubfolderScanResult) -> None:
        self._results[result.name] = result
        self._upsert_list_item(result)
        self._update_summary()

    def _on_scan_complete(self) -> None:
        self._scan_done = True
        self._status_label.setText(f"Scan complete — {len(self._results)} subfolder(s)")
        self._progress.setVisible(False)
        self._cancel_scan_btn.setText("Close")
        self._run_btn.setEnabled(True)
        self._sort_list()
        self._update_summary()
        # Auto-select untransformed subfolders as a convenience
        self._select_untransformed()

    def _on_error(self, msg: str) -> None:
        self._status_label.setText(f"Scan error: {msg}")
        self._progress.setVisible(False)
        self._cancel_scan_btn.setText("Close")

    def _on_cancel_scan(self) -> None:
        if not self._scan_done:
            self._worker.cancel()
        self.reject()

    def _on_run(self) -> None:
        selected = self._checked_results()
        if not selected:
            return
        self.run_requested.emit(selected)
        self.accept()

    def _on_item_changed(self, item: QListWidgetItem) -> None:
        self._update_summary()

    # ------------------------------------------------------------------
    # List management
    # ------------------------------------------------------------------

    def _upsert_list_item(self, result: SubfolderScanResult) -> None:
        """Add a new item or update an existing one for *result*."""
        # Check if item already exists (yellow comes in twice: Stage 1 placeholder not used,
        # Stage 2 final result is the only emission — so each name appears exactly once).
        for i in range(self._list.count()):
            if self._list.item(i).data(Qt.ItemDataRole.UserRole) == result.name:
                self._list.item(i).setText(self._item_text(result))
                self._list.item(i).setForeground(_STATUS_COLOR[result.status])
                return

        item = QListWidgetItem(self._item_text(result))
        item.setData(Qt.ItemDataRole.UserRole, result.name)
        item.setForeground(_STATUS_COLOR[result.status])
        item.setFlags(
            Qt.ItemFlag.ItemIsEnabled
            | Qt.ItemFlag.ItemIsUserCheckable
        )
        # Default: check red and yellow, uncheck green and grey
        default_checked = result.status in ("red", "yellow")
        item.setCheckState(
            Qt.CheckState.Checked if default_checked else Qt.CheckState.Unchecked
        )
        self._list.addItem(item)

    def _item_text(self, result: SubfolderScanResult) -> str:
        label = _STATUS_LABEL[result.status]
        if result.status == "green":
            return f"{result.name}  —  {label}  ({result.total} files)"
        if result.status == "grey":
            return f"{result.name}  —  {label}"
        if result.status == "red":
            return f"{result.name}  —  {result.pending} to process  /  {result.total} total"
        # yellow
        if result.pending >= 0:
            return (
                f"{result.name}  —  mixed: {result.pending} to process, "
                f"{result.done} done  /  {result.total} total"
            )
        return f"{result.name}  —  mixed (deep scanning…)"

    def _sort_list(self) -> None:
        """Sort list items alphabetically by subfolder name."""
        items: list[tuple[str, QListWidgetItem]] = []
        while self._list.count():
            item = self._list.takeItem(0)
            name = item.data(Qt.ItemDataRole.UserRole)
            items.append((name, item))
        for _, item in sorted(items, key=lambda t: t[0]):
            self._list.addItem(item)

    # ------------------------------------------------------------------
    # Quick-select helpers
    # ------------------------------------------------------------------

    def _select_untransformed(self) -> None:
        for i in range(self._list.count()):
            item = self._list.item(i)
            name = item.data(Qt.ItemDataRole.UserRole)
            result = self._results.get(name)
            state = (
                Qt.CheckState.Checked
                if result and result.status in ("red", "yellow")
                else Qt.CheckState.Unchecked
            )
            item.setCheckState(state)

    def _select_all(self) -> None:
        for i in range(self._list.count()):
            self._list.item(i).setCheckState(Qt.CheckState.Checked)

    def _deselect_all(self) -> None:
        for i in range(self._list.count()):
            self._list.item(i).setCheckState(Qt.CheckState.Unchecked)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def _checked_results(self) -> list[SubfolderScanResult]:
        out: list[SubfolderScanResult] = []
        for i in range(self._list.count()):
            item = self._list.item(i)
            if item.checkState() == Qt.CheckState.Checked:
                name = item.data(Qt.ItemDataRole.UserRole)
                if name in self._results:
                    out.append(self._results[name])
        return out

    def _update_summary(self) -> None:
        checked = self._checked_results()
        # pending is always a real count for stored results (red: all files, yellow: filtered, green/grey: 0)
        # pending == -1 would only appear for mid-scan yellow placeholders, which are never stored in _results.
        total_files = sum(r.pending for r in checked if r.pending >= 0)
        n = len(checked)
        if n == 0:
            self._summary_label.setText("No subfolders selected.")
        else:
            self._summary_label.setText(
                f"{n} subfolder(s) selected — ~{total_files:,} file(s) to process"
            )
