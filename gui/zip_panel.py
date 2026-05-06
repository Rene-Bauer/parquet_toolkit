"""Self-contained ZIP → Parquet converter panel widget."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from time import perf_counter

from PyQt6.QtCore import QTimer
from PyQt6.QtGui import QColor, QTextCharFormat, QTextCursor
from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QStyle,
    QSystemTrayIcon,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from gui.resources_panel import ResourcesPanel
from gui.workers import ZipConverterWorker, _format_duration

MAX_LOG_ENTRIES: int = 5_000

_LOG_DIR_OVERRIDE: str | None = None
"""Redirect log files during tests; None = project_root/logs/."""

_COLOR_ERROR = QColor("#cc0000")
_COLOR_WARN  = QColor("#aa8800")


class ZipPanel(QWidget):
    """ZIP → Parquet converter panel — third tab of the main window."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._worker: ZipConverterWorker | None = None
        self._is_paused: bool = False
        self._run_start_time: float = 0.0
        self._log_entries: list[tuple[str, QColor | None]] = []
        self._log_flush_count: int = 0
        self._log_stream = None
        self._log_file_path: str | None = None
        self._tray_icon: QSystemTrayIcon | None = None
        self._worker_cleanup_timer: QTimer | None = None

        root = QVBoxLayout(self)
        root.setSpacing(8)
        root.setContentsMargins(12, 12, 12, 12)

        root.addWidget(self._build_connection_group())
        root.addWidget(self._build_settings_group())
        root.addWidget(self._build_action_row())

        self._resources_panel = ResourcesPanel()
        root.addWidget(self._resources_panel)

        root.addWidget(self._build_log_group())

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def resources_panel(self) -> ResourcesPanel:
        """Exposed so MainWindow can connect SystemMonitorWorker to it."""
        return self._resources_panel

    # ------------------------------------------------------------------
    # Panel builders
    # ------------------------------------------------------------------

    def _build_connection_group(self) -> QGroupBox:
        box = QGroupBox("Azure Connection")
        layout = QHBoxLayout(box)

        layout.addWidget(QLabel("Connection string:"))
        self._conn_edit = QLineEdit()
        self._conn_edit.setPlaceholderText(
            "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=..."
        )
        self._conn_edit.setEchoMode(QLineEdit.EchoMode.Password)
        layout.addWidget(self._conn_edit, stretch=3)

        layout.addWidget(QLabel("Container:"))
        self._container_edit = QLineEdit()
        self._container_edit.setPlaceholderText("my-container")
        layout.addWidget(self._container_edit, stretch=1)

        return box

    def _build_settings_group(self) -> QGroupBox:
        box = QGroupBox("Conversion Settings")
        layout = QVBoxLayout(box)

        # Row 1: source prefix + output prefix
        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Source prefix (ZIP):"))
        self._source_edit = QLineEdit()
        self._source_edit.setPlaceholderText("archive/exports/")
        row1.addWidget(self._source_edit, stretch=1)
        row1.addSpacing(16)
        row1.addWidget(QLabel("Output prefix (Parquet):"))
        self._output_edit = QLineEdit()
        self._output_edit.setPlaceholderText("converted/")
        row1.addWidget(self._output_edit, stretch=1)
        layout.addLayout(row1)

        # Row 2: CSV options
        row2 = QHBoxLayout()
        row2.addWidget(QLabel("CSV delimiter:"))
        self._delimiter_combo = QComboBox()
        self._delimiter_combo.addItems([
            ", (comma)", "; (semicolon)", "\\t (tab)", "| (pipe)",
        ])
        self._delimiter_combo.setFixedWidth(130)
        row2.addWidget(self._delimiter_combo)
        row2.addSpacing(16)
        row2.addWidget(QLabel("CSV encoding:"))
        self._encoding_combo = QComboBox()
        self._encoding_combo.addItems(["utf-8", "latin-1", "cp1252"])
        self._encoding_combo.setFixedWidth(100)
        row2.addWidget(self._encoding_combo)
        row2.addStretch()
        layout.addLayout(row2)

        # Row 3: worker count + autoscale + max attempts
        row3 = QHBoxLayout()
        row3.addWidget(QLabel("Workers:"))
        self._workers_spin = QSpinBox()
        self._workers_spin.setRange(1, 64)
        self._workers_spin.setValue(4)
        self._workers_spin.setFixedWidth(60)
        row3.addWidget(self._workers_spin)
        self._autoscale_check = QCheckBox("Autoscale")
        self._autoscale_check.toggled.connect(
            lambda checked: self._workers_spin.setEnabled(not checked)
        )
        self._autoscale_check.setChecked(True)
        row3.addWidget(self._autoscale_check)
        row3.addSpacing(16)
        row3.addWidget(QLabel("Max attempts:"))
        self._attempts_spin = QSpinBox()
        self._attempts_spin.setRange(1, 20)
        self._attempts_spin.setValue(5)
        self._attempts_spin.setFixedWidth(60)
        row3.addWidget(self._attempts_spin)
        row3.addStretch()
        layout.addLayout(row3)

        return box

    def _build_action_row(self) -> QWidget:
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)

        self._convert_btn = QPushButton("Convert")
        self._cancel_btn = QPushButton("Cancel")
        self._cancel_btn.setEnabled(False)
        self._pause_btn = QPushButton("Pause")
        self._pause_btn.setFixedWidth(80)
        self._pause_btn.setEnabled(False)
        self._eta_label = QLabel("")
        self._eta_label.setStyleSheet("color: gray;")
        self._progress = QProgressBar()
        self._progress.setVisible(False)

        layout.addWidget(self._convert_btn)
        layout.addWidget(self._cancel_btn)
        layout.addWidget(self._pause_btn)
        layout.addWidget(self._eta_label)
        layout.addWidget(self._progress, stretch=1)

        self._convert_btn.clicked.connect(self._on_convert)
        self._cancel_btn.clicked.connect(self._on_cancel)
        self._pause_btn.clicked.connect(self._on_pause_resume)

        return widget

    def _build_log_group(self) -> QGroupBox:
        box = QGroupBox("Log")
        layout = QVBoxLayout(box)

        toolbar = QHBoxLayout()
        btn_export = QPushButton("Export Log…")
        btn_export.setFixedWidth(95)
        btn_export.clicked.connect(self._on_export_log)
        toolbar.addStretch()
        toolbar.addWidget(btn_export)
        layout.addLayout(toolbar)

        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setMinimumHeight(140)
        layout.addWidget(self._log)

        self._log_path_label = QLabel("")
        self._log_path_label.setStyleSheet("color: gray; font-style: italic;")
        self._log_path_label.setWordWrap(True)
        layout.addWidget(self._log_path_label)
        return box

    # ------------------------------------------------------------------
    # Helpers: delimiter / encoding
    # ------------------------------------------------------------------

    def _selected_delimiter(self) -> str:
        mapping = {
            ", (comma)": ",",
            "; (semicolon)": ";",
            "\\t (tab)": "\t",
            "| (pipe)": "|",
        }
        return mapping.get(self._delimiter_combo.currentText(), ",")

    def _selected_encoding(self) -> str:
        return self._encoding_combo.currentText()

    # ------------------------------------------------------------------
    # Slots
    # ------------------------------------------------------------------

    def _on_convert(self) -> None:
        conn = self._conn_edit.text().strip()
        container = self._container_edit.text().strip()
        source_prefix = self._source_edit.text().strip()
        output_prefix = self._output_edit.text().strip()

        if not conn or not container:
            self._log_error("Connection string and container are required.")
            return
        if not output_prefix:
            self._log_error("Output prefix is required.")
            return

        self._convert_btn.setEnabled(False)
        self._cancel_btn.setEnabled(True)
        self._pause_btn.setEnabled(True)
        self._progress.setValue(0)
        self._progress.setVisible(True)
        self._eta_label.setText("Listing ZIP blobs…")
        self._run_start_time = perf_counter()
        self._open_log_stream()
        self._log_info(
            f"Starting conversion: {source_prefix} → {output_prefix} "
            f"(delimiter={self._selected_delimiter()!r}, "
            f"encoding={self._selected_encoding()})"
        )

        self._worker = ZipConverterWorker(
            connection_string=conn,
            container=container,
            source_prefix=source_prefix,
            output_prefix=output_prefix,
            max_workers=self._workers_spin.value(),
            autoscale=self._autoscale_check.isChecked(),
            max_attempts=self._attempts_spin.value(),
            csv_delimiter=self._selected_delimiter(),
            csv_encoding=self._selected_encoding(),
        )
        self._worker.listing_complete.connect(self._on_listing_complete)
        self._worker.progress.connect(self._on_progress)
        self._worker.file_error.connect(self._on_file_error)
        self._worker.finished.connect(self._on_finished)
        self._worker.cancelled.connect(self._on_cancelled)
        self._worker.log_message.connect(self._log_info)
        self._worker.workers_scaled.connect(self._on_workers_scaled)
        self._worker.paused.connect(self._on_worker_paused)
        self._worker.resumed.connect(self._on_worker_resumed)
        self._worker.start()

    def _on_cancel(self) -> None:
        if self._worker and self._worker.isRunning():
            self._worker.cancel()

    def _on_pause_resume(self) -> None:
        if not self._worker or not self._worker.isRunning():
            return
        if self._is_paused:
            self._worker.resume()
        else:
            self._worker.pause()

    def _on_listing_complete(self, total: int) -> None:
        self._progress.setMaximum(max(total, 1))
        self._eta_label.setText("")
        self._log_info(f"Listing complete: {total} ZIP file(s) found.")

    def _on_progress(
        self,
        completed: int,
        total: int,
        blob_name: str,
        duration_ms: float,
        worker_id: int,
        rows_converted: int,
    ) -> None:
        self._progress.setMaximum(total)
        self._progress.setValue(completed)

        elapsed = perf_counter() - self._run_start_time
        if elapsed > 1.0 and completed > 0:
            remaining = total - completed
            if remaining > 0:
                s_per_file = elapsed / completed
                eta_s = s_per_file * remaining
                finish = datetime.now() + timedelta(seconds=int(eta_s))
                finish_str = (
                    finish.strftime("%H:%M") if finish.date() == datetime.now().date()
                    else finish.strftime("%d.%m. %H:%M")
                )
                self._eta_label.setText(
                    f"ETA {_format_duration(eta_s)} (done ~{finish_str})"
                )

        width = len(str(total))
        self._log_info(
            f"[{str(completed).zfill(width)}/{total}] {blob_name}: "
            f"{rows_converted} row(s) converted ({duration_ms / 1000:.2f}s)"
        )

    def _on_file_error(self, blob_name: str, error: str) -> None:
        self._log_error(f"  FAILED: {blob_name}\n{error.strip()}")

    def _on_finished(self, result: dict) -> None:
        elapsed = perf_counter() - self._run_start_time
        self._eta_label.setText(f"Elapsed: {_format_duration(elapsed)}")
        self._log_info(
            f"Done in {_format_duration(elapsed)}: "
            f"{result['fileCount']} file(s) converted, "
            f"{result['rowCount']} total rows."
        )
        if result["fileCount"] > 0:
            self._show_notification(
                "ZIP → Parquet complete",
                f"{result['fileCount']} file(s), {result['rowCount']} rows",
            )
        self._set_idle_state()
        self._close_log_stream()
        self._schedule_worker_cleanup()

    def _on_cancelled(self) -> None:
        elapsed = perf_counter() - self._run_start_time
        self._eta_label.setText(f"Elapsed: {_format_duration(elapsed)}")
        self._log_info("Cancelled by user.")
        self._set_idle_state()
        self._close_log_stream()
        self._schedule_worker_cleanup()

    def _on_workers_scaled(self, new_count: int, old_count: int,
                           direction: str, reason: str) -> None:
        self._workers_spin.setValue(new_count)
        arrow = "↑" if direction == "up" else "↓"
        self._log_info(f"[Autoscale {arrow}] {old_count}→{new_count} workers — {reason}")

    def _on_worker_paused(self) -> None:
        self._is_paused = True
        self._pause_btn.setText("Resume")

    def _on_worker_resumed(self) -> None:
        self._is_paused = False
        self._pause_btn.setText("Pause")

    # ------------------------------------------------------------------
    # UI state helpers
    # ------------------------------------------------------------------

    def _set_idle_state(self) -> None:
        self._convert_btn.setEnabled(True)
        self._cancel_btn.setEnabled(False)
        self._pause_btn.setEnabled(False)
        self._pause_btn.setText("Pause")
        self._progress.setVisible(False)
        self._is_paused = False

    def _schedule_worker_cleanup(self) -> None:
        if self._worker_cleanup_timer is not None:
            self._worker_cleanup_timer.stop()
        timer = QTimer(self)
        timer.setSingleShot(True)
        timer.timeout.connect(self._cleanup_worker)
        timer.start(200)
        self._worker_cleanup_timer = timer

    def _cleanup_worker(self) -> None:
        if self._worker is not None:
            self._worker.deleteLater()
            self._worker = None

    # ------------------------------------------------------------------
    # Log helpers
    # ------------------------------------------------------------------

    def _log_info(self, text: str) -> None:
        self._append_log(text, color=None)

    def _log_error(self, text: str) -> None:
        self._append_log(text, color=_COLOR_ERROR)

    def _append_log(self, text: str, color: QColor | None) -> None:
        if self._log_stream is not None:
            try:
                self._log_stream.write(text + "\n")
                self._log_stream.flush()
            except Exception:
                pass
        self._log_entries.append((text, color))
        if len(self._log_entries) >= MAX_LOG_ENTRIES:
            self._flush_log_to_file()
            return
        self._write_to_widget(text, color)

    def _flush_log_to_file(self) -> None:
        self._log_flush_count += 1
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"zip_converter_log_{timestamp}_{self._log_flush_count}.txt"
        log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        path = os.path.join(log_dir, filename)
        try:
            with open(path, "w", encoding="utf-8") as f:
                for entry_text, _ in self._log_entries:
                    f.write(entry_text + "\n")
        except Exception:
            pass
        self._log_entries.clear()
        self._log.clear()
        self._write_to_widget(f"[Log flushed to {path}]", _COLOR_WARN)

    def _write_to_widget(self, text: str, color: QColor | None) -> None:
        cursor = self._log.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)
        fmt = QTextCharFormat()
        if color:
            fmt.setForeground(color)
        else:
            fmt.setForeground(self._log.palette().text().color())
        cursor.setCharFormat(fmt)
        cursor.insertText(text + "\n")
        self._log.setTextCursor(cursor)
        self._log.ensureCursorVisible()

    def _on_export_log(self) -> None:
        from PyQt6.QtWidgets import QFileDialog
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Log",
            f"zip_converter_log_{timestamp}.txt",
            "Text Files (*.txt);;All Files (*)",
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as f:
                for text, _ in self._log_entries:
                    f.write(text + "\n")
            self._log_info(f"Log exported to: {path}")
        except Exception as exc:
            self._log_error(f"Failed to export log: {exc}")

    # ------------------------------------------------------------------
    # Log streaming helpers
    # ------------------------------------------------------------------

    def _open_log_stream(self) -> None:
        self._close_log_stream()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"zip_converter_log_{timestamp}.txt"
        if _LOG_DIR_OVERRIDE:
            log_dir = _LOG_DIR_OVERRIDE
        else:
            log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
        path = os.path.join(log_dir, filename)
        try:
            os.makedirs(log_dir, exist_ok=True)
            self._log_stream = open(path, "w", encoding="utf-8")
            self._log_file_path = path
            self._log_path_label.setText(f"Streaming to: {filename}")
        except Exception:
            self._log_stream = None
            self._log_file_path = None

    def _close_log_stream(self) -> None:
        if self._log_stream is not None:
            try:
                self._log_stream.flush()
                self._log_stream.close()
            except Exception:
                pass
            self._log_stream = None
            self._log_path_label.setText("")

    def _show_notification(self, title: str, body: str) -> None:
        if not QSystemTrayIcon.isSystemTrayAvailable():
            return
        if self._tray_icon is None:
            self._tray_icon = QSystemTrayIcon(self)
            icon = QApplication.instance().windowIcon()
            if icon.isNull():
                icon = self.style().standardIcon(
                    QStyle.StandardPixmap.SP_MessageBoxInformation
                )
            self._tray_icon.setIcon(icon)
            self._tray_icon.show()
        self._tray_icon.showMessage(
            title, body, QSystemTrayIcon.MessageIcon.Information, 5000
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def closeEvent(self, event) -> None:  # type: ignore[override]
        self._close_log_stream()
        if self._worker is not None and self._worker.isRunning():
            self._worker.cancel()
            self._worker.wait(5_000)
        super().closeEvent(event)
