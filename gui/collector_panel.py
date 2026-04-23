"""Self-contained Data-Collector panel widget."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from time import perf_counter

from PyQt6.QtCore import QTimer
from PyQt6.QtGui import QColor, QTextCharFormat, QTextCursor
from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from gui.collector_schema_table import CollectorSchemaTable
from gui.resources_panel import ResourcesPanel
from gui.workers import DataCollectorWorker, SchemaLoaderWorker, _format_duration
from parquet_transform.collector import _REQUIRED_COLS


class CollectorPanel(QWidget):
    """Full Data-Collector UI — runs independently of the Schema Transformer tab."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._worker: DataCollectorWorker | None = None
        self._is_paused: bool = False
        self._run_start_time: float = 0.0
        self._worker_cleanup_timer: QTimer | None = None
        self._schema_worker: SchemaLoaderWorker | None = None

        root = QVBoxLayout(self)
        root.setSpacing(8)
        root.setContentsMargins(12, 12, 12, 12)

        root.addWidget(self._build_connection_group())
        root.addWidget(self._build_filter_group())

        self._schema_group = self._build_schema_group()
        self._schema_group.setVisible(False)
        root.addWidget(self._schema_group)

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

        layout.addWidget(QLabel("Output container:"))
        self._output_container_edit = QLineEdit()
        self._output_container_edit.setPlaceholderText("leave empty = same container")
        layout.addWidget(self._output_container_edit, stretch=1)

        return box

    def _build_filter_group(self) -> QGroupBox:
        box = QGroupBox("Collection Settings")
        layout = QVBoxLayout(box)

        # Row 1: filter column + IDs
        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Filter by:"))
        self._filter_combo = QComboBox()
        self._filter_combo.addItems(["SenderUid", "DeviceUid"])
        row1.addWidget(self._filter_combo)
        row1.addSpacing(16)
        row1.addWidget(QLabel("IDs (one per line):"))
        self._ids_edit = QPlainTextEdit()
        self._ids_edit.setMaximumHeight(80)
        self._ids_edit.setPlaceholderText("uid_001\nuid_002\nuid_003")
        row1.addWidget(self._ids_edit, stretch=1)
        layout.addLayout(row1)

        # Row 2: source + output prefix
        row2 = QHBoxLayout()
        row2.addWidget(QLabel("Source prefix:"))
        self._source_edit = QLineEdit()
        self._source_edit.setPlaceholderText("transformed/data/")
        row2.addWidget(self._source_edit, stretch=1)
        self._load_schema_btn = QPushButton("Load Schema")
        self._load_schema_btn.setFixedWidth(110)
        self._load_schema_btn.setToolTip(
            "Read the schema from the first Parquet file under Source prefix.\n"
            "Use Column Selection to pick which columns to include."
        )
        self._load_schema_btn.clicked.connect(self._on_load_schema)
        row2.addWidget(self._load_schema_btn)
        row2.addSpacing(16)
        row2.addWidget(QLabel("Output prefix:"))
        self._output_edit = QLineEdit()
        self._output_edit.setPlaceholderText("collected/")
        row2.addWidget(self._output_edit, stretch=1)
        layout.addLayout(row2)

        # Row 3: worker count + autoscale + RAM limit
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
        self._autoscale_check.setChecked(True)  # fires toggled(True) → slot disables spin
        row3.addWidget(self._autoscale_check)
        row3.addSpacing(16)
        row3.addWidget(QLabel("RAM limit (MB):"))
        self._ram_spin = QSpinBox()
        self._ram_spin.setRange(128, 8192)
        self._ram_spin.setValue(1024)
        self._ram_spin.setSingleStep(128)
        self._ram_spin.setFixedWidth(80)
        row3.addWidget(self._ram_spin)
        row3.addStretch()
        layout.addLayout(row3)

        return box

    def _build_schema_group(self) -> QGroupBox:
        box = QGroupBox("Column Selection")
        layout = QVBoxLayout(box)
        self._schema_table = CollectorSchemaTable()
        layout.addWidget(self._schema_table)
        return box

    def _build_action_row(self) -> QWidget:
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)

        self._collect_btn = QPushButton("Collect")
        self._cancel_btn = QPushButton("Cancel")
        self._cancel_btn.setEnabled(False)
        self._pause_btn = QPushButton("Pause")
        self._pause_btn.setFixedWidth(80)
        self._pause_btn.setEnabled(False)
        self._eta_label = QLabel("")
        self._eta_label.setStyleSheet("color: gray;")
        self._progress = QProgressBar()
        self._progress.setVisible(False)

        layout.addWidget(self._collect_btn)
        layout.addWidget(self._cancel_btn)
        layout.addWidget(self._pause_btn)
        layout.addWidget(self._eta_label)
        layout.addWidget(self._progress, stretch=1)

        self._collect_btn.clicked.connect(self._on_collect)
        self._cancel_btn.clicked.connect(self._on_cancel)
        self._pause_btn.clicked.connect(self._on_pause_resume)

        return widget

    def _build_log_group(self) -> QGroupBox:
        box = QGroupBox("Log")
        layout = QVBoxLayout(box)
        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setMinimumHeight(140)
        layout.addWidget(self._log)
        return box

    # ------------------------------------------------------------------
    # Slots
    # ------------------------------------------------------------------

    def _on_load_schema(self) -> None:
        conn = self._conn_edit.text().strip()
        container = self._container_edit.text().strip()
        source_prefix = self._source_edit.text().strip()

        if not conn or not container:
            self._log_error("Connection string and container are required.")
            return

        self._load_schema_btn.setEnabled(False)
        self._schema_table.clear_schema()
        self._schema_group.setVisible(False)
        self._log_info(f"Loading schema from '{source_prefix}'…")

        self._schema_worker = SchemaLoaderWorker(conn, container, source_prefix)
        self._schema_worker.schema_loaded.connect(self._on_schema_loaded)
        self._schema_worker.error.connect(self._on_schema_error)
        self._schema_worker.finished.connect(self._on_schema_worker_finished)
        self._schema_worker.start()

    def _on_schema_loaded(
        self,
        schema: object,
        file_count: int,
        total_bytes: int,
        unknown_size_names: list[str],
    ) -> None:
        self._schema_table.load_schema(schema)
        # Lock the four columns that MetadataAccumulator always needs for the
        # Parquet footer (dateFrom/dateTo/recordCount/deviceIds).  Without them
        # the writer thread crashes with a KeyError when building metadata.
        self._schema_table.lock_columns(sorted(_REQUIRED_COLS))
        self._schema_group.setVisible(True)
        plural = "s" if file_count != 1 else ""
        locked_str = ", ".join(sorted(_REQUIRED_COLS))
        self._log_info(
            f"Schema loaded: {len(schema)} column(s), {file_count} file{plural}. "
            f"Required columns locked: {locked_str}."
        )

    def _on_schema_error(self, msg: str) -> None:
        self._log_error(f"Failed to load schema: {msg}")

    def _on_schema_worker_finished(self) -> None:
        self._load_schema_btn.setEnabled(True)
        if self._schema_worker is not None:
            self._schema_worker.deleteLater()
            self._schema_worker = None

    def _on_collect(self) -> None:
        conn = self._conn_edit.text().strip()
        container = self._container_edit.text().strip()
        if not conn or not container:
            self._log_error("Connection string and container are required.")
            return

        filter_values = [
            line.strip()
            for line in self._ids_edit.toPlainText().splitlines()
            if line.strip()
        ]
        if not filter_values:
            self._log_error("Enter at least one ID.")
            return

        output_prefix = self._output_edit.text().strip()
        if not output_prefix:
            self._log_error("Output prefix is required.")
            return

        if self._schema_worker is not None and self._schema_worker.isRunning():
            self._log_error("Schema load is in progress — please wait.")
            return

        selected_columns = self._schema_table.get_selected_columns()

        self._collect_btn.setEnabled(False)
        self._cancel_btn.setEnabled(True)
        self._pause_btn.setEnabled(True)
        self._pause_btn.setText("Pause")
        self._is_paused = False
        self._run_start_time = perf_counter()
        self._eta_label.setText("")
        self._progress.setValue(0)
        self._progress.setVisible(True)
        col_note = (
            f" [{len(selected_columns)} columns]" if selected_columns is not None else ""
        )
        self._log_info(
            f"Starting collection: {self._filter_combo.currentText()} "
            f"{filter_values} → {output_prefix}{col_note}"
        )

        self._worker = DataCollectorWorker(
            connection_string=conn,
            container=container,
            source_prefix=self._source_edit.text().strip(),
            output_prefix=output_prefix,
            filter_col=self._filter_combo.currentText(),
            filter_values=filter_values,
            output_container=self._output_container_edit.text().strip(),
            max_workers=self._workers_spin.value(),
            ram_limit_mb=self._ram_spin.value(),
            autoscale=self._autoscale_check.isChecked(),
            selected_columns=selected_columns,
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

    def _on_worker_paused(self) -> None:
        self._is_paused = True
        self._pause_btn.setText("Resume")
        self._log_info("Collection paused.")

    def _on_worker_resumed(self) -> None:
        self._is_paused = False
        self._pause_btn.setText("Pause")
        self._log_info("Collection resumed.")

    def _on_listing_complete(self, total: int) -> None:
        self._progress.setMaximum(total)
        self._log_info(f"Found {total} Parquet blobs to scan.")

    def _on_progress(self, completed: int, total: int, blob_name: str, matched_rows: int) -> None:
        self._progress.setValue(completed)
        if matched_rows > 0:
            short = blob_name.rsplit("/", 1)[-1]
            self._log_info(f"  [{completed}/{total}] {short}: {matched_rows} row(s) matched")

        # ETA
        elapsed = perf_counter() - self._run_start_time
        if completed > 0 and elapsed > 1.0 and total > 0:
            s_per_blob = elapsed / completed
            remaining = total - completed
            elapsed_str = _format_duration(elapsed)
            if remaining > 0:
                eta_s = s_per_blob * remaining
                finish_time = datetime.now() + timedelta(seconds=int(eta_s))
                if finish_time.date() == datetime.now().date():
                    finish_str = finish_time.strftime("%H:%M")
                else:
                    finish_str = finish_time.strftime("%d.%m. %H:%M")
                self._eta_label.setText(
                    f"Elapsed: {elapsed_str}  ·  ETA {_format_duration(eta_s)} (~{finish_str})"
                )
            else:
                self._eta_label.setText(f"Elapsed: {elapsed_str}")

    def _on_file_error(self, blob_name: str, error: str) -> None:
        self._log_error(f"[{blob_name}]: {error}")

    def _on_finished(self, result: dict) -> None:
        self._collect_btn.setEnabled(True)
        self._cancel_btn.setEnabled(False)
        self._pause_btn.setEnabled(False)
        self._pause_btn.setText("Pause")
        self._is_paused = False
        self._eta_label.setText("")
        self._progress.setVisible(False)
        row_count = result.get("rowCount", 0)
        if row_count == 0:
            self._log_info("Collection complete: no matching rows found.")
        else:
            out_blob = result.get("outputBlob", "")
            out_container = result.get("outputContainer", "")
            self._log_info(
                f"Collection complete: {row_count} rows → [{out_container}] {out_blob}"
            )
        self._resources_panel.clear_worker_throughput()
        self._request_worker_cleanup()

    def _on_cancelled(self) -> None:
        self._collect_btn.setEnabled(True)
        self._cancel_btn.setEnabled(False)
        self._pause_btn.setEnabled(False)
        self._pause_btn.setText("Pause")
        self._is_paused = False
        self._eta_label.setText("")
        self._progress.setVisible(False)
        self._log_info("Collection cancelled.")
        self._resources_panel.clear_worker_throughput()
        self._request_worker_cleanup()

    def _on_workers_scaled(self, new_count: int, old_count: int, direction: str, reason: str) -> None:
        self._log_info(f"Workers {old_count}→{new_count} ({direction}): {reason}")

    def closeEvent(self, event) -> None:
        """Cancel any running worker and wait for it to exit before closing."""
        # Wait for schema loader (no cancel method — it's a short network call)
        if self._schema_worker is not None and self._schema_worker.isRunning():
            self._schema_worker.wait(5000)

        if self._worker is not None and self._worker.isRunning():
            self._worker.cancel()
            if not self._worker.wait(5000):
                self._log_error("Data Collector is still stopping — close cancelled.")
                event.ignore()
                return
        self._request_worker_cleanup(force=True)
        super().closeEvent(event)

    def _request_worker_cleanup(self, force: bool = False) -> None:
        """Dispose the worker only after its QThread stops running."""
        if self._worker is None:
            return
        if force or not self._worker.isRunning():
            self._finalize_worker()
            return
        if self._worker_cleanup_timer is None:
            self._worker_cleanup_timer = QTimer(self)
            self._worker_cleanup_timer.setSingleShot(True)
            self._worker_cleanup_timer.timeout.connect(self._request_worker_cleanup)
        if not self._worker_cleanup_timer.isActive():
            self._worker_cleanup_timer.start(100)

    def _finalize_worker(self) -> None:
        if self._worker is None:
            return
        if self._worker_cleanup_timer is not None and self._worker_cleanup_timer.isActive():
            self._worker_cleanup_timer.stop()
        self._worker.deleteLater()
        self._worker = None

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------

    def _log_info(self, text: str) -> None:
        self._append_log(text, color=None)

    def _log_error(self, text: str) -> None:
        self._append_log(text, color=QColor("#cc0000"))

    def _append_log(self, text: str, color: QColor | None) -> None:
        cursor = self._log.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)
        fmt = QTextCharFormat()
        if color:
            fmt.setForeground(color)
        cursor.setCharFormat(fmt)
        cursor.insertText(text + "\n")
        self._log.setTextCursor(cursor)
        self._log.ensureCursorVisible()
