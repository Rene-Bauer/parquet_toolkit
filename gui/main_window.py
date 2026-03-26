"""
Main application window for the Parquet Schema Transformer.
"""
from __future__ import annotations

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor, QFont, QTextCharFormat, QTextCursor
from PyQt6.QtWidgets import (
    QButtonGroup,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QSizePolicy,
    QStatusBar,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from gui.schema_table import SchemaTable
from gui.workers import SchemaLoaderWorker, TransformWorker


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Parquet Schema Transformer")
        self.setMinimumSize(800, 700)

        self._schema_worker: SchemaLoaderWorker | None = None
        self._transform_worker: TransformWorker | None = None
        self._file_count: int = 0

        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setSpacing(8)
        root.setContentsMargins(12, 12, 12, 12)

        root.addWidget(self._build_connection_group())
        root.addWidget(self._build_schema_group())
        root.addWidget(self._build_output_group())
        root.addWidget(self._build_action_row())
        root.addWidget(self._build_log_group())

        self._build_statusbar()
        self._set_ready_state()

    # ------------------------------------------------------------------
    # UI construction helpers
    # ------------------------------------------------------------------

    def _build_connection_group(self) -> QGroupBox:
        box = QGroupBox("Azure Connection")
        layout = QVBoxLayout(box)

        # Connection string row
        cs_row = QHBoxLayout()
        cs_label = QLabel("Connection String:")
        cs_label.setFixedWidth(120)
        self._conn_str_edit = QLineEdit()
        self._conn_str_edit.setPlaceholderText(
            "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=..."
        )
        self._conn_str_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self._toggle_cs_btn = QPushButton("👁")
        self._toggle_cs_btn.setFixedWidth(32)
        self._toggle_cs_btn.setToolTip("Show / hide connection string")
        self._toggle_cs_btn.setCheckable(True)
        self._toggle_cs_btn.toggled.connect(self._on_toggle_connection_string)
        cs_row.addWidget(cs_label)
        cs_row.addWidget(self._conn_str_edit)
        cs_row.addWidget(self._toggle_cs_btn)
        layout.addLayout(cs_row)

        # Container + prefix row
        cp_row = QHBoxLayout()
        container_label = QLabel("Container:")
        container_label.setFixedWidth(120)
        self._container_edit = QLineEdit()
        self._container_edit.setPlaceholderText("my-container")
        prefix_label = QLabel("Folder Prefix:")
        self._prefix_edit = QLineEdit()
        self._prefix_edit.setPlaceholderText("raw/events/")
        self._load_btn = QPushButton("Load Schema")
        self._load_btn.setFixedWidth(110)
        self._load_btn.clicked.connect(self._on_load_schema)
        cp_row.addWidget(container_label)
        cp_row.addWidget(self._container_edit)
        cp_row.addSpacing(16)
        cp_row.addWidget(prefix_label)
        cp_row.addWidget(self._prefix_edit)
        cp_row.addSpacing(8)
        cp_row.addWidget(self._load_btn)
        layout.addLayout(cp_row)

        return box

    def _build_schema_group(self) -> QGroupBox:
        self._schema_group = QGroupBox("Schema")
        layout = QVBoxLayout(self._schema_group)
        self._schema_table = SchemaTable()
        layout.addWidget(self._schema_table)
        return self._schema_group

    def _build_output_group(self) -> QGroupBox:
        box = QGroupBox("Output")
        layout = QHBoxLayout(box)

        self._inplace_radio = QRadioButton("In-place (overwrite source files)")
        self._newprefix_radio = QRadioButton("New prefix:")
        self._output_prefix_edit = QLineEdit()
        self._output_prefix_edit.setPlaceholderText("transformed/events/")
        self._output_prefix_edit.setEnabled(False)

        group = QButtonGroup(self)
        group.addButton(self._inplace_radio)
        group.addButton(self._newprefix_radio)
        self._inplace_radio.setChecked(True)

        self._newprefix_radio.toggled.connect(self._output_prefix_edit.setEnabled)

        layout.addWidget(self._inplace_radio)
        layout.addWidget(self._newprefix_radio)
        layout.addWidget(self._output_prefix_edit, stretch=1)
        return box

    def _build_action_row(self) -> QWidget:
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)

        self._dryrun_btn = QPushButton("Dry Run")
        self._dryrun_btn.setFixedWidth(100)
        self._dryrun_btn.setToolTip(
            "Simulate the transformation — no files will be uploaded"
        )
        self._dryrun_btn.clicked.connect(lambda: self._on_apply(dry_run=True))

        self._apply_btn = QPushButton("Apply to All Files")
        self._apply_btn.setFixedWidth(150)
        self._apply_btn.clicked.connect(lambda: self._on_apply(dry_run=False))

        self._cancel_btn = QPushButton("Cancel")
        self._cancel_btn.setFixedWidth(80)
        self._cancel_btn.setEnabled(False)
        self._cancel_btn.clicked.connect(self._on_cancel)

        layout.addWidget(self._dryrun_btn)
        layout.addStretch()
        layout.addWidget(self._apply_btn)
        layout.addWidget(self._cancel_btn)
        return widget

    def _build_log_group(self) -> QGroupBox:
        box = QGroupBox("Log")
        layout = QVBoxLayout(box)
        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setFont(QFont("Consolas", 9))
        self._log.setMinimumHeight(140)
        layout.addWidget(self._log)
        return box

    def _build_statusbar(self) -> None:
        bar = QStatusBar()
        self.setStatusBar(bar)
        self._progress = QProgressBar()
        self._progress.setMaximumWidth(300)
        self._progress.setVisible(False)
        self._status_label = QLabel("Ready")
        bar.addWidget(self._status_label)
        bar.addPermanentWidget(self._progress)

    # ------------------------------------------------------------------
    # State helpers
    # ------------------------------------------------------------------

    def _set_ready_state(self) -> None:
        self._load_btn.setEnabled(True)
        self._dryrun_btn.setEnabled(False)
        self._apply_btn.setEnabled(False)
        self._cancel_btn.setEnabled(False)
        self._progress.setVisible(False)

    def _set_loading_state(self) -> None:
        self._load_btn.setEnabled(False)
        self._dryrun_btn.setEnabled(False)
        self._apply_btn.setEnabled(False)
        self._status_label.setText("Loading schema…")

    def _set_schema_loaded_state(self) -> None:
        self._load_btn.setEnabled(True)
        self._dryrun_btn.setEnabled(True)
        self._apply_btn.setEnabled(True)
        self._cancel_btn.setEnabled(False)
        self._progress.setVisible(False)

    def _set_processing_state(self) -> None:
        self._load_btn.setEnabled(False)
        self._dryrun_btn.setEnabled(False)
        self._apply_btn.setEnabled(False)
        self._cancel_btn.setEnabled(True)
        self._progress.setValue(0)
        self._progress.setVisible(True)

    # ------------------------------------------------------------------
    # Slot implementations
    # ------------------------------------------------------------------

    def _on_toggle_connection_string(self, checked: bool) -> None:
        mode = QLineEdit.EchoMode.Normal if checked else QLineEdit.EchoMode.Password
        self._conn_str_edit.setEchoMode(mode)

    def _on_load_schema(self) -> None:
        conn = self._conn_str_edit.text().strip()
        container = self._container_edit.text().strip()
        prefix = self._prefix_edit.text().strip()

        if not conn or not container:
            self._log_error("Connection string and container are required.")
            return

        self._schema_table.clear_schema()
        self._set_loading_state()
        self._log_info(f"Connecting to container '{container}', prefix '{prefix}'…")

        self._schema_worker = SchemaLoaderWorker(conn, container, prefix)
        self._schema_worker.schema_loaded.connect(self._on_schema_loaded)
        self._schema_worker.error.connect(self._on_load_error)
        self._schema_worker.start()

    def _on_schema_loaded(self, schema, file_count: int) -> None:
        self._file_count = file_count
        self._schema_group.setTitle(
            f"Schema  ({file_count} .parquet file{'s' if file_count != 1 else ''} found)"
        )
        self._schema_table.load_schema(schema)
        self._set_schema_loaded_state()
        self._log_info(
            f"Schema loaded: {len(schema)} column(s), {file_count} file(s) in folder."
        )

    def _on_load_error(self, msg: str) -> None:
        self._set_schema_loaded_state()
        self._dryrun_btn.setEnabled(False)
        self._apply_btn.setEnabled(False)
        self._load_btn.setEnabled(True)
        self._log_error(f"Failed to load schema:\n{msg}")
        self._status_label.setText("Error — see log")

    def _on_apply(self, dry_run: bool) -> None:
        col_configs = self._schema_table.get_column_configs()
        if not col_configs:
            self._log_info("No transforms selected — nothing to do.")
            return

        conn = self._conn_str_edit.text().strip()
        container = self._container_edit.text().strip()
        prefix = self._prefix_edit.text().strip()

        output_prefix: str | None = None
        if self._newprefix_radio.isChecked():
            output_prefix = self._output_prefix_edit.text().strip() or None

        label = "[DRY RUN] " if dry_run else ""
        self._log_info(
            f"{label}Starting — {self._file_count} file(s), "
            f"{len(col_configs)} transform(s)…"
        )

        self._set_processing_state()
        self._progress.setMaximum(self._file_count)

        self._transform_worker = TransformWorker(
            connection_string=conn,
            container=container,
            prefix=prefix,
            col_configs=col_configs,
            output_prefix=output_prefix,
            dry_run=dry_run,
        )
        self._transform_worker.progress.connect(self._on_transform_progress)
        self._transform_worker.file_error.connect(self._on_file_error)
        self._transform_worker.finished.connect(self._on_transform_finished)
        self._transform_worker.cancelled.connect(self._on_transform_cancelled)
        self._transform_worker.start()

    def _on_cancel(self) -> None:
        if self._transform_worker and self._transform_worker.isRunning():
            self._cancel_btn.setEnabled(False)
            self._log_info("Cancelling after current file…")
            self._transform_worker.cancel()

    def _on_transform_progress(self, current: int, total: int, blob_name: str) -> None:
        self._progress.setValue(current)
        self._status_label.setText(f"{current} / {total} files")
        short = blob_name.split("/")[-1]
        self._log_info(f"  Processing ({current}/{total}): {short}")

    def _on_file_error(self, blob_name: str, tb: str) -> None:
        self._log_error(f"  FAILED: {blob_name}\n{tb.strip()}")

    def _on_transform_finished(self, processed: int, failed: int) -> None:
        self._set_schema_loaded_state()
        msg = f"Done. {processed} succeeded"
        if failed:
            msg += f", {failed} failed"
        msg += "."
        self._log_info(msg)
        self._status_label.setText(msg)

    def _on_transform_cancelled(self) -> None:
        self._set_schema_loaded_state()
        self._log_info("Cancelled by user.")
        self._status_label.setText("Cancelled")

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
