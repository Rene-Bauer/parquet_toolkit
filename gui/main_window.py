"""
Main application window for the Parquet Schema Transformer.
"""
from __future__ import annotations

import csv
import ctypes
import os
import sys
from datetime import datetime, timedelta
from time import perf_counter

import psutil

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor, QFont, QTextCharFormat, QTextCursor
from PyQt6.QtWidgets import (
    QButtonGroup,
    QCheckBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QSpinBox,
    QStatusBar,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from gui.resources_panel import ResourcesPanel
from gui.schema_table import SchemaTable
from gui.system_monitor_worker import SystemMonitorWorker
from gui.workers import SchemaLoaderWorker, TransformWorker, _format_bytes
from gui.collector_panel import CollectorPanel
from parquet_transform.checkpoint import FailedList, RunCheckpoint

# Derive a sensible worker cap from available RAM.
# Each active worker holds roughly 80 MB in-flight:
#   ~24 MB input download buffer  +  PyArrow table in memory  +  ~10 MB zstd output buffer.
# Reserve 2 GB for the OS and the application itself.
# Hard ceiling: 512 (OS scheduler overhead becomes significant beyond that; network
# is always the real bottleneck long before RAM runs out on a typical dev machine).
_BYTES_PER_WORKER: int = 80 * 1024 * 1024   # 80 MB
_RAM_RESERVE: int      = 2 * 1024 * 1024 * 1024  # 2 GB
MAX_WORKERS: int = max(4, min(512, int(
    (psutil.virtual_memory().total - _RAM_RESERVE) / _BYTES_PER_WORKER
)))

DEFAULT_MAX_ATTEMPTS = 5
MAX_LOG_ENTRIES = 5_000
"""Maximum number of log lines kept in memory. Oldest entries are dropped first."""

# Log colour palette
_COLOR_ERROR   = QColor("#cc0000")   # red   – network failures
_COLOR_CORRUPT = QColor("#cc6600")   # orange – corrupted / non-retriable
_COLOR_WARN    = QColor("#aa8800")   # amber  – warnings (unknown size etc.)

# Windows standby prevention via SetThreadExecutionState
_ES_CONTINUOUS      = 0x80000000
_ES_SYSTEM_REQUIRED = 0x00000001


def _prevent_standby() -> None:
    """Tell Windows to keep the system awake (no standby/sleep)."""
    if sys.platform == "win32":
        ctypes.windll.kernel32.SetThreadExecutionState(
            _ES_CONTINUOUS | _ES_SYSTEM_REQUIRED
        )


def _allow_standby() -> None:
    """Restore normal Windows standby behaviour."""
    if sys.platform == "win32":
        ctypes.windll.kernel32.SetThreadExecutionState(_ES_CONTINUOUS)


def _format_duration(seconds: float) -> str:
    """Format seconds as 'Xh Ym Zs' / 'Xm Ys' / 'X.XXs'."""
    if seconds < 60:
        return f"{seconds:.2f}s"
    if seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours}h {minutes}m {secs}s"


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Parquet Schema Transformer")
        self.setMinimumSize(920, 780)

        self._schema_worker: SchemaLoaderWorker | None = None
        self._transform_worker: TransformWorker | None = None
        self._file_count: int = 0
        self._dry_run: bool = False

        # Improvement 9: unlimited retry depth (counter instead of bool flag)
        self._retry_depth: int = 0

        # Unhealthy-blob tracking (reset on each fresh run)
        self._unknown_size_blobs: list[str] = []
        self._corrupted_blobs: list[tuple[str, str, int]] = []  # (name, reason, size_bytes)

        # Improvement 3: blob sizes from listing passed to retry batches
        self._blob_sizes: dict[str, int] = {}

        # Feature 5: multi-prefix batch queue
        self._pending_prefixes: list[str] = []
        self._current_prefix_index: int = 0
        self._active_prefix: str = ""

        # Feature 4: ETA / throughput tracking
        self._run_start_time: float = 0.0
        self._bytes_processed: int = 0
        self._files_completed_in_run: int = 0
        self._total_files_in_run: int = 0

        # Feature 13: pause state
        self._is_paused: bool = False

        # Feature 14: log storage for filtering and export
        self._log_entries: list[tuple[str, QColor | None]] = []
        self._log_filter: str = ""
        self._log_flush_count: int = 0  # number of times log was flushed to file

        central = QWidget()
        self.setCentralWidget(central)

        tabs = QTabWidget(central)
        outer = QVBoxLayout(central)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(tabs)

        # --- Tab 1: Schema Transformer (existing layout, completely unchanged) ---
        transformer_widget = QWidget()
        root = QVBoxLayout(transformer_widget)
        root.setSpacing(8)
        root.setContentsMargins(12, 12, 12, 12)

        root.addWidget(self._build_connection_group())
        root.addWidget(self._build_schema_group())
        root.addWidget(self._build_output_group())
        root.addWidget(self._build_action_row())

        # System resources panel — lives between action row and log
        self._resources_panel = ResourcesPanel()
        root.addWidget(self._resources_panel)

        root.addWidget(self._build_log_group())

        tabs.addTab(transformer_widget, "Schema Transformer")

        # --- Tab 2: Data Collector ---
        self._collector_panel = CollectorPanel()
        tabs.addTab(self._collector_panel, "Data Collector")

        self._build_statusbar()

        # Start background system monitor (runs for the whole app lifetime)
        self._sys_monitor = SystemMonitorWorker(interval_ms=1000, parent=self)
        self._sys_monitor.snapshot_ready.connect(self._resources_panel.update_snapshot)
        self._sys_monitor.start()

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
        cs_label.setFixedWidth(130)
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

        # Container / prefix row
        cp_row = QHBoxLayout()
        container_label = QLabel("Container:")
        container_label.setFixedWidth(130)
        self._container_edit = QLineEdit()
        self._container_edit.setPlaceholderText("my-container")
        prefix_label = QLabel("Folder Prefix:")
        self._prefix_edit = QLineEdit()
        self._prefix_edit.setPlaceholderText("raw/events/2026/03/")
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

        # Feature 5: additional batch prefixes
        batch_row = QHBoxLayout()
        batch_label = QLabel("Batch Prefixes:")
        batch_label.setFixedWidth(130)
        batch_label.setToolTip(
            "Optional – one prefix per line.\n"
            "Apply will process the main prefix first, then each line here sequentially."
        )
        self._batch_prefixes_edit = QPlainTextEdit()
        self._batch_prefixes_edit.setPlaceholderText(
            "raw/events/2026/01/\nraw/events/2026/02/\n(optional – one per line)"
        )
        self._batch_prefixes_edit.setMaximumHeight(54)
        self._batch_prefixes_edit.setFont(QFont("Consolas", 8))
        batch_row.addWidget(batch_label)
        batch_row.addWidget(self._batch_prefixes_edit)
        layout.addLayout(batch_row)

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

        worker_label = QLabel("Workers:")
        worker_label.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignRight)
        worker_label.setFixedWidth(70)
        self._worker_spin = QSpinBox()
        self._worker_spin.setRange(1, MAX_WORKERS)
        self._worker_spin.setValue(self._default_worker_count())
        self._worker_spin.setToolTip("Number of parallel worker threads")

        _ram_gb = psutil.virtual_memory().total / (1024 ** 3)
        self._max_workers_label = QLabel(f"/ {MAX_WORKERS}")
        self._max_workers_label.setToolTip(
            f"Hard cap derived from available RAM.\n"
            f"System RAM: {_ram_gb:.0f} GB  ·  ~80 MB reserved per worker  ·  2 GB for OS\n"
            f"→ max {MAX_WORKERS} parallel workers\n\n"
            f"The autoscaler will stay well below this based on measured network bandwidth."
        )

        self._autoscale_check = QCheckBox("Auto-Scale")
        self._autoscale_check.setToolTip(
            "Automatically calibrate the worker count based on measured upload bandwidth.\n"
            "Processes the first few files with 2 workers, measures throughput,\n"
            "then scales up to the optimal number for your connection."
        )
        self._autoscale_check.setChecked(True)
        self._autoscale_check.toggled.connect(self._on_autoscale_toggled)

        attempts_label = QLabel("Attempts:")
        attempts_label.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignRight)
        self._attempts_spin = QSpinBox()
        self._attempts_spin.setRange(1, 20)
        self._attempts_spin.setValue(DEFAULT_MAX_ATTEMPTS)
        self._attempts_spin.setToolTip(
            "Attempts per file (with exponential back-off) before auto-retry batch"
        )

        self._dryrun_btn = QPushButton("Dry Run")
        self._dryrun_btn.setFixedWidth(100)
        self._dryrun_btn.setToolTip("Simulate the transformation — no files will be uploaded")
        self._dryrun_btn.clicked.connect(lambda: self._on_apply(dry_run=True))

        self._failed_btn = QPushButton("Process Failed Files")
        self._failed_btn.setFixedWidth(160)
        self._failed_btn.setToolTip(
            "Retry all files that failed in previous runs for this prefix.\n"
            "Successfully processed files are removed from the failed list."
        )
        self._failed_btn.setEnabled(False)
        self._failed_btn.clicked.connect(self._on_process_failed)

        self._apply_btn = QPushButton("Apply to All Files")
        self._apply_btn.setFixedWidth(150)
        self._apply_btn.clicked.connect(lambda: self._on_apply(dry_run=False))

        # Feature 13: Pause / Resume
        self._pause_btn = QPushButton("Pause")
        self._pause_btn.setFixedWidth(80)
        self._pause_btn.setEnabled(False)
        self._pause_btn.clicked.connect(self._on_pause_resume)

        self._cancel_btn = QPushButton("Cancel")
        self._cancel_btn.setFixedWidth(80)
        self._cancel_btn.setEnabled(False)
        self._cancel_btn.clicked.connect(self._on_cancel)

        layout.addWidget(worker_label)
        layout.addWidget(self._worker_spin)
        layout.addWidget(self._max_workers_label)
        layout.addWidget(self._autoscale_check)
        layout.addSpacing(12)
        layout.addWidget(attempts_label)
        layout.addWidget(self._attempts_spin)
        layout.addSpacing(12)
        layout.addWidget(self._dryrun_btn)
        layout.addWidget(self._failed_btn)
        layout.addStretch()
        layout.addWidget(self._apply_btn)
        layout.addWidget(self._pause_btn)
        layout.addWidget(self._cancel_btn)
        return widget

    def _build_log_group(self) -> QGroupBox:
        box = QGroupBox("Log")
        layout = QVBoxLayout(box)

        # Feature 14: filter toolbar
        toolbar = QHBoxLayout()

        filter_label = QLabel("Filter:")
        self._log_filter_edit = QLineEdit()
        self._log_filter_edit.setPlaceholderText("Search log…")
        self._log_filter_edit.setFixedWidth(200)
        self._log_filter_edit.textChanged.connect(self._on_log_filter_changed)

        btn_all = QPushButton("All")
        btn_all.setFixedWidth(40)
        btn_all.clicked.connect(lambda: self._set_log_filter(""))

        btn_failed = QPushButton("Failed")
        btn_failed.setFixedWidth(55)
        btn_failed.clicked.connect(lambda: self._set_log_filter("FAILED"))

        btn_skipped = QPushButton("Skipped")
        btn_skipped.setFixedWidth(60)
        btn_skipped.clicked.connect(lambda: self._set_log_filter("SKIPPED"))

        btn_corrupted = QPushButton("Corrupted")
        btn_corrupted.setFixedWidth(70)
        btn_corrupted.clicked.connect(lambda: self._set_log_filter("CORRUPTED"))

        # Feature 2: log export
        btn_export_log = QPushButton("Export Log…")
        btn_export_log.setFixedWidth(95)
        btn_export_log.setToolTip("Save all log entries to a .txt file")
        btn_export_log.clicked.connect(self._on_export_log)

        # Feature 3: unhealthy blob CSV export
        btn_export_csv = QPushButton("Export Unhealthy CSV…")
        btn_export_csv.setFixedWidth(160)
        btn_export_csv.setToolTip("Save corrupted / unknown-size blob report to a .csv file")
        btn_export_csv.clicked.connect(self._on_export_unhealthy_csv)

        toolbar.addWidget(filter_label)
        toolbar.addWidget(self._log_filter_edit)
        toolbar.addSpacing(6)
        toolbar.addWidget(btn_all)
        toolbar.addWidget(btn_failed)
        toolbar.addWidget(btn_skipped)
        toolbar.addWidget(btn_corrupted)
        toolbar.addStretch()
        toolbar.addWidget(btn_export_log)
        toolbar.addWidget(btn_export_csv)
        layout.addLayout(toolbar)

        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setFont(QFont("Consolas", 9))
        self._log.setMinimumHeight(140)
        # Safety net: Qt-level hard cap so the widget never grows unbounded
        self._log.document().setMaximumBlockCount(MAX_LOG_ENTRIES + 100)
        layout.addWidget(self._log)
        return box

    def _build_statusbar(self) -> None:
        bar = QStatusBar()
        self.setStatusBar(bar)
        self._progress = QProgressBar()
        self._progress.setMaximumWidth(300)
        self._progress.setVisible(False)
        self._status_label = QLabel("Ready")
        # Feature 4: throughput / ETA label
        self._throughput_label = QLabel("")
        self._throughput_label.setAlignment(Qt.AlignmentFlag.AlignRight)
        bar.addWidget(self._status_label)
        bar.addPermanentWidget(self._throughput_label)
        bar.addPermanentWidget(self._progress)

    # ------------------------------------------------------------------
    # UI state helpers
    # ------------------------------------------------------------------

    def _set_ready_state(self) -> None:
        self._load_btn.setEnabled(True)
        self._dryrun_btn.setEnabled(False)
        self._apply_btn.setEnabled(False)
        self._failed_btn.setEnabled(False)
        self._pause_btn.setEnabled(False)
        self._cancel_btn.setEnabled(False)
        self._progress.setVisible(False)
        self._throughput_label.setText("")

    def _set_loading_state(self) -> None:
        self._load_btn.setEnabled(False)
        self._dryrun_btn.setEnabled(False)
        self._apply_btn.setEnabled(False)
        self._status_label.setText("Loading schema...")

    def _set_schema_loaded_state(self) -> None:
        _allow_standby()
        self._load_btn.setEnabled(True)
        self._dryrun_btn.setEnabled(True)
        self._apply_btn.setEnabled(True)
        self._failed_btn.setEnabled(True)
        self._pause_btn.setEnabled(False)
        self._pause_btn.setText("Pause")
        self._cancel_btn.setEnabled(False)
        self._progress.setVisible(False)
        self._throughput_label.setText("")
        self._schema_table.setEnabled(True)
        self._is_paused = False
        # Reset bandwidth cap label — no live measurement outside a run
        self._max_workers_label.setText(f"/ {MAX_WORKERS}")

    def _set_processing_state(self) -> None:
        self._load_btn.setEnabled(False)
        self._dryrun_btn.setEnabled(False)
        self._apply_btn.setEnabled(False)
        self._failed_btn.setEnabled(False)
        self._pause_btn.setEnabled(True)
        self._pause_btn.setText("Pause")
        self._cancel_btn.setEnabled(True)
        self._progress.setValue(0)
        self._progress.setVisible(True)
        self._schema_table.setEnabled(False)
        self._is_paused = False

    # ------------------------------------------------------------------
    # Signal slots
    # ------------------------------------------------------------------

    def _on_autoscale_toggled(self, checked: bool) -> None:
        """When Auto-Scale is on, the spinner shows the current value but is
        read-only (the calibration phase will set it).  When off, the user
        controls the worker count directly."""
        self._worker_spin.setEnabled(not checked)
        if checked:
            self._worker_spin.setToolTip(
                "Auto-Scale is ON — workers will be set automatically "
                "after calibration"
            )
        else:
            self._worker_spin.setToolTip("Number of parallel worker threads")

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

        # Reset unhealthy tracking for the new load
        self._unknown_size_blobs = []
        self._corrupted_blobs = []
        self._blob_sizes = {}

        self._schema_table.clear_schema()
        self._set_loading_state()
        self._log_info(f"Connecting to container '{container}', prefix '{prefix}'...")

        self._schema_worker = SchemaLoaderWorker(conn, container, prefix)
        self._schema_worker.schema_loaded.connect(self._on_schema_loaded)
        self._schema_worker.error.connect(self._on_load_error)
        self._schema_worker.start()

    def _on_schema_loaded(
        self,
        schema,
        file_count: int,
        total_bytes: int,
        unknown_size_names: list,
    ) -> None:
        self._file_count = file_count
        self._unknown_size_blobs = list(unknown_size_names)
        known_count = file_count - len(unknown_size_names)

        # Schema group title
        if unknown_size_names:
            size_tag = (
                f"{_format_bytes(total_bytes)} · "
                f"{len(unknown_size_names)} size unknown"
            )
        else:
            size_tag = _format_bytes(total_bytes)
        plural = "s" if file_count != 1 else ""
        self._schema_group.setTitle(
            f"Schema  ({file_count} .parquet file{plural} found · {size_tag})"
        )

        self._schema_table.load_schema(schema)
        self._set_schema_loaded_state()

        # Log summary
        if unknown_size_names:
            self._log_info(
                f"Schema loaded: {len(schema)} column(s), {file_count} file(s). "
                f"{known_count} with known size ({_format_bytes(total_bytes)} total), "
                f"{len(unknown_size_names)} size unknown."
            )
            self._append_log(
                f"  ⚠ {len(unknown_size_names)} blob(s) with unknown size "
                f"(excluded from total):",
                color=_COLOR_WARN,
            )
            for name in unknown_size_names[:50]:
                self._append_log(f"    • {name}  [size unknown]", color=_COLOR_WARN)
            if len(unknown_size_names) > 50:
                self._append_log(
                    f"    ... and {len(unknown_size_names) - 50} more (not shown)",
                    color=_COLOR_WARN,
                )
        else:
            self._log_info(
                f"Schema loaded: {len(schema)} column(s), {file_count} file(s), "
                f"{_format_bytes(total_bytes)} total."
            )

    def _on_load_error(self, msg: str) -> None:
        self._set_schema_loaded_state()
        self._dryrun_btn.setEnabled(False)
        self._apply_btn.setEnabled(False)
        self._failed_btn.setEnabled(False)
        self._load_btn.setEnabled(True)
        self._log_error(f"Failed to load schema:\n{msg}")
        self._status_label.setText("Error — see log")

    def _on_process_failed(self) -> None:
        """Retry all files in the FailedList for the current container+prefix."""
        container = self._container_edit.text().strip()
        prefix = self._prefix_edit.text().strip()

        col_configs = self._schema_table.get_column_configs()
        if not col_configs:
            self._log_info("[Failed List] No transforms selected — nothing to do.")
            return

        try:
            fl = FailedList.load_or_create(container, prefix)
        except RuntimeError as exc:
            QMessageBox.critical(self, "Corrupt Failed List", str(exc))
            return

        if not fl.entries:
            self._log_info("[Failed List] No failed files recorded for this prefix.")
            return

        entries = fl.entries
        lines = [f"  \u2022 {e['name']}  ({e['type']})" for e in entries[:10]]
        if len(entries) > 10:
            lines.append(f"  ... and {len(entries) - 10} more")

        msg = QMessageBox(self)
        msg.setWindowTitle("Process Failed Files")
        msg.setText(
            f"{len(entries)} file(s) failed in previous run(s):\n\n"
            + "\n".join(lines)
            + "\n\nProcess them now using the current transforms?"
        )
        process_btn = msg.addButton("Process", QMessageBox.ButtonRole.AcceptRole)
        msg.addButton("Cancel", QMessageBox.ButtonRole.RejectRole)
        msg.exec()

        if msg.clickedButton() != process_btn:
            return

        self._log_info(f"[Failed List] Processing {len(entries)} failed file(s)...")
        self._dry_run = False
        self._retry_depth = 0
        self._corrupted_blobs = []
        self._pending_prefixes = []
        self._run_start_time = perf_counter()
        self._bytes_processed = 0
        self._files_completed_in_run = 0
        self._total_files_in_run = len(entries)

        self._start_transform(
            blob_names=fl.blob_names(),
            failed_list=fl,
        )

    def _on_apply(self, dry_run: bool) -> None:
        col_configs = self._schema_table.get_column_configs()
        if not col_configs:
            self._log_info("No transforms selected — nothing to do.")
            return

        self._dry_run = dry_run
        self._retry_depth = 0
        self._corrupted_blobs = []
        _prevent_standby()

        # Feature 5: build prefix queue
        main_prefix = self._prefix_edit.text().strip()
        extra_raw = self._batch_prefixes_edit.toPlainText().strip()
        extra_lines = [ln.strip() for ln in extra_raw.splitlines() if ln.strip()]
        self._pending_prefixes = [main_prefix] + extra_lines
        self._current_prefix_index = 0
        self._blob_sizes = {}

        # Feature 4: reset run-level tracking
        self._run_start_time = perf_counter()
        self._bytes_processed = 0
        self._files_completed_in_run = 0
        self._total_files_in_run = 0

        if len(self._pending_prefixes) > 1:
            self._log_info(
                f"Batch mode: {len(self._pending_prefixes)} prefix(es) queued."
            )

        self._start_next_prefix()

    # ------------------------------------------------------------------
    # Multi-prefix batch orchestration
    # ------------------------------------------------------------------

    def _resolve_checkpoint(
        self, container: str, prefix: str, output_prefix: str | None
    ) -> tuple[RunCheckpoint, FailedList, bool] | None:
        """
        Check for an existing checkpoint, show dialogs as needed, and return
        (checkpoint, failed_list, retry_failed), or None if the user cancelled
        on a complete checkpoint (caller should skip this prefix).

        Dialog logic:
        - complete checkpoint    → ask Start Fresh (or cancel — returns None)
        - in_progress checkpoint → ask Resume / Start Fresh
        - failed entries present on resume → ask Retry Failed Files
        """
        try:
            cp = RunCheckpoint.load_or_create(container, prefix, output_prefix)
            fl = FailedList.load_or_create(container, prefix)
        except RuntimeError as exc:
            QMessageBox.critical(self, "Corrupt Checkpoint", str(exc))
            return None
        retry_failed = False
        resuming = False

        if cp.is_complete():
            msg = QMessageBox(self)
            msg.setWindowTitle("Previous Run Complete")
            msg.setText(
                f"The previous run for prefix '{prefix}' completed successfully.\n\n"
                "Start a fresh run?"
            )
            fresh_btn = msg.addButton("Start Fresh", QMessageBox.ButtonRole.AcceptRole)
            msg.addButton("Cancel", QMessageBox.ButtonRole.RejectRole)
            msg.exec()
            if msg.clickedButton() != fresh_btn:
                return None  # user cancelled — caller should skip this prefix
            cp.reset()
            fl.clear()

        elif cp.cursor is not None:
            resuming = True
            # in_progress with a cursor — previous run was interrupted
            msg = QMessageBox(self)
            msg.setWindowTitle("Resume Previous Run")
            msg.setText(
                f"A previous run for prefix '{prefix}' was interrupted.\n"
                f"Last processed: {cp.cursor}\n\n"
                "Resume from where it left off, or start fresh?"
            )
            resume_btn = msg.addButton("Resume", QMessageBox.ButtonRole.AcceptRole)
            fresh_btn = msg.addButton("Start Fresh", QMessageBox.ButtonRole.DestructiveRole)
            msg.exec()
            if msg.clickedButton() == fresh_btn:
                resuming = False
                cp.reset()
                fl.clear()

        # Failed-list dialog: shown only when resuming
        if resuming and fl.entries:
            total_failed = len(fl.entries)
            msg = QMessageBox(self)
            msg.setWindowTitle("Previously Failed Files")
            msg.setText(
                f"{total_failed} file(s) failed in a previous run "
                f"({fl.corrupt_count} corrupt, {fl.network_count} network).\n\n"
                "Retry them in this run?"
            )
            retry_btn = msg.addButton("Retry Failed Files", QMessageBox.ButtonRole.AcceptRole)
            msg.addButton("Skip", QMessageBox.ButtonRole.RejectRole)
            msg.exec()
            retry_failed = (msg.clickedButton() == retry_btn)

        return cp, fl, retry_failed

    def _start_next_prefix(self) -> None:
        """Advance to the next prefix in the queue and start processing."""
        while self._current_prefix_index < len(self._pending_prefixes):
            prefix = self._pending_prefixes[self._current_prefix_index]
            self._active_prefix = prefix
            self._blob_sizes = {}  # fresh sizes for each new prefix

            batch_count = len(self._pending_prefixes)
            if batch_count > 1:
                self._log_info(
                    f"[{self._current_prefix_index + 1}/{batch_count}] "
                    f"Processing prefix: '{prefix}'"
                )

            container = self._container_edit.text().strip()
            output_prefix: str | None = None
            if self._newprefix_radio.isChecked():
                output_prefix = self._output_prefix_edit.text().strip() or None

            result = self._resolve_checkpoint(container, prefix, output_prefix)
            if result is None:
                self._log_info(f"[Checkpoint] Skipping prefix '{prefix}' — previous run was complete.")
                self._current_prefix_index += 1
                continue
            checkpoint, failed_list, retry_failed = result

            self._start_transform(
                blob_names=None,
                prefix_override=prefix,
                checkpoint=checkpoint,
                failed_list=failed_list,
                retry_failed=retry_failed,
            )
            return

        self._log_info("All prefixes in batch completed.")
        self._set_schema_loaded_state()

    def _start_transform(
        self,
        blob_names: list[str] | None = None,
        blob_sizes: dict[str, int] | None = None,
        prefix_override: str | None = None,
        checkpoint: RunCheckpoint | None = None,
        failed_list: FailedList | None = None,
        retry_failed: bool = False,
    ) -> None:
        """Create and start a TransformWorker."""
        # Improvement 8: always read connection fields at call time (not cached)
        conn = self._conn_str_edit.text().strip()
        container = self._container_edit.text().strip()
        prefix = prefix_override if prefix_override is not None else self._prefix_edit.text().strip()

        output_prefix: str | None = None
        if self._newprefix_radio.isChecked():
            output_prefix = self._output_prefix_edit.text().strip() or None

        col_configs = self._schema_table.get_column_configs()
        worker_count = self._worker_spin.value()
        max_attempts = self._attempts_spin.value()
        autoscale = self._autoscale_check.isChecked()
        label = "[DRY RUN] " if self._dry_run else ""

        if blob_names is not None:
            self._log_info(
                f"{label}Auto-retry #{self._retry_depth} — {len(blob_names)} file(s), "
                f"{worker_count} worker(s), {max_attempts} attempt(s)/file..."
            )
        else:
            mode_str = " (Auto-Scale ON)" if autoscale else ""
            self._log_info(
                f"{label}Starting — {len(col_configs)} transform(s), "
                f"{worker_count} worker(s){mode_str}, "
                f"{max_attempts} attempt(s)/file..."
            )

        file_count_for_bar = len(blob_names) if blob_names is not None else self._file_count
        self._set_processing_state()
        self._progress.setMaximum(max(file_count_for_bar, 1))

        self._transform_worker = TransformWorker(
            connection_string=conn,
            container=container,
            prefix=prefix,
            col_configs=col_configs,
            output_prefix=output_prefix,
            dry_run=self._dry_run,
            worker_count=worker_count,
            max_worker_cap=self._worker_spin.maximum(),
            max_attempts=max_attempts,
            blob_names=blob_names,
            blob_sizes=blob_sizes,
            autoscale=autoscale,
            checkpoint=checkpoint,
            failed_list=failed_list,
            retry_failed=retry_failed,
        )
        self._transform_worker.listing_complete.connect(self._on_listing_complete)
        self._transform_worker.progress.connect(self._on_transform_progress)
        self._transform_worker.retry_batch_started.connect(self._on_retry_batch_started)
        self._transform_worker.workers_reduced.connect(self._on_workers_reduced)
        self._transform_worker.file_error.connect(self._on_file_error)
        self._transform_worker.corrupted_blob.connect(self._on_corrupted_blob)
        self._transform_worker.finished.connect(self._on_transform_finished)
        self._transform_worker.cancelled.connect(self._on_transform_cancelled)
        self._transform_worker.paused_signal.connect(self._on_worker_paused)
        self._transform_worker.resumed_signal.connect(self._on_worker_resumed)
        self._transform_worker.workers_scaled.connect(self._on_workers_scaled)
        self._transform_worker.log_message.connect(self._log_info)
        self._transform_worker.throughput_update.connect(
            self._resources_panel.update_worker_throughput
        )
        self._transform_worker.bandwidth_cap_changed.connect(self._on_bandwidth_cap_changed)
        self._transform_worker.start()

    # ------------------------------------------------------------------
    # Worker signal slots
    # ------------------------------------------------------------------

    def _on_listing_complete(self, sizes: object) -> None:
        """Improvement 3: store blob sizes from listing for retry batches."""
        if isinstance(sizes, dict):
            self._blob_sizes = sizes
            count = len(sizes)
            self._total_files_in_run += count
            self._progress.setMaximum(max(count, 1))
            self._log_info(f"Listing complete: {count} file(s) found.")

    def _on_cancel(self) -> None:
        if self._transform_worker and self._transform_worker.isRunning():
            self._cancel_btn.setEnabled(False)
            self._pause_btn.setEnabled(False)
            # Clear pending batch so no more prefixes start after cancel
            self._pending_prefixes = []
            self._log_info("Cancelling after current file...")
            self._transform_worker.cancel()

    def _on_pause_resume(self) -> None:
        """Feature 13: toggle pause / resume on the running worker."""
        if not self._transform_worker or not self._transform_worker.isRunning():
            return
        if self._is_paused:
            self._transform_worker.resume()
        else:
            self._transform_worker.pause()

    def _on_worker_paused(self) -> None:
        self._is_paused = True
        self._pause_btn.setText("Resume")
        self._status_label.setText("Paused — press Resume to continue")

    def _on_worker_resumed(self) -> None:
        self._is_paused = False
        self._pause_btn.setText("Pause")

    def _on_transform_progress(
        self,
        completed: int,
        total: int,
        blob_name: str,
        duration_ms: float,
        worker_id: int,
        skipped: bool,
        note: str,
        size_bytes: int,
    ) -> None:
        self._progress.setMaximum(total)
        self._progress.setValue(completed)
        self._files_completed_in_run += 1
        if size_bytes > 0:
            self._bytes_processed += size_bytes

        # Feature 4: throughput + ETA
        elapsed = perf_counter() - self._run_start_time
        parts: list[str] = []
        if elapsed > 1.0 and self._bytes_processed > 0:
            mb_per_s = (self._bytes_processed / elapsed) / (1024 * 1024)
            parts.append(f"{mb_per_s:.1f} MB/s")
        if self._files_completed_in_run > 0 and self._total_files_in_run > 0:
            remaining = self._total_files_in_run - self._files_completed_in_run
            if remaining > 0:
                s_per_file = elapsed / self._files_completed_in_run
                eta_s = s_per_file * remaining
                finish_time = datetime.now() + timedelta(seconds=int(eta_s))
                if finish_time.date() == datetime.now().date():
                    finish_str = finish_time.strftime("%H:%M")
                else:
                    finish_str = finish_time.strftime("%d.%m. %H:%M")
                parts.append(f"ETA {_format_duration(eta_s)} (done ~{finish_str})")
        self._throughput_label.setText("  " + "  ·  ".join(parts) if parts else "")

        self._status_label.setText(f"{completed} / {total} files")

        width = len(str(total))
        current_str = str(completed).zfill(width)
        total_str   = str(total).zfill(width)
        seconds = duration_ms / 1000.0
        size_str = f" | {_format_bytes(size_bytes)}" if size_bytes > 0 else ""
        base = (
            f"Worker #{worker_id} | {current_str}/{total_str} | "
            f"{blob_name} | {seconds:.2f}s{size_str}"
        )
        if skipped:
            reason = note or "already in target schema"
            self._log_info(f"{base} | SKIPPED ({reason})")
        else:
            self._log_info(base)

    def _on_file_error(self, blob_name: str, tb: str) -> None:
        self._log_error(f"  FAILED: {blob_name}\n{tb.strip()}")

    def _on_corrupted_blob(self, blob_name: str, reason: str, size_bytes: int) -> None:
        self._corrupted_blobs.append((blob_name, reason, size_bytes))
        size_str = f" ({_format_bytes(size_bytes)})" if size_bytes > 0 else ""
        self._append_log(
            f"  CORRUPTED{size_str}: {blob_name}\n  {reason}",
            color=_COLOR_CORRUPT,
        )

    def _on_transform_finished(
        self,
        processed: int,
        failed_network: int,
        failed_corrupt: int,
        total_seconds: float,
        avg_seconds: float,
        retriable_failed_names: list,
    ) -> None:
        total_failed = failed_network + failed_corrupt
        msg = (
            f"Done in {_format_duration(total_seconds)} (avg {avg_seconds:.2f}s/file). "
            f"{processed} succeeded"
        )
        if total_failed:
            parts = []
            if failed_network:
                parts.append(f"{failed_network} network error(s)")
            if failed_corrupt:
                parts.append(f"{failed_corrupt} corrupted")
            msg += f", {total_failed} failed ({', '.join(parts)})."
        else:
            msg += "."
        self._log_info(msg)

        self._write_unhealthy_summary()

        # Improvement 9: unlimited auto-retry depth for network failures
        if failed_network > 0:
            self._retry_depth += 1
            self._log_info(
                f"Auto-retry #{self._retry_depth} queued for "
                f"{failed_network} network failure(s)..."
            )
            # Auto-retry: explicit blob list, no checkpoint — progress within this
            # retry batch is not checkpointed. See design doc for rationale.
            self._start_transform(
                blob_names=list(retriable_failed_names),
                blob_sizes=self._blob_sizes or None,
                prefix_override=self._active_prefix,
            )
            return

        # No network failures — advance to next prefix (or finish)
        self._retry_depth = 0
        self._current_prefix_index += 1

        if self._current_prefix_index < len(self._pending_prefixes):
            # More prefixes to process — stay in processing state
            self._start_next_prefix()
        else:
            # All done
            self._resources_panel.clear_worker_throughput()
            self._status_label.setText(msg)
            self._set_schema_loaded_state()

    def _write_unhealthy_summary(self) -> None:
        """Print a consolidated unhealthy-blob report to the log."""
        total_unhealthy = len(self._unknown_size_blobs) + len(self._corrupted_blobs)
        if total_unhealthy == 0:
            return

        self._append_log(
            f"\n── Unhealthy blobs total: {total_unhealthy} ──",
            color=_COLOR_WARN,
        )

        if self._unknown_size_blobs:
            self._append_log(
                f"  Unknown size ({len(self._unknown_size_blobs)}) "
                f"— reported at schema load; processing was still attempted.",
                color=_COLOR_WARN,
            )

        if self._corrupted_blobs:
            corrupt_total = sum(s for _, _, s in self._corrupted_blobs)
            corrupt_size_str = f" · {_format_bytes(corrupt_total)}" if corrupt_total > 0 else ""
            self._append_log(
                f"  Corrupted / unprocessable "
                f"({len(self._corrupted_blobs)}{corrupt_size_str}):",
                color=_COLOR_CORRUPT,
            )
            for name, reason, size_bytes in self._corrupted_blobs:
                size_str = f" [{_format_bytes(size_bytes)}]" if size_bytes > 0 else ""
                self._append_log(f"    • {name}{size_str}", color=_COLOR_CORRUPT)
                self._append_log(f"      Reason: {reason}", color=_COLOR_CORRUPT)

        self._append_log("── End of unhealthy report ──\n", color=_COLOR_WARN)

    def _on_transform_cancelled(self) -> None:
        self._retry_depth = 0
        self._pending_prefixes = []
        self._resources_panel.clear_worker_throughput()
        self._set_schema_loaded_state()
        self._write_unhealthy_summary()
        self._log_info("Cancelled by user.")
        self._status_label.setText("Cancelled")

    def _on_retry_batch_started(self, attempt: int, remaining: int, delay: int) -> None:
        self._log_info(
            f"Retry pass {attempt}/{self._attempts_spin.value()} — "
            f"{remaining} file(s), waiting {delay}s (exponential back-off)..."
        )

    def _on_workers_scaled(self, new_count: int, old_count: int, direction: str, reason: str) -> None:
        """Called when the AdaptiveScaler adjusts the worker count."""
        self._worker_spin.setValue(new_count)
        arrow = "↑" if direction == "up" else "↓"
        self._log_info(
            f"[Autoscale {arrow}] {old_count}→{new_count} workers — {reason}"
        )

    def _on_bandwidth_cap_changed(self, bw_cap: int) -> None:
        """Update the max-workers label with both the live bandwidth cap and the RAM cap."""
        self._max_workers_label.setText(f"/ {bw_cap} bw · {MAX_WORKERS} ram")

    def _on_workers_reduced(self, new_count: int, conn_errors: int) -> None:
        self._worker_spin.setValue(new_count)
        self._log_info(
            f"Workers reduced to {new_count} "
            f"({conn_errors} connection error(s) in this pass)."
        )

    # ------------------------------------------------------------------
    # Log helpers
    # ------------------------------------------------------------------

    def _log_info(self, text: str) -> None:
        self._append_log(text, color=None)

    def _log_error(self, text: str) -> None:
        self._append_log(text, color=_COLOR_ERROR)

    def _append_log(self, text: str, color: QColor | None) -> None:
        self._log_entries.append((text, color))

        # Overflow guard: flush to file and clear widget — never rebuild
        if len(self._log_entries) >= MAX_LOG_ENTRIES:
            self._flush_log_to_file()
            return

        # Append-only fast path
        if self._entry_matches_filter(text):
            self._write_to_widget(text, color)

    def _entry_matches_filter(self, text: str) -> bool:
        if not self._log_filter:
            return True
        return self._log_filter.lower() in text.lower()

    def _flush_log_to_file(self) -> None:
        """Write all in-memory log entries to a timestamped file, then clear."""
        self._log_flush_count += 1
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"parquet_transformer_log_{timestamp}_{self._log_flush_count}.txt"
        log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        path = os.path.join(log_dir, filename)
        try:
            with open(path, "w", encoding="utf-8") as f:
                for entry_text, _ in self._log_entries:
                    f.write(entry_text + "\n")
        except Exception:
            pass  # best-effort; don't block the GUI for a flush failure
        self._log_entries.clear()
        self._log.clear()
        self._write_to_widget(
            f"[Log flushed to {path} ({MAX_LOG_ENTRIES} entries) — continuing]",
            _COLOR_WARN,
        )

    def _rebuild_log_view(self) -> None:
        """Repopulate the log widget from _log_entries with the current filter.

        Protected with setUpdatesEnabled to avoid per-line repaints.
        Only called from filter changes — never during processing overflow.
        """
        self._log.setUpdatesEnabled(False)
        try:
            self._log.clear()
            for text, color in self._log_entries:
                if self._entry_matches_filter(text):
                    self._write_to_widget(text, color)
        finally:
            self._log.setUpdatesEnabled(True)

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

    def _on_log_filter_changed(self, text: str) -> None:
        self._log_filter = text.strip()
        self._rebuild_log_view()

    def _set_log_filter(self, text: str) -> None:
        """Set the filter text programmatically (also updates the QLineEdit)."""
        self._log_filter_edit.setText(text)  # triggers _on_log_filter_changed

    # ------------------------------------------------------------------
    # Export actions
    # ------------------------------------------------------------------

    def _on_export_log(self) -> None:
        """Feature 2: Export all log entries to a plain-text file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Log",
            f"parquet_transformer_log_{timestamp}.txt",
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

    def _on_export_unhealthy_csv(self) -> None:
        """Feature 3: Export corrupted / unknown-size blobs to a CSV report."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Unhealthy Blob Report",
            f"unhealthy_blobs_{timestamp}.csv",
            "CSV Files (*.csv);;All Files (*)",
        )
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    ["blob_name", "issue_type", "reason", "size_bytes", "size_human"]
                )
                for name in self._unknown_size_blobs:
                    writer.writerow([name, "unknown_size", "", "", "? B"])
                for name, reason, size_bytes in self._corrupted_blobs:
                    writer.writerow(
                        [name, "corrupted", reason, size_bytes, _format_bytes(size_bytes)]
                    )
            total = len(self._unknown_size_blobs) + len(self._corrupted_blobs)
            self._log_info(
                f"Unhealthy blob report ({total} entries) exported to: {path}"
            )
        except Exception as exc:
            self._log_error(f"Failed to export unhealthy CSV: {exc}")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def closeEvent(self, event) -> None:  # type: ignore[override]
        """Stop background threads before the window closes."""
        # Cancel any in-progress Data Collector run and wait for it to exit.
        # Qt does not propagate closeEvent to child widgets, so we do it explicitly.
        self._collector_panel.closeEvent(event)
        if not event.isAccepted():
            return
        self._sys_monitor.stop()
        self._sys_monitor.wait(2000)   # give it up to 2 s to exit cleanly
        super().closeEvent(event)

    # ------------------------------------------------------------------
    # Misc helpers
    # ------------------------------------------------------------------

    def _default_worker_count(self) -> int:
        """Return a sensible default for I/O-bound workloads.
        CPU count is inappropriate here — use a fixed IO-appropriate default.
        """
        return 8
