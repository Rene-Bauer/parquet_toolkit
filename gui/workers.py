"""
Background QThread workers for long-running Azure + Parquet operations.

All heavy I/O runs off the main thread so the UI stays responsive.
Workers communicate back to the main thread via Qt signals.
"""
from __future__ import annotations

import io
import queue
import threading
import traceback
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from PyQt6.QtCore import QThread, pyqtSignal

from parquet_transform.processor import ColumnConfig, apply_transforms, compute_output_name
from parquet_transform.storage import BlobStorageClient

try:
    from azure.core.exceptions import (
        AzureError,
        ServiceRequestError,
        ServiceResponseError,
    )
except ImportError:  # pragma: no cover - azure is always installed in production, but tests may skip it.
    AzureError = ServiceRequestError = ServiceResponseError = tuple()


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

BLOB_OPERATION_TIMEOUT_S: int = 60
"""Seconds before a single Azure download or upload is considered hung and aborted."""

BACKOFF_MAX_S: int = 30
"""Upper bound for exponential back-off delay between retry passes (seconds)."""

CONNECTION_ERROR_THROTTLE_RATIO: float = 0.5
"""If this fraction of files in a pass fail with connection errors, halve the worker count."""


_EXPECTED_OUTPUT_TYPES: dict[str, pa.DataType] = {
    # Known target schemas per transform; used to short-circuit work when files are already compliant.
    "binary16_to_uuid": pa.string(),
    "timestamp_ns_to_ms_utc": pa.timestamp("ms", tz="UTC"),
}


@dataclass
class _FileResult:
    status: str  # "success", "skipped", "error"
    duration_ms: float
    error: str | None = None
    skip_reason: str | None = None
    short_error: str | None = None
    suppress_trace: bool = False


def _first_line(text: str) -> str:
    if not text:
        return "Unknown error"
    stripped = text.strip()
    return stripped.splitlines()[0] if stripped else "Unknown error"


def _summarize_exception(exc: BaseException) -> tuple[str, bool]:
    """
    Return a short, user-friendly error plus whether the traceback should be suppressed.
    """
    message = str(exc).strip() or exc.__class__.__name__

    if isinstance(exc, (ServiceRequestError, ServiceResponseError)):
        return (f"Azure blob request failed: {message}", True)
    if isinstance(exc, AzureError):
        return (f"Azure error: {message}", True)
    if isinstance(exc, TimeoutError):
        return (f"Timeout: {message}", True)
    if isinstance(exc, ConnectionError):
        return (f"Connection error: {message}", True)

    return (f"{exc.__class__.__name__}: {message}", False)


class SchemaLoaderWorker(QThread):
    """
    Connects to Azure, counts Parquet blobs, and reads the schema from
    the first file (footer only - fast).
    """

    schema_loaded = pyqtSignal(object, int)  # (pa.Schema, file_count)
    error = pyqtSignal(str)

    def __init__(
        self,
        connection_string: str,
        container: str,
        prefix: str,
        parent=None,
    ):
        super().__init__(parent)
        self._connection_string = connection_string
        self._container = container
        self._prefix = prefix

    def run(self) -> None:
        try:
            client = BlobStorageClient(self._connection_string, self._container)
            blob_names = client.list_blobs(self._prefix)
            if not blob_names:
                self.error.emit(
                    f"No .parquet files found under prefix '{self._prefix}'."
                )
                return
            schema = client.read_schema(blob_names[0])
            self.schema_loaded.emit(schema, len(blob_names))
        except Exception as exc:
            short_error, _ = _summarize_exception(exc)
            self.error.emit(short_error)


class TransformWorker(QThread):
    """
    Processes all Parquet blobs: download -> transform -> upload.
    Supports cancellation, dry-run mode, and multi-threaded execution.
    """

    # (completed_files, total_files, blob_name, duration_ms, worker_id, skipped, note)
    progress = pyqtSignal(int, int, str, float, int, bool, str)
    # (attempt_number, remaining_files, backoff_delay_seconds)
    retry_batch_started = pyqtSignal(int, int, int)
    # (new_worker_count, connection_error_count)
    workers_reduced = pyqtSignal(int, int)
    # (blob_name, error_message)
    file_error = pyqtSignal(str, str)
    # (files_processed, files_failed, total_seconds, average_seconds, failed_blob_names)
    finished = pyqtSignal(int, int, float, float, list)
    cancelled = pyqtSignal()

    def __init__(
        self,
        connection_string: str,
        container: str,
        prefix: str,
        col_configs: list[ColumnConfig],
        output_prefix: str | None,
        dry_run: bool = False,
        worker_count: int = 1,
        max_attempts: int = 1,
        blob_names: list[str] | None = None,
        parent=None,
    ):
        super().__init__(parent)
        self._connection_string = connection_string
        self._container = container
        self._prefix = prefix
        self._col_configs = col_configs
        self._output_prefix = output_prefix
        self._dry_run = dry_run
        self._worker_count = max(1, worker_count)
        self._max_attempts = max(1, max_attempts)
        self._blob_names = blob_names
        self._cancel_event = threading.Event()
        self._file_stats: list[dict[str, Any]] = []

    def _log_retry(
        self,
        blob_name: str,
        attempt: int,
        short_error: str,
        full_error: str | None,
    ) -> None:
        message = f"Attempt {attempt} failed (will retry): {short_error.strip()}"
        if full_error:
            message += f"\n{full_error.strip()}"
        self.file_error.emit(blob_name, message)

    def _log_final_failure(
        self,
        blob_name: str,
        attempts: int,
        short_error: str,
        full_error: str | None,
        suppress_trace: bool = False,
    ) -> None:
        message = f"Final failure after {attempts} attempt(s): {short_error.strip()}"
        if full_error and not suppress_trace:
            message += f"\n{full_error.strip()}"
        self.file_error.emit(blob_name, message)

    def cancel(self) -> None:
        """Request cancellation after the current file finishes."""
        self._cancel_event.set()

    def run(self) -> None:
        # ------------------------------------------------------------------
        # Resolve blob list (provided directly or fetched from Azure)
        # ------------------------------------------------------------------
        if self._blob_names is not None:
            blob_names = list(self._blob_names)
        else:
            try:
                listing_client = BlobStorageClient(self._connection_string, self._container)
                blob_names = listing_client.list_blobs(self._prefix)
            except Exception:
                self.file_error.emit("(connection)", traceback.format_exc())
                self.finished.emit(0, 1, 0.0, 0.0, [])
                return

        total = len(blob_names)
        if total == 0:
            self.finished.emit(0, 0, 0.0, 0.0, [])
            return

        stats_lock = threading.Lock()
        processed = 0
        failed = 0
        completed = 0
        total_duration_ms = 0.0
        start_time = perf_counter()
        attempt_counter: dict[str, int] = {}
        duration_tracker: dict[str, float] = {}
        failed_blob_names: list[str] = []

        def finalize_success(
            *,
            blob_name: str,
            worker_id: int,
            attempts: int,
            duration_ms: float,
            skipped: bool,
            note: str,
        ) -> None:
            nonlocal processed, completed, total_duration_ms
            with stats_lock:
                processed += 1
                completed += 1
                completed_so_far = completed
                total_duration_ms += duration_ms
                self._file_stats.append(
                    {
                        "blob": blob_name,
                        "duration_ms": duration_ms,
                        "worker_id": worker_id,
                        "attempts": attempts,
                        "success": True,
                        "skipped": skipped,
                    }
                )
            self.progress.emit(
                completed_so_far,
                total,
                blob_name,
                duration_ms,
                worker_id,
                skipped,
                note,
            )

        def finalize_failure(
            *,
            blob_name: str,
            worker_id: int,
            attempts: int,
            duration_ms: float,
            error: str,
            short_error: str,
            suppress_trace: bool,
        ) -> None:
            nonlocal failed, completed, total_duration_ms
            with stats_lock:
                failed += 1
                completed += 1
                completed_so_far = completed
                total_duration_ms += duration_ms
                failed_blob_names.append(blob_name)
                self._file_stats.append(
                    {
                        "blob": blob_name,
                        "duration_ms": duration_ms,
                        "worker_id": worker_id,
                        "attempts": attempts,
                        "success": False,
                        "skipped": False,
                    }
                )
            self._log_final_failure(blob_name, attempts, short_error, error, suppress_trace)
            self.progress.emit(
                completed_so_far,
                total,
                blob_name,
                duration_ms,
                worker_id,
                False,
                "",
            )

        def run_pass(blob_batch: list[str], pass_number: int) -> tuple[list[str], int]:
            """Process one batch; returns (failed_blobs, connection_error_count)."""
            if not blob_batch or self._cancel_event.is_set():
                return [], 0

            worker_total = min(self._worker_count, len(blob_batch))
            task_queue: queue.Queue[Any] = queue.Queue()
            sentinel = object()
            for blob_name in blob_batch:
                task_queue.put(blob_name)
            for _ in range(worker_total):
                task_queue.put(sentinel)

            next_round: list[str] = []
            next_round_lock = threading.Lock()
            conn_error_count = 0
            conn_error_lock = threading.Lock()

            def worker_loop(worker_id: int) -> None:
                nonlocal conn_error_count
                worker_client = BlobStorageClient(
                    self._connection_string, self._container
                )
                while True:
                    item = task_queue.get()
                    if item is sentinel:
                        task_queue.task_done()
                        break

                    blob_name = str(item)

                    if self._cancel_event.is_set():
                        task_queue.task_done()
                        continue

                    attempt_counter[blob_name] = attempt_counter.get(blob_name, 0) + 1
                    result = self._process_blob_once(worker_client, blob_name)
                    duration_tracker[blob_name] = (
                        duration_tracker.get(blob_name, 0.0) + result.duration_ms
                    )
                    aggregated_duration = duration_tracker[blob_name]

                    if result.status in {"success", "skipped"}:
                        skipped = result.status == "skipped"
                        finalize_success(
                            blob_name=blob_name,
                            worker_id=worker_id,
                            attempts=attempt_counter[blob_name],
                            duration_ms=aggregated_duration,
                            skipped=skipped,
                            note=result.skip_reason or "",
                        )
                        duration_tracker.pop(blob_name, None)
                    else:
                        error_text = result.error or "Unknown error"
                        short_msg = result.short_error or _first_line(error_text)
                        if result.suppress_trace:
                            with conn_error_lock:
                                conn_error_count += 1
                        if pass_number < self._max_attempts:
                            with next_round_lock:
                                next_round.append(blob_name)
                            self._log_retry(
                                blob_name,
                                attempt_counter[blob_name],
                                short_msg,
                                None if result.suppress_trace else error_text,
                            )
                        else:
                            finalize_failure(
                                blob_name=blob_name,
                                worker_id=worker_id,
                                attempts=attempt_counter[blob_name],
                                duration_ms=aggregated_duration,
                                error=error_text,
                                short_error=short_msg,
                                suppress_trace=result.suppress_trace,
                            )
                            duration_tracker.pop(blob_name, None)

                    task_queue.task_done()

            threads = [
                threading.Thread(target=worker_loop, args=(i + 1,), daemon=True)
                for i in range(worker_total)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            return next_round, conn_error_count

        # ------------------------------------------------------------------
        # Multi-pass retry loop with exponential back-off
        # ------------------------------------------------------------------
        remaining = list(blob_names)
        pass_number = 1
        while (
            remaining
            and pass_number <= self._max_attempts
            and not self._cancel_event.is_set()
        ):
            if pass_number > 1:
                delay = min(2 ** (pass_number - 1), BACKOFF_MAX_S)
                self.retry_batch_started.emit(pass_number, len(remaining), delay)
                # Interruptible sleep: returns True immediately if cancel is set
                if self._cancel_event.wait(timeout=float(delay)):
                    break

            next_round, conn_errors = run_pass(remaining, pass_number)

            # Throttle workers if the majority of failures were connection errors
            if conn_errors > 0 and conn_errors >= len(remaining) * CONNECTION_ERROR_THROTTLE_RATIO:
                old_count = self._worker_count
                self._worker_count = max(1, self._worker_count // 2)
                if self._worker_count < old_count:
                    self.workers_reduced.emit(self._worker_count, conn_errors)

            remaining = next_round
            pass_number += 1

        if self._cancel_event.is_set():
            self.cancelled.emit()
            return

        total_seconds = perf_counter() - start_time
        avg_seconds = (
            (total_duration_ms / max(completed, 1)) / 1000.0 if completed else 0.0
        )
        self.finished.emit(processed, failed, total_seconds, avg_seconds, failed_blob_names)

    def _process_blob_once(
        self,
        client: BlobStorageClient,
        blob_name: str,
    ) -> _FileResult:
        """Download, optionally transform, and upload a single blob once."""
        file_start = perf_counter()
        try:
            raw = client.download_bytes(blob_name, timeout=BLOB_OPERATION_TIMEOUT_S)
            buf = io.BytesIO(raw)
            table = pq.read_table(buf)

            if self._should_skip_table(table):
                duration_ms = (perf_counter() - file_start) * 1000.0
                return _FileResult(
                    status="skipped",
                    duration_ms=duration_ms,
                    skip_reason="already in target schema",
                )

            table = apply_transforms(table, self._col_configs)
            output_name = compute_output_name(
                blob_name, self._prefix, self._output_prefix
            )

            if not self._dry_run:
                out_buf = io.BytesIO()
                pq.write_table(table, out_buf)
                client.upload_bytes(
                    output_name,
                    out_buf.getvalue(),
                    overwrite=True,
                    timeout=BLOB_OPERATION_TIMEOUT_S,
                )

            duration_ms = (perf_counter() - file_start) * 1000.0
            return _FileResult(status="success", duration_ms=duration_ms)
        except Exception as exc:
            duration_ms = (perf_counter() - file_start) * 1000.0
            short_error, suppress_trace = _summarize_exception(exc)
            return _FileResult(
                status="error",
                duration_ms=duration_ms,
                error=traceback.format_exc(),
                short_error=short_error,
                suppress_trace=suppress_trace,
            )

    def _should_skip_table(self, table: pa.Table) -> bool:
        """
        Return True if every configured transform already matches its target type.
        """
        if not self._col_configs:
            return False

        schema = table.schema
        for cfg in self._col_configs:
            expected = _EXPECTED_OUTPUT_TYPES.get(cfg.transform)
            if expected is None:
                return False
            if cfg.name not in schema.names:
                return False
            try:
                field = schema.field_by_name(cfg.name)
            except KeyError:
                return False
            if field.type != expected:
                return False
        return True
