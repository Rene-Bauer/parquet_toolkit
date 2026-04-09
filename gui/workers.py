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
from dataclasses import dataclass
from time import perf_counter
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from PyQt6.QtCore import QThread, pyqtSignal

from parquet_transform.checkpoint import FailedList, RunCheckpoint
from parquet_transform.processor import ColumnConfig, apply_transforms, compute_output_name
from parquet_transform.storage import BlobStorageClient

try:
    from azure.core.exceptions import (
        AzureError,
        ServiceRequestError,
        ServiceResponseError,
    )
except ImportError:  # pragma: no cover
    AzureError = ServiceRequestError = ServiceResponseError = tuple()

try:
    import pyarrow.lib as _pa_lib
    _ARROW_EXCEPTIONS: tuple[type, ...] = (_pa_lib.ArrowInvalid, _pa_lib.ArrowException)
except (ImportError, AttributeError):
    _ARROW_EXCEPTIONS = ()


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

DOWNLOAD_TIMEOUT_S: int = 120
"""Seconds before an Azure download is considered hung and aborted."""

UPLOAD_TIMEOUT_S: int = 300
"""Seconds before an Azure upload is considered hung and aborted."""

BACKOFF_MAX_S: int = 30
"""Upper bound for exponential back-off delay between retry passes (seconds)."""

CONNECTION_ERROR_THROTTLE_RATIO: float = 0.1
"""If this fraction of files in a pass fail with connection errors, halve the
worker count.  0.1 = trigger at 10 %+ error rate."""

UPLOAD_BANDWIDTH_FALLBACK_KBYTES_S: int = 350
"""Fallback bandwidth assumption (KB/s) used when calibration has no large-file
data to measure from (e.g. all files in the calibration batch were tiny).
350 KB/s is a conservative estimate for a typical business connection to Azure."""

_CALIB_BATCH_SIZE: int = 4
"""Number of files processed in the calibration pass before scaling workers."""

_CALIB_LARGE_FILE_THRESHOLD_MB: int = 8
"""Only trigger the calibration phase when the p95 file size exceeds this value.
Below this threshold the uploads are fast enough that timeouts are unlikely even
with many workers, so calibration overhead is not worth the cost."""


_EXPECTED_OUTPUT_TYPES: dict[str, pa.DataType] = {
    "binary16_to_uuid": pa.string(),
    "timestamp_ns_to_ms_utc": pa.timestamp("ms", tz="UTC"),
}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _format_bytes(n: int) -> str:
    if n < 0:
        return "? B"
    value = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} PB"


@dataclass
class _FileResult:
    status: str
    duration_ms: float
    error: str | None = None
    skip_reason: str | None = None
    short_error: str | None = None
    suppress_trace: bool = False
    retriable: bool = True
    upload_bytes: int = 0       # > 0 only on successful upload (for calibration)
    upload_seconds: float = 0.0 # wall-clock seconds for the upload step only


def _first_line(text: str) -> str:
    if not text:
        return "Unknown error"
    stripped = text.strip()
    return stripped.splitlines()[0] if stripped else "Unknown error"


def _summarize_exception(exc: BaseException) -> tuple[str, bool, bool]:
    message = str(exc).strip() or exc.__class__.__name__
    if _ARROW_EXCEPTIONS and isinstance(exc, _ARROW_EXCEPTIONS):
        return (f"Corrupted Parquet file: {message}", False, False)
    if isinstance(exc, (ServiceRequestError, ServiceResponseError)):
        return (f"Azure blob request failed: {message}", True, True)
    if isinstance(exc, AzureError):
        return (f"Azure error: {message}", True, True)
    if isinstance(exc, TimeoutError):
        return (f"Timeout: {message}", True, True)
    if isinstance(exc, ConnectionError):
        return (f"Connection error: {message}", True, True)
    return (f"{exc.__class__.__name__}: {message}", False, False)


class SchemaLoaderWorker(QThread):
    schema_loaded = pyqtSignal(object, int, object, object)
    error = pyqtSignal(str)

    def __init__(self, connection_string: str, container: str, prefix: str, parent=None):
        super().__init__(parent)
        self._connection_string = connection_string
        self._container = container
        self._prefix = prefix

    def run(self) -> None:
        try:
            client = BlobStorageClient(self._connection_string, self._container)
            blobs = client.list_blobs_with_sizes(self._prefix)
            client.close()
            if not blobs:
                self.error.emit(f"No .parquet files found under prefix '{self._prefix}'.")
                return
            blob_names = [name for name, _ in blobs]
            known   = [(n, s) for n, s in blobs if s >= 0]
            unknown = [n      for n, s in blobs if s <  0]
            total_bytes = sum(s for _, s in known)
            schema = client.read_schema(blob_names[0])
            self.schema_loaded.emit(schema, len(blobs), total_bytes, unknown)
        except Exception as exc:
            short_error, _, _ = _summarize_exception(exc)
            self.error.emit(short_error)


class TransformWorker(QThread):
    """
    Processes all Parquet blobs: download -> transform -> upload.

    Adaptive concurrency
    --------------------
    When the p95 file size exceeds _CALIB_LARGE_FILE_THRESHOLD_MB, the worker
    automatically runs a short calibration pass (first _CALIB_BATCH_SIZE files
    with 2 threads) and measures real upload throughput.  From that measurement
    it derives how many threads can upload concurrently without starving each
    other's TCP connections and hitting OS-level write timeouts.  The thread
    pool for the main run is then set to that calibrated value — so the thread
    count scales automatically with the available bandwidth:

        max_workers = (measured_total_bw × UPLOAD_TIMEOUT_S / p95_bytes) × 0.7

    Examples:
        300 KB/s  → ~3 workers   (home connection, 23 MB files)
        1   MB/s  → ~9 workers   (office)
        5   MB/s  → 32 workers   (datacenter / fast link)
    """

    progress = pyqtSignal(int, int, str, float, int, bool, str, int)
    listing_complete = pyqtSignal(object)
    retry_batch_started = pyqtSignal(int, int, int)
    workers_reduced = pyqtSignal(int, int)
    file_error = pyqtSignal(str, str)
    corrupted_blob = pyqtSignal(str, str, int)
    finished = pyqtSignal(int, int, int, float, float, list)
    cancelled = pyqtSignal()
    paused_signal = pyqtSignal()
    resumed_signal = pyqtSignal()
    # Emitted after calibration: (calibrated_worker_count, measured_bandwidth_kbs)
    workers_calibrated = pyqtSignal(int, int)

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
        blob_sizes: dict[str, int] | None = None,
        autoscale: bool = False,
        checkpoint: RunCheckpoint | None = None,
        failed_list: FailedList | None = None,
        retry_failed: bool = False,
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
        self._blob_sizes = blob_sizes
        self._autoscale = autoscale
        self._checkpoint = checkpoint
        self._failed_list = failed_list
        self._retry_failed = retry_failed
        self._cancel_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()

    def cancel(self) -> None:
        self._cancel_event.set()
        self._pause_event.set()

    def pause(self) -> None:
        self._pause_event.clear()
        self.paused_signal.emit()

    def resume(self) -> None:
        self._pause_event.set()
        self.resumed_signal.emit()

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        blob_sizes: dict[str, int] = {}

        if self._blob_names is not None:
            blob_names = list(self._blob_names)
            if self._blob_sizes is not None:
                blob_sizes = dict(self._blob_sizes)
        else:
            try:
                listing_client = BlobStorageClient(self._connection_string, self._container)
                blobs = listing_client.list_blobs_with_sizes(self._prefix)
                listing_client.close()
                blob_names = [name for name, _ in blobs]
                blob_sizes = {name: size for name, size in blobs if size >= 0}
                self.listing_complete.emit(dict(blob_sizes))
            except Exception:
                self.file_error.emit("(connection)", traceback.format_exc())
                self.finished.emit(0, 1, 0, 0.0, 0.0, [])
                return

        # Checkpoint: skip already-processed blobs on resume
        if self._checkpoint and blob_names:
            before = len(blob_names)
            blob_names = [b for b in blob_names if not self._checkpoint.should_skip(b)]
            skipped = before - len(blob_names)
            if skipped > 0:
                self.file_error.emit(
                    "(checkpoint)",
                    f"[Checkpoint] Resuming — skipping {skipped} already-processed file(s)"
                )

        # Failed list: prepend previously failed blobs for retry (deduplicated)
        if self._retry_failed and self._failed_list:
            failed_names = self._failed_list.blob_names()
            existing = set(blob_names)
            extra = [n for n in failed_names if n not in existing]
            blob_names = extra + blob_names
            if extra:
                type_map = {e["name"]: e["type"] for e in self._failed_list.entries}
                for name in extra:
                    entry_type = type_map.get(name, "unknown")
                    self.file_error.emit(
                        "(checkpoint)",
                        f"[Failed List] Retrying previously failed file: {name} ({entry_type})"
                    )

        total = len(blob_names)
        if total == 0:
            self.finished.emit(0, 0, 0, 0.0, 0.0, [])
            return

        # ------------------------------------------------------------------
        # Determine p95 file size to decide if calibration is needed
        # ------------------------------------------------------------------
        p95_bytes = 0
        if blob_sizes:
            sorted_sizes = sorted(s for s in blob_sizes.values() if s > 0)
            if sorted_sizes:
                p95_idx = max(0, int(len(sorted_sizes) * 0.95) - 1)
                p95_bytes = sorted_sizes[p95_idx]

        needs_calibration = (
            self._autoscale
            and p95_bytes >= _CALIB_LARGE_FILE_THRESHOLD_MB * 1024 * 1024
            and not self._dry_run
            and total >= _CALIB_BATCH_SIZE
            and self._worker_count > 2
        )

        # ------------------------------------------------------------------
        # Shared counters (used by both calibration and main pass)
        # ------------------------------------------------------------------
        stats_lock = threading.Lock()
        processed       = 0
        failed_network  = 0
        failed_corrupt  = 0
        completed       = 0
        total_duration_ms = 0.0
        start_time = perf_counter()
        attempt_counter: dict[str, int] = {}
        duration_tracker: dict[str, float] = {}
        retriable_failed_names: list[str] = []

        # Upload timing for calibration (bytes, upload_seconds per successful upload)
        upload_measurements: list[tuple[int, float]] = []
        upload_meas_lock = threading.Lock()

        def finalize_success(*, blob_name, worker_id, attempts, duration_ms,
                             skipped, note, size_bytes):
            nonlocal processed, completed, total_duration_ms
            with stats_lock:
                processed += 1
                completed += 1
                completed_so_far = completed
                total_duration_ms += duration_ms
                if self._checkpoint:
                    self._checkpoint.advance_cursor(blob_name)
                if self._failed_list:
                    self._failed_list.remove(blob_name)
            self.progress.emit(completed_so_far, total, blob_name,
                               duration_ms, worker_id, skipped, note, size_bytes)

        def finalize_failure(*, blob_name, worker_id, attempts, duration_ms,
                             error, short_error, suppress_trace, retriable, size_bytes):
            nonlocal failed_network, failed_corrupt, completed, total_duration_ms
            with stats_lock:
                if retriable:
                    failed_network += 1
                    retriable_failed_names.append(blob_name)
                else:
                    failed_corrupt += 1
                completed += 1
                completed_so_far = completed
                total_duration_ms += duration_ms
                if self._failed_list:
                    ftype = "network" if retriable else "corrupt"
                    self._failed_list.add_or_update(blob_name, ftype, short_error)
                    self.file_error.emit(
                        "(checkpoint)",
                        f"[Failed List] Recorded {ftype} failure: {blob_name}"
                    )
            if retriable:
                self._log_final_failure(blob_name, attempts, short_error, error, suppress_trace)
            self.progress.emit(completed_so_far, total, blob_name,
                               duration_ms, worker_id, False, "", size_bytes)

        def run_pass(blob_batch: list[str], pass_number: int) -> tuple[list[str], int]:
            if not blob_batch or self._cancel_event.is_set():
                return [], 0

            worker_total = min(self._worker_count, len(blob_batch))
            task_queue: queue.Queue[Any] = queue.Queue()
            sentinel = object()
            for name in blob_batch:
                task_queue.put(name)
            for _ in range(worker_total):
                task_queue.put(sentinel)

            next_round: list[str] = []
            next_round_lock = threading.Lock()
            conn_error_count = 0
            conn_error_lock  = threading.Lock()

            def worker_loop(worker_id: int) -> None:
                nonlocal conn_error_count
                worker_client = BlobStorageClient(self._connection_string, self._container)
                while True:
                    item = task_queue.get()
                    if item is sentinel:
                        task_queue.task_done()
                        break

                    blob_name = str(item)
                    self._pause_event.wait()
                    if self._cancel_event.is_set():
                        task_queue.task_done()
                        continue

                    size_bytes = blob_sizes.get(blob_name, 0)
                    attempt_counter[blob_name] = attempt_counter.get(blob_name, 0) + 1
                    result = self._process_blob_once(worker_client, blob_name)
                    duration_tracker[blob_name] = (
                        duration_tracker.get(blob_name, 0.0) + result.duration_ms
                    )
                    aggregated_duration = duration_tracker[blob_name]

                    # Collect upload timing for calibration
                    if result.upload_bytes > 0 and result.upload_seconds > 0:
                        with upload_meas_lock:
                            upload_measurements.append(
                                (result.upload_bytes, result.upload_seconds)
                            )

                    if result.status in {"success", "skipped"}:
                        finalize_success(
                            blob_name=blob_name, worker_id=worker_id,
                            attempts=attempt_counter[blob_name],
                            duration_ms=aggregated_duration,
                            skipped=result.status == "skipped",
                            note=result.skip_reason or "", size_bytes=size_bytes,
                        )
                        duration_tracker.pop(blob_name, None)
                    else:
                        error_text = result.error or "Unknown error"
                        short_msg  = result.short_error or _first_line(error_text)

                        if result.suppress_trace:
                            with conn_error_lock:
                                conn_error_count += 1

                        if not result.retriable:
                            self.corrupted_blob.emit(blob_name, short_msg, size_bytes)
                            finalize_failure(
                                blob_name=blob_name, worker_id=worker_id,
                                attempts=attempt_counter[blob_name],
                                duration_ms=aggregated_duration, error=error_text,
                                short_error=short_msg, suppress_trace=result.suppress_trace,
                                retriable=False, size_bytes=size_bytes,
                            )
                            duration_tracker.pop(blob_name, None)
                        elif pass_number < self._max_attempts:
                            with next_round_lock:
                                next_round.append(blob_name)
                            self._log_retry(blob_name, attempt_counter[blob_name],
                                            short_msg,
                                            None if result.suppress_trace else error_text)
                        else:
                            finalize_failure(
                                blob_name=blob_name, worker_id=worker_id,
                                attempts=attempt_counter[blob_name],
                                duration_ms=aggregated_duration, error=error_text,
                                short_error=short_msg, suppress_trace=result.suppress_trace,
                                retriable=True, size_bytes=size_bytes,
                            )
                            duration_tracker.pop(blob_name, None)

                    task_queue.task_done()

            threads = [
                threading.Thread(target=worker_loop, args=(i + 1,), daemon=True)
                for i in range(worker_total)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            return next_round, conn_error_count

        # ------------------------------------------------------------------
        # Calibration pass (only for large files, skipped on retry batches)
        # ------------------------------------------------------------------
        remaining = list(blob_names)

        if needs_calibration:
            target_worker_count = self._worker_count
            self._worker_count = 2  # conservative start

            calib_batch = remaining[:_CALIB_BATCH_SIZE]
            remaining   = remaining[_CALIB_BATCH_SIZE:]

            # Pass number 0 = calibration; failures go back into remaining
            calib_failed, _ = run_pass(calib_batch, pass_number=0)
            remaining = calib_failed + remaining

            # Compute calibrated worker count from measured upload times.
            #
            # With C concurrent workers each uploading simultaneously, the
            # observed per-connection throughput is:
            #   per_conn_bw = total_bytes / total_connection_seconds
            #
            # Total link bandwidth ≈ per_conn_bw × C (TCP fair-share assumption).
            # Maximum safe workers = total_bw × UPLOAD_TIMEOUT_S / p95_bytes × safety.
            new_worker_count = target_worker_count  # default: no change
            measured_bw_kbs  = 0

            with upload_meas_lock:
                measurements = list(upload_measurements)

            if measurements:
                total_upload_bytes = sum(b for b, _ in measurements)
                total_conn_secs    = sum(t for _, t in measurements)
                if total_conn_secs > 0:
                    per_conn_bw = total_upload_bytes / total_conn_secs  # bytes/s per slot
                    total_bw    = per_conn_bw * 2  # calibrated with 2 workers
                    measured_bw_kbs = int(total_bw / 1024)

                    raw = total_bw * UPLOAD_TIMEOUT_S / p95_bytes * 0.7
                    new_worker_count = max(2, min(target_worker_count, int(raw)))

            self._worker_count = new_worker_count
            self.workers_calibrated.emit(new_worker_count, measured_bw_kbs)

        # ------------------------------------------------------------------
        # Multi-pass retry loop with exponential back-off
        # ------------------------------------------------------------------
        pass_number = 1
        while (
            remaining
            and pass_number <= self._max_attempts
            and not self._cancel_event.is_set()
        ):
            if pass_number > 1:
                delay = min(2 ** (pass_number - 1), BACKOFF_MAX_S)
                self.retry_batch_started.emit(pass_number, len(remaining), delay)
                if self._cancel_event.wait(timeout=float(delay)):
                    break

            next_round, conn_errors = run_pass(remaining, pass_number)

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

        if self._checkpoint:
            self._checkpoint.mark_complete()
            self.file_error.emit("(checkpoint)", "[Checkpoint] Run marked as complete")

        total_seconds = perf_counter() - start_time
        avg_seconds = (total_duration_ms / max(completed, 1)) / 1000.0 if completed else 0.0
        self.finished.emit(
            processed, failed_network, failed_corrupt,
            total_seconds, avg_seconds, retriable_failed_names,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _log_retry(self, blob_name, attempt, short_error, full_error) -> None:
        message = f"Attempt {attempt} failed (will retry): {short_error.strip()}"
        if full_error:
            message += f"\n{full_error.strip()}"
        self.file_error.emit(blob_name, message)

    def _log_final_failure(self, blob_name, attempts, short_error, full_error,
                           suppress_trace=False) -> None:
        message = f"Final failure after {attempts} attempt(s): {short_error.strip()}"
        if full_error and not suppress_trace:
            message += f"\n{full_error.strip()}"
        self.file_error.emit(blob_name, message)

    def _process_blob_once(self, client: BlobStorageClient, blob_name: str) -> _FileResult:
        """Download, optionally transform, and upload a single blob once."""
        file_start = perf_counter()
        try:
            raw = client.download_bytes(blob_name, timeout=DOWNLOAD_TIMEOUT_S)
            buf = io.BytesIO(raw)
            table = pq.read_table(buf)

            if self._should_skip_table(table):
                return _FileResult(
                    status="skipped",
                    duration_ms=(perf_counter() - file_start) * 1000.0,
                    skip_reason="already in target schema",
                )

            table = apply_transforms(table, self._col_configs)
            output_name = compute_output_name(
                blob_name, self._prefix, self._output_prefix
            )

            upload_bytes  = 0
            upload_seconds = 0.0
            if not self._dry_run:
                out_buf = io.BytesIO()
                pq.write_table(table, out_buf)
                upload_data = out_buf.getvalue()
                t_upload = perf_counter()
                client.upload_bytes(output_name, upload_data,
                                    overwrite=True, timeout=UPLOAD_TIMEOUT_S)
                upload_seconds = perf_counter() - t_upload
                upload_bytes   = len(upload_data)

            return _FileResult(
                status="success",
                duration_ms=(perf_counter() - file_start) * 1000.0,
                upload_bytes=upload_bytes,
                upload_seconds=upload_seconds,
            )
        except Exception as exc:
            short_error, suppress_trace, retriable = _summarize_exception(exc)
            return _FileResult(
                status="error",
                duration_ms=(perf_counter() - file_start) * 1000.0,
                error=traceback.format_exc(),
                short_error=short_error,
                suppress_trace=suppress_trace,
                retriable=retriable,
            )

    def _should_skip_table(self, table: pa.Table) -> bool:
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
                field = schema.field(cfg.name)
            except KeyError:
                return False
            if field.type != expected:
                return False
        return True
