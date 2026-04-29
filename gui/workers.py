"""
Background QThread workers for long-running Azure + Parquet operations.

All heavy I/O runs off the main thread so the UI stays responsive.
Workers communicate back to the main thread via Qt signals.
"""
from __future__ import annotations

import concurrent.futures
import io
import queue
import threading
import time
import traceback
from dataclasses import dataclass
from time import perf_counter
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from PyQt6.QtCore import Qt, QThread, pyqtSignal

from parquet_transform.checkpoint import SubfolderCheckpoint, FailedList, RunCheckpoint
from parquet_transform.processor import ColumnConfig, apply_transforms, compute_output_name
from parquet_transform.scaler import AdaptiveScaler
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

DOWNLOAD_TIMEOUT_S: int = 200
"""Seconds before an Azure download is considered hung and aborted.

Set to 200 s (previously 120 s) after production logs showed that files
timing out at 120 s consistently succeeded on retry within 200–450 s,
indicating a degraded-but-alive network rather than dead connections.
Raising the limit reduces unnecessary retry-pass churn and false-positive
error signals to the autoscaler without meaningfully delaying dead-connection
detection (the rolling window fills from other parallel workers).
"""

UPLOAD_TIMEOUT_S: int = 300
"""Seconds before an Azure upload is considered hung and aborted."""

BACKOFF_MAX_S: int = 30
"""Upper bound for exponential back-off delay between retry passes (seconds)."""

CONNECTION_ERROR_THROTTLE_RATIO: float = 0.1
"""If this fraction of files in a pass fail with connection errors, halve the
worker count.  0.1 = trigger at 10 %+ error rate."""

SCALER_WINDOW_SIZE: int = 100
"""Rolling window size — number of upload measurements kept."""

SCALER_MIN_SAMPLES: int = 5
"""Minimum measurements collected before any scaling decision is made."""

SCALER_CHECK_INTERVAL: int = 5
"""Number of upload-relevant results (success or error) between scaling checks."""

SCALER_DOWN_ERROR_RATE: float = 0.15
"""Scale-down trigger: error rate threshold (fraction, e.g. 0.15 = 15%)."""

SCALER_DOWN_THROUGHPUT_DROP: float = 0.30
"""Scale-down trigger: recent throughput must be this far below window median."""

SCALER_UP_MIN_HEADROOM: int = 2
"""Scale-up trigger: minimum headroom slots required to consider scaling up."""

SCALER_UP_CONFIRM_CHECKS: int = 2
"""Scale-up trigger: headroom must be stable for this many consecutive checks."""

SCALER_UP_MAX_STEP: int = 8
"""Scale-up: maximum workers added per incremental check."""

SCALER_HOT_ERROR_RATE: float = 0.5
"""Error rate threshold that triggers immediate scale-down (hot spike)."""

SCALER_HOT_ERROR_MIN_SAMPLES: int = 20
"""Minimum window size before hot-error detection becomes active."""

SCALER_RECOVERY_ERROR_CEILING: float = 0.05
"""Scale-up allowed only when window error rate falls below this ceiling."""

SCALER_OVERRUN_HEADROOM_THRESHOLD: int = 1
"""Capacity-overrun scale-down: fire when headroom drops below
``-SCALER_OVERRUN_HEADROOM_THRESHOLD`` (i.e., current workers exceed the
sustainable bandwidth estimate by more than this many slots).
A threshold of 1 means: scale down as soon as we have ≥2 workers more than
the connection can sustain at the current file size."""

# --- Phase B: AIMD plateau guard ---

SCALER_PLATEAU_THRESHOLD: float = 0.05
"""Minimum fractional throughput gain (vs the bps recorded at the last scale-up)
required before another scale-up is allowed.  0.05 = 5%.
Prevents jumping past the network saturation point: if adding workers didn't
actually increase total MB/s by at least this margin, we've hit the ceiling."""

# --- Phase C: Universal Scalability Law (USL) ---

SCALER_USL_MIN_LEVELS: int = 5
"""Minimum number of *distinct* worker-count levels that must each have at least
``SCALER_USL_MIN_SAMPLES_PER_LEVEL`` throughput observations before a USL curve
fit is attempted."""

SCALER_USL_MIN_SAMPLES_PER_LEVEL: int = 3
"""Minimum observations per worker-count level for USL fitting."""

SCALER_USL_AGREE_TOLERANCE: float = 0.20
"""Fractional difference below which the formula estimate (Phase B) and the USL
N_opt (Phase C) are considered to *agree*.  When they agree the USL ceiling is
enforced; when they disagree the more conservative of the two is used and the
scaler adds only one worker at a time until they converge.  0.20 = within 20%."""


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


def _try_delete_blob(client: "BlobStorageClient", blob_name: str) -> None:
    """Delete blob silently — used for best-effort cleanup after upload failure."""
    try:
        client.delete_blob(blob_name)
    except Exception:
        pass


def _attempt_upload_verify(
    client: "BlobStorageClient",
    blob_name: str,
    file_path: str,
    expected_rows: int,
    expected_schema: pa.Schema,
    timeout: int,
) -> Exception | None:
    """Single upload + verify cycle. Returns None on success, Exception on any failure.

    Cleans up the blob (best-effort delete) before returning a failure so the
    caller can retry with a clean slate.
    """
    try:
        client.upload_stream(blob_name, file_path, timeout=timeout)
    except Exception as exc:
        _try_delete_blob(client, blob_name)
        return exc

    try:
        actual_rows, actual_schema = client.read_parquet_footer(blob_name)
    except Exception as exc:
        _try_delete_blob(client, blob_name)
        return exc

    if actual_rows != expected_rows:
        _try_delete_blob(client, blob_name)
        return RuntimeError(
            f"Upload verification failed for {blob_name!r}: "
            f"expected {expected_rows} rows, got {actual_rows}"
        )

    if not actual_schema.equals(expected_schema, check_metadata=False):
        _try_delete_blob(client, blob_name)
        return RuntimeError(
            f"Upload verification failed for {blob_name!r}: schema mismatch — "
            f"expected {expected_schema}, got {actual_schema}"
        )

    return None


def _upload_verify_with_retry(
    client: "BlobStorageClient",
    blob_name: str,
    file_path: str,
    expected_rows: int,
    expected_schema: pa.Schema,
    timeout: int = UPLOAD_TIMEOUT_S,
) -> None:
    """Upload file_path to blob_name and verify row count + schema.

    Retries once on any failure (upload error or verification mismatch).
    Deletes the blob before each retry so the next attempt starts clean.
    Raises RuntimeError if both attempts fail.
    """
    last_exc: Exception | None = None
    for _ in range(2):
        last_exc = _attempt_upload_verify(
            client, blob_name, file_path, expected_rows, expected_schema, timeout
        )
        if last_exc is None:
            return
    raise RuntimeError(
        f"Upload of {blob_name!r} failed after 2 attempts: {last_exc}"
    ) from last_exc


class SchemaLoaderWorker(QThread):
    schema_loaded = pyqtSignal(object, int, object, object)
    error = pyqtSignal(str)

    def __init__(self, connection_string: str, container: str, prefix: str, parent=None):
        super().__init__(parent)
        self._connection_string = connection_string
        self._container = container
        self._prefix = prefix

    def run(self) -> None:
        c_list = c_schema = None
        try:
            c_list = BlobStorageClient(self._connection_string, self._container)
            c_schema = BlobStorageClient(self._connection_string, self._container)

            def _fetch_schema() -> pa.Schema:
                first = c_schema.list_first_parquet_blob(self._prefix)
                if first is None:
                    raise RuntimeError(
                        f"No .parquet files found under prefix '{self._prefix}'."
                    )
                return c_schema.read_schema(first)

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                fut_blobs = pool.submit(c_list.list_blobs_with_sizes, self._prefix)
                fut_schema = pool.submit(_fetch_schema)

            blobs = fut_blobs.result()
            schema = fut_schema.result()

            if not blobs:
                self.error.emit(f"No .parquet files found under prefix '{self._prefix}'.")
                return

            known   = [(n, s) for n, s in blobs if s >= 0]
            unknown = [n      for n, s in blobs if s <  0]
            total_bytes = sum(s for _, s in known)
            self.schema_loaded.emit(schema, len(blobs), total_bytes, unknown)
        except Exception as exc:
            short_error, _, _ = _summarize_exception(exc)
            self.error.emit(short_error)
        finally:
            for _c in (c_list, c_schema):
                if _c is not None:
                    _c.close()


class TransformWorker(QThread):
    """
    Processes all Parquet blobs: download -> transform -> upload.

    Adaptive concurrency
    --------------------
    When autoscale=True, an AdaptiveScaler continuously monitors upload
    throughput and error rate.  Every SCALER_CHECK_INTERVAL successfully
    completed files it may scale the worker count up or down based on:

    - Scale down: recent error rate exceeds SCALER_DOWN_ERROR_RATE, or recent
      throughput drops more than SCALER_DOWN_THROUGHPUT_DROP below the window
      median.
    - Scale up: available headroom (configured_max − current) is at least
      SCALER_UP_MIN_HEADROOM for SCALER_UP_CONFIRM_CHECKS consecutive checks.

    Changes are announced via the ``workers_scaled`` signal.
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
    workers_scaled = pyqtSignal(int, int, str, str)
    # (new_count, old_count, direction, reason)
    # direction: "up" | "down"
    log_message = pyqtSignal(str)
    throughput_update = pyqtSignal(float, float)
    # (measured_total_bps, estimated_capacity_bps)
    bandwidth_cap_changed = pyqtSignal(int)
    # effective bandwidth ceiling in workers (dynamic, emitted whenever it changes)
    # Emitted on every scaler check — even when no scaling decision is made.

    def __init__(
        self,
        connection_string: str,
        container: str,
        prefix: str,
        col_configs: list[ColumnConfig],
        output_prefix: str | None,
        dry_run: bool = False,
        worker_count: int = 1,
        max_worker_cap: int | None = None,
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
        cap = max_worker_cap if max_worker_cap is not None else self._worker_count
        self._autoscale_max_workers = max(self._worker_count, cap)
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
        self._completed_since_scale_check = 0
        self._pending_force_scale = False
        self._real_work_started = False

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
                self.log_message.emit(
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
                    self.log_message.emit(
                        f"[Failed List] Retrying previously failed file: {name} ({entry_type})"
                    )

        total = len(blob_names)
        if total == 0:
            if self._checkpoint:
                self._checkpoint.mark_complete()
            self.finished.emit(0, 0, 0, 0.0, 0.0, [])
            return

        # ------------------------------------------------------------------
        # Determine p95 file size (used by scaler)
        # ------------------------------------------------------------------
        p95_bytes = 0
        if blob_sizes:
            sorted_sizes = sorted(s for s in blob_sizes.values() if s > 0)
            if sorted_sizes:
                p95_idx = max(0, int(len(sorted_sizes) * 0.95) - 1)
                p95_bytes = sorted_sizes[p95_idx]

        # ------------------------------------------------------------------
        # Shared counters
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

        # Adaptive scaler (only active when autoscale=True and not a dry run)
        scaler: AdaptiveScaler | None = None
        self._completed_since_scale_check = 0
        self._pending_force_scale = False
        self._real_work_started = False
        if self._autoscale and not self._dry_run:
            scaler = AdaptiveScaler(
                window_size=SCALER_WINDOW_SIZE,
                min_samples=SCALER_MIN_SAMPLES,
                upload_timeout_s=UPLOAD_TIMEOUT_S,
                down_error_rate=SCALER_DOWN_ERROR_RATE,
                down_throughput_drop=SCALER_DOWN_THROUGHPUT_DROP,
                up_min_headroom=SCALER_UP_MIN_HEADROOM,
                up_confirm_checks=SCALER_UP_CONFIRM_CHECKS,
                up_max_step=SCALER_UP_MAX_STEP,
                configured_max_workers=self._autoscale_max_workers,
                hot_error_rate=SCALER_HOT_ERROR_RATE,
                hot_error_min_samples=SCALER_HOT_ERROR_MIN_SAMPLES,
                recovery_error_ceiling=SCALER_RECOVERY_ERROR_CEILING,
                overrun_headroom_threshold=SCALER_OVERRUN_HEADROOM_THRESHOLD,
                plateau_threshold=SCALER_PLATEAU_THRESHOLD,
                usl_min_levels=SCALER_USL_MIN_LEVELS,
                usl_min_samples_per_level=SCALER_USL_MIN_SAMPLES_PER_LEVEL,
                usl_agree_tolerance=SCALER_USL_AGREE_TOLERANCE,
            )

        def finalize_success(*, blob_name, worker_id, attempts, duration_ms,
                             skipped, note, size_bytes):
            nonlocal processed, completed, total_duration_ms
            first_real_trigger = False
            with stats_lock:
                processed += 1
                completed += 1
                completed_so_far = completed
                total_duration_ms += duration_ms
                if self._checkpoint:
                    self._checkpoint.advance_cursor(blob_name)
                if self._failed_list:
                    self._failed_list.remove(blob_name)
                if scaler is not None and not skipped:
                    self._completed_since_scale_check += 1
                    if not self._real_work_started:
                        self._real_work_started = True
                        self._pending_force_scale = True
                        first_real_trigger = True

            if first_real_trigger:
                self.log_message.emit("[Autoscale WARN] First transformed file detected — forcing scale check")

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
                # Network failures count towards scaling checks
                if scaler is not None and retriable:
                    self._completed_since_scale_check += 1

            if retriable:
                self._log_final_failure(blob_name, attempts, short_error, error, suppress_trace)
            self.progress.emit(completed_so_far, total, blob_name,
                               duration_ms, worker_id, False, "", size_bytes)

        def run_pass(blob_batch: list[str], pass_number: int) -> tuple[list[str], int]:
            if not blob_batch or self._cancel_event.is_set():
                return [], 0

            worker_total = min(self._worker_count, len(blob_batch))
            task_queue: queue.Queue[Any] = queue.Queue()
            sentinel = object()  # used only as a cancel-path escape hatch; not pre-pushed
            for name in blob_batch:
                task_queue.put(name)

            next_round: list[str] = []
            next_round_lock = threading.Lock()
            conn_error_count = 0
            conn_error_lock  = threading.Lock()

            # Pending work counter — decremented each time a file reaches its final
            # disposition (success or all attempts exhausted).  When it hits 0 and
            # the queue is empty, idle threads exit cleanly.
            pending = [len(blob_batch)]
            pending_lock = threading.Lock()

            # Cooperative scale-down: when the scaler requests fewer workers, this
            # counter is incremented by the delta.  Each thread checks before pulling
            # its next task and exits voluntarily, leaving the file in the queue for
            # the remaining (now fewer) threads to pick up.
            _exit_requested = [0]
            _exit_lock = threading.Lock()

            # Dynamic thread pool — new threads may be spawned mid-pass on scale-up.
            _threads: list[threading.Thread] = []
            _threads_lock = threading.Lock()
            _next_id = [1]  # Worker IDs, shared by initial and dynamically spawned threads

            def _spawn_workers(count: int) -> None:
                """Spawn `count` new worker threads.

                Threads self-terminate via the pending counter or the cooperative
                exit mechanism; no sentinel is pushed per thread.
                """
                for _ in range(count):
                    wid = _next_id[0]
                    _next_id[0] += 1
                    t = threading.Thread(target=worker_loop, args=(wid,), daemon=True)
                    with _threads_lock:
                        _threads.append(t)
                    t.start()

            # One-at-a-time scale check: the first thread to acquire wins;
            # others skip (the interval counter will catch the next window).
            _scale_check_lock = threading.Lock()

            def _try_scale_check() -> None:
                """Run scale check if due; spawn new threads on scale-up.

                Compares the scaler's target against *actually alive* threads so
                that threads which survived a soft scale-down are counted and we
                never stack new threads on top of already-running ones.
                """
                if not _scale_check_lock.acquire(blocking=False):
                    return
                try:
                    delta = self._maybe_run_scale_check(scaler, p95_bytes)
                    if delta > 0:
                        # How many threads are genuinely still running?
                        with _threads_lock:
                            actual_alive = sum(1 for t in _threads if t.is_alive())
                        target = self._worker_count  # just updated by _maybe_run_scale_check
                        to_spawn = max(0, target - actual_alive)
                        if to_spawn > 0:
                            _spawn_workers(to_spawn)
                    elif delta < 0:
                        # Request |delta| threads to exit after their current file.
                        # Each thread checks _exit_requested before pulling the next
                        # task, so the reduction takes effect as soon as threads
                        # finish whatever they are currently downloading/uploading.
                        with _exit_lock:
                            _exit_requested[0] += abs(delta)
                finally:
                    _scale_check_lock.release()

            def worker_loop(worker_id: int) -> None:
                nonlocal conn_error_count
                worker_client = BlobStorageClient(self._connection_string, self._container)
                try:
                    while True:
                        # ── Cooperative scale-down ──────────────────────────────────
                        # Before pulling the next task, check whether the scaler has
                        # requested a reduction.  The current file (if any) has already
                        # been processed; the next file stays in the queue for the
                        # surviving threads.  This limits extra bandwidth consumption
                        # to at most one extra file per excess thread after a halving.
                        with _exit_lock:
                            if _exit_requested[0] > 0:
                                _exit_requested[0] -= 1
                                return

                        # ── Pull next task (short timeout so exit check re-runs) ───
                        try:
                            item = task_queue.get(block=True, timeout=0.5)
                        except queue.Empty:
                            # Queue empty — exit only when all pending work is done.
                            # pending > 0 means another thread may re-enqueue a file
                            # after a failed attempt; keep waiting.
                            with pending_lock:
                                if pending[0] <= 0:
                                    return
                            continue

                        if item is sentinel:
                            task_queue.task_done()
                            return

                        blob_name = str(item)
                        self._pause_event.wait()
                        if self._cancel_event.is_set():
                            # Return the file to the queue so the end-of-pass drain
                            # can collect it for the next run.
                            task_queue.put(blob_name)
                            task_queue.task_done()
                            return

                        size_bytes = blob_sizes.get(blob_name, 0)
                        attempt_counter[blob_name] = attempt_counter.get(blob_name, 0) + 1
                        result = self._process_blob_once(worker_client, blob_name)
                        duration_tracker[blob_name] = (
                            duration_tracker.get(blob_name, 0.0) + result.duration_ms
                        )
                        aggregated_duration = duration_tracker[blob_name]

                        # Feed upload measurement to scaler
                        if scaler is not None:
                            if result.upload_bytes > 0 and result.upload_seconds > 0:
                                scaler.record_upload(result.upload_bytes, result.upload_seconds, True)
                            elif result.status == "error":
                                scaler.record_upload(0, 0.0, False)
                            if scaler.consume_hot_error_flag():
                                self._pending_force_scale = True
                                self.log_message.emit("[Autoscale WARN] Timeout spike detected — forcing scale check")
                                _try_scale_check()

                        if result.status in {"success", "skipped"}:
                            finalize_success(
                                blob_name=blob_name, worker_id=worker_id,
                                attempts=attempt_counter[blob_name],
                                duration_ms=aggregated_duration,
                                skipped=result.status == "skipped",
                                note=result.skip_reason or "", size_bytes=size_bytes,
                            )
                            duration_tracker.pop(blob_name, None)
                            with pending_lock:
                                pending[0] -= 1

                        else:
                            error_text = result.error or "Unknown error"
                            short_msg  = result.short_error or _first_line(error_text)

                            if result.suppress_trace:
                                with conn_error_lock:
                                    conn_error_count += 1

                            if not result.retriable:
                                # Corrupted file — permanent failure, never retry.
                                self.corrupted_blob.emit(blob_name, short_msg, size_bytes)
                                finalize_failure(
                                    blob_name=blob_name, worker_id=worker_id,
                                    attempts=attempt_counter[blob_name],
                                    duration_ms=aggregated_duration, error=error_text,
                                    short_error=short_msg, suppress_trace=result.suppress_trace,
                                    retriable=False, size_bytes=size_bytes,
                                )
                                duration_tracker.pop(blob_name, None)
                                with pending_lock:
                                    pending[0] -= 1

                            elif attempt_counter.get(blob_name, 0) < self._max_attempts:
                                # Retriable error, more attempts remain.
                                # Re-queue immediately so the (now smaller) thread pool
                                # picks it up without waiting for a new pass or back-off.
                                # The total attempt count is preserved in attempt_counter
                                # across both intra-pass retries and inter-pass rounds.
                                task_queue.put(blob_name)
                                self._completed_since_scale_check += 1  # keep interval checks firing
                                self._log_retry(
                                    blob_name, attempt_counter[blob_name],
                                    short_msg,
                                    None if result.suppress_trace else error_text,
                                )
                                # pending unchanged — the file still needs work

                            else:
                                # All attempts exhausted.
                                finalize_failure(
                                    blob_name=blob_name, worker_id=worker_id,
                                    attempts=attempt_counter[blob_name],
                                    duration_ms=aggregated_duration, error=error_text,
                                    short_error=short_msg, suppress_trace=result.suppress_trace,
                                    retriable=True, size_bytes=size_bytes,
                                )
                                duration_tracker.pop(blob_name, None)
                                with pending_lock:
                                    pending[0] -= 1

                        # Interval / forced scale check
                        if scaler is not None:
                            _try_scale_check()

                        task_queue.task_done()

                finally:
                    worker_client.close()

            _spawn_workers(worker_total)

            # Wait for all threads, including those dynamically spawned during scale-up.
            # On cancel: exit the loop immediately — worker threads are daemon=True
            # and will be killed when the process exits.  Waiting here after cancel
            # would block window close for the full download/upload timeout (Issue 3).
            while True:
                with _threads_lock:
                    alive = [t for t in _threads if t.is_alive()]
                if not alive:
                    break
                if self._cancel_event.is_set():
                    break
                for t in alive:
                    t.join(timeout=0.5)

            # Drain files remaining in the queue after cooperative thread exits.
            # These are files that were re-queued after a failure (or not yet
            # started) when threads exited early due to a scale-down.  They
            # haven't exhausted max_attempts, so forward them to next_round for
            # the next pass (which runs with the reduced worker count after the
            # back-off delay).
            while True:
                try:
                    item = task_queue.get_nowait()
                except queue.Empty:
                    break
                if isinstance(item, str):
                    with next_round_lock:
                        next_round.append(item)

            return next_round, conn_error_count

        remaining = list(blob_names)

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
                self._worker_count = max(2, self._worker_count // 2)
                if self._worker_count < old_count:
                    self.workers_reduced.emit(self._worker_count, conn_errors)

            remaining = next_round
            pass_number += 1

        if self._cancel_event.is_set():
            self.cancelled.emit()
            return

        if self._checkpoint:
            self._checkpoint.mark_complete()
            self.log_message.emit("[Checkpoint] Run marked as complete")

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

    def _maybe_run_scale_check(self, scaler: AdaptiveScaler | None, p95_bytes: int) -> int:
        """Run AdaptiveScaler when either interval or forced conditions are met.

        Returns the worker-count delta: positive means scale-up, negative means scale-down,
        0 means no change.  The caller is responsible for spawning new threads on scale-up.

        Always emits ``throughput_update`` when a check actually runs (regardless
        of whether the worker count changes), so the UI can update its live meters.
        """
        if scaler is None or p95_bytes <= 0:
            return 0
        interval_due = self._completed_since_scale_check >= SCALER_CHECK_INTERVAL
        force_due = self._pending_force_scale
        if not (interval_due or force_due):
            return 0
        if not scaler.window_ready():
            return 0
        if interval_due:
            self._completed_since_scale_check = 0
        if force_due:
            self._pending_force_scale = False

        old_count = self._worker_count

        # Compute throughput at the current (pre-decision) worker count.
        # This is what gets recorded for USL fitting — it reflects actual
        # observed performance at old_count, not a projection.
        measured_bps, capacity_bps = scaler.compute_throughput_stats(old_count, p95_bytes)

        # Phase C: feed (N, bps) observation before the scaling decision so the
        # USL model learns from the current stable state, not the new target.
        if measured_bps > 0:
            scaler.record_throughput_observation(old_count, measured_bps)

        # Scale decision (Phase B plateau guard + Phase C ceiling both apply inside)
        new_count, reason = scaler.should_scale(old_count, p95_bytes)
        if new_count != old_count:
            self._worker_count = new_count
            direction = "up" if new_count > old_count else "down"
            self.workers_scaled.emit(new_count, old_count, direction, reason)

        # Log the moment Phase C (USL) first comes online
        if scaler.usl_just_activated():
            alpha, beta, n_opt = scaler.get_usl_result()
            self.log_message.emit(
                f"[USL] Model fitted — α={alpha:.3f} (contention) "
                f"β={beta:.4f} (coherency) → N_opt={n_opt} workers"
            )

        # Log and signal whenever the effective bandwidth ceiling changes (grows or shrinks)
        cap_changed, new_cap = scaler.consume_bandwidth_cap_update()
        if cap_changed:
            self.log_message.emit(
                f"[Autoscale] Bandwidth ceiling: {new_cap} workers"
            )
            self.bandwidth_cap_changed.emit(new_cap)

        # Emit throughput stats for the ResourcesPanel (use new worker count so
        # the UI reflects the post-decision state)
        if self._worker_count != old_count:
            measured_bps, capacity_bps = scaler.compute_throughput_stats(
                self._worker_count, p95_bytes
            )
        self.throughput_update.emit(measured_bps, capacity_bps)

        return new_count - old_count

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
                pq.write_table(table, out_buf, compression="zstd", compression_level=3)
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


# ---------------------------------------------------------------------------
# Data Collector Worker
# ---------------------------------------------------------------------------

class DataCollectorWorker(QThread):
    """
    Parallel producer-consumer collector.

    N producer threads download and filter Parquet blobs from Azure.
    One writer thread accumulates filtered chunks and flushes to a local
    temp file via ParquetWriter when the RAM buffer reaches ram_limit_mb.
    After all producers finish the temp file is rewritten with recalculated
    metadata, uploaded to Azure, and the temp files are deleted.
    """

    listing_complete = pyqtSignal(int)
    progress = pyqtSignal(int, int, str, int)   # completed, total, blob_name, matched_rows
    file_error = pyqtSignal(str, str)
    finished = pyqtSignal(dict)
    cancelled = pyqtSignal()
    log_message = pyqtSignal(str)
    workers_scaled = pyqtSignal(int, int, str, str)
    paused = pyqtSignal()
    resumed = pyqtSignal()

    def __init__(
        self,
        connection_string: str,
        container: str,
        source_prefix: str,
        output_prefix: str,
        filter_col: str,
        filter_values: list[str],
        output_container: str = "",
        max_workers: int = 4,
        ram_limit_mb: int = 1024,
        autoscale: bool = True,
        selected_columns: list[str] | None = None,
        max_output_bytes: int = 0,
        start_part: int | None = None,   # when set, always use _part_NNN naming (subfolder mode)
        _is_subfolder_run: bool = False, # internal: prevents subfolder detection recursion
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._conn = connection_string
        self._container_name = container
        self._source_prefix = source_prefix
        self._output_prefix = output_prefix
        self._filter_col = filter_col
        self._filter_values = filter_values
        self._output_container = output_container or container
        self._max_workers = max_workers
        self._ram_limit_bytes = ram_limit_mb * 1024 * 1024
        self._autoscale = autoscale
        self._selected_columns = selected_columns
        self._max_output_bytes = max_output_bytes
        self._start_part = start_part
        self._is_subfolder_run = _is_subfolder_run
        self._worker_count = max_workers
        self._cancel_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()  # starts unpaused
        self._cancel_reason: str | None = None
        self._subfolder_inner: "DataCollectorWorker | None" = None

    def cancel(self) -> None:
        self._cancel_reason = "user"
        self._cancel_event.set()
        self._pause_event.set()  # unblock any paused producers so they can exit
        # Forward cancellation to a running inner worker (subfolder mode)
        if self._subfolder_inner is not None:
            self._subfolder_inner.cancel()

    def pause(self) -> None:
        if self._pause_event.is_set():   # only act if currently running
            self._pause_event.clear()
            self.paused.emit()

    def resume(self) -> None:
        if not self._pause_event.is_set():   # only act if currently paused
            self._pause_event.set()
            self.resumed.emit()

    def _run_subfolder_mode(self, subfolders: list[str]) -> None:
        """Process *subfolders* one-by-one with a local SubfolderCheckpoint.

        Each subfolder is a synchronous mini-run using a fresh DataCollectorWorker
        with _is_subfolder_run=True (no further subfolder detection). The checkpoint
        is updated after each successful subfolder so interrupted runs resume from
        where they left off.
        """
        checkpoint = SubfolderCheckpoint.load_or_create(
            container=self._container_name,
            source_prefix=self._source_prefix,
            filter_col=self._filter_col,
            filter_values=self._filter_values,
            output_prefix=self._output_prefix,
            output_container=self._output_container,
        )

        n = len(subfolders)
        skipped = checkpoint.done_count
        self.log_message.emit(
            f"{n} Subfolder gefunden — "
            f"{skipped} bereits erledigt, {n - skipped} ausstehend. "
            f"Checkpoint: {checkpoint.path.name}"
        )

        for idx, subfolder in enumerate(subfolders):
            if self._cancel_event.is_set():
                self.cancelled.emit()
                return

            if checkpoint.is_done(subfolder):
                self.log_message.emit(
                    f"Subfolder {idx + 1}/{n}: {subfolder} — übersprungen"
                )
                continue

            self.log_message.emit(
                f"Subfolder {idx + 1}/{n}: {subfolder} — wird verarbeitet …"
            )

            subfolder_prefix = self._source_prefix.rstrip("/") + "/" + subfolder

            inner = DataCollectorWorker(
                connection_string=self._conn,
                container=self._container_name,
                source_prefix=subfolder_prefix,
                output_prefix=self._output_prefix,
                filter_col=self._filter_col,
                filter_values=self._filter_values,
                output_container=self._output_container,
                max_workers=self._max_workers,
                ram_limit_mb=self._ram_limit_bytes // (1024 * 1024),
                autoscale=self._autoscale,
                selected_columns=self._selected_columns,
                max_output_bytes=self._max_output_bytes,
                start_part=checkpoint.next_part,
                _is_subfolder_run=True,
            )

            # Capture result and cancellation synchronously.
            # DirectConnection: slots execute in the current thread immediately
            # when inner.run() emits — no event loop needed.
            _result: dict = {}
            _was_cancelled = [False]

            inner.finished.connect(
                lambda r: _result.update(r),
                Qt.ConnectionType.DirectConnection,
            )
            inner.cancelled.connect(
                lambda: _was_cancelled.__setitem__(0, True),
                Qt.ConnectionType.DirectConnection,
            )

            # Forward display signals to panel (panel uses AutoConnection → QueuedConnection
            # across threads, so these are safely delivered to the main thread).
            inner.progress.connect(self.progress, Qt.ConnectionType.DirectConnection)
            inner.log_message.connect(self.log_message, Qt.ConnectionType.DirectConnection)
            inner.file_error.connect(self.file_error, Qt.ConnectionType.DirectConnection)
            inner.listing_complete.connect(
                self.listing_complete, Qt.ConnectionType.DirectConnection
            )
            inner.workers_scaled.connect(
                self.workers_scaled, Qt.ConnectionType.DirectConnection
            )
            inner.paused.connect(self.paused, Qt.ConnectionType.DirectConnection)
            inner.resumed.connect(self.resumed, Qt.ConnectionType.DirectConnection)

            checkpoint.mark_in_progress(subfolder)
            self._subfolder_inner = inner
            inner.run()   # synchronous — runs in this QThread's thread
            self._subfolder_inner = None

            if _was_cancelled[0] or self._cancel_event.is_set():
                self.cancelled.emit()
                return

            rows = _result.get("rowCount", 0)
            parts_produced = len(_result.get("outputBlobs", []))
            checkpoint.mark_done(subfolder, parts_produced, rows)

            if rows > 0:
                self.log_message.emit(
                    f"Subfolder {subfolder} fertig: "
                    f"{rows:,} Zeilen, {parts_produced} Teil(e)"
                )
            else:
                self.log_message.emit(
                    f"Subfolder {subfolder} fertig: keine Treffer"
                )

        self.log_message.emit(
            f"Alle {n} Subfolder verarbeitet — "
            f"{checkpoint.total_rows:,} Zeilen gesamt"
        )
        self.finished.emit({
            "rowCount": checkpoint.total_rows,
            "outputBlobs": [],
            "outputContainer": self._output_container,
        })

    def run(self) -> None:
        import os
        import tempfile
        from parquet_transform.collector import (
            _REQUIRED_COLS,
            make_output_blob_name,
            MetadataAccumulator,
            rewrite_with_metadata,
        )
        from parquet_transform.scaler import CollectorScaler

        source_client = None
        output_client = None
        tmp1_ref: list = [None]      # mutable so writer can swap the temp file on flush
        writer_ref: list = [None]
        part_names: list[str] = []   # populated by writer; read by main thread after join
        writer_stop = threading.Event()
        writer_thread: threading.Thread | None = None
        # Sentinel: set to True before every terminal signal emit so the
        # finally block can emit finished({rowCount:0}) if an unhandled
        # exception silently skipped all normal exit points (Issue 5).
        _emitted = [False]

        self._cancel_reason = None

        # ── Subfolder detection ───────────────────────────────────────────────
        # When the source prefix contains virtual subdirectories, process them
        # one-by-one with a local checkpoint so interrupted runs can resume.
        # _is_subfolder_run=True suppresses this check for inner runs (prevents recursion).
        if not self._is_subfolder_run:
            try:
                _probe_client = BlobStorageClient(self._conn, self._container_name)
                try:
                    _subfolders = _probe_client.list_blob_prefixes(self._source_prefix)
                finally:
                    _probe_client.close()
            except Exception:
                _subfolders = []

            if _subfolders:
                self._run_subfolder_mode(_subfolders)
                return
        # ── End subfolder detection ───────────────────────────────────────────

        try:
            source_client = BlobStorageClient(self._conn, self._container_name)
            output_client = (
                source_client
                if self._output_container == self._container_name
                else BlobStorageClient(self._conn, self._output_container)
            )
            blobs = source_client.list_blobs(self._source_prefix)
            total = len(blobs)
            self.listing_complete.emit(total)

            if not blobs:
                _emitted[0] = True
                self.finished.emit({"rowCount": 0})
                return

            # ── Shared state ──────────────────────────────────────────────
            task_queue: queue.Queue = queue.Queue()
            for b in blobs:
                task_queue.put(b)

            chunk_queue: queue.Queue = queue.Queue(maxsize=64)

            completed = [0]
            completed_lock = threading.Lock()

            ram_used = [0]
            ram_lock = threading.Lock()

            # Errors from producer threads are buffered here and emitted on
            # the main (run) thread to ensure PyQt signal delivery.
            error_queue: queue.Queue = queue.Queue()

            meta_acc = MetadataAccumulator()

            tmp1_fd, tmp1_initial = tempfile.mkstemp(suffix=".parquet")
            os.close(tmp1_fd)
            tmp1_ref[0] = tmp1_initial

            writer_error: list = [None]
            # Arrow schema used to create the ParquetWriter (set on first chunk).
            # Subsequent chunks are cast to this schema with safe=False so that
            # mixed-schema source files (e.g. some transformed to timestamp[ms,UTC],
            # some still timestamp[ns]) don't abort the collection run.
            writer_schema: list = [None]

            # ── Writer thread ─────────────────────────────────────────────
            # Per-part metadata accumulator (reset after each flush).
            # The global meta_acc tracks totals for finished.emit.
            part_acc_ref: list = [MetadataAccumulator()]
            # When start_part is provided (subfolder mode), start the counter one below
            # so the first _flush_and_upload increment lands on start_part.
            part_counter = [self._start_part - 1 if self._start_part is not None else 0]

            def _flush_and_upload() -> None:
                """Close writer, rewrite metadata, upload via stream, start fresh tmp."""
                if writer_ref[0] is not None:
                    writer_ref[0].close()
                    writer_ref[0] = None

                current_tmp = tmp1_ref[0]
                if part_acc_ref[0].total_rows == 0:
                    return  # nothing to flush

                part_counter[0] += 1
                # Use _part_NNN naming when:
                #   (a) splitting is active (max_output_bytes > 0), OR
                #   (b) called from subfolder mode (start_part set → global part numbering required)
                if self._max_output_bytes > 0 or self._start_part is not None:
                    out_name = make_output_blob_name(
                        self._output_prefix,
                        self._filter_col,
                        self._filter_values,
                        part=part_counter[0],
                    )
                else:
                    out_name = make_output_blob_name(
                        self._output_prefix,
                        self._filter_col,
                        self._filter_values,
                    )

                final_meta = part_acc_ref[0].to_metadata()
                rw_fd, rw_tmp = tempfile.mkstemp(suffix=".parquet")
                os.close(rw_fd)
                try:
                    rewrite_with_metadata(current_tmp, rw_tmp, final_meta)
                    _upload_verify_with_retry(
                        output_client,
                        out_name,
                        rw_tmp,
                        expected_rows=part_acc_ref[0].total_rows,
                        expected_schema=writer_schema[0],
                    )
                    part_names.append(out_name)
                finally:
                    try:
                        os.unlink(rw_tmp)
                    except Exception:
                        pass
                    # Always clean up the raw accumulation file regardless of
                    # whether rewrite/upload succeeded; prevents orphaned temp
                    # files when upload_stream raises (e.g. network timeout).
                    try:
                        os.unlink(current_tmp)
                    except Exception:
                        pass

                # Prepare a fresh temp file for the next part
                new_fd, new_tmp = tempfile.mkstemp(suffix=".parquet")
                os.close(new_fd)
                tmp1_ref[0] = new_tmp
                writer_schema[0] = None          # force re-init on next write
                part_acc_ref[0] = MetadataAccumulator()

            def _writer_loop() -> None:
                exited_normally = False
                try:
                    while True:
                        try:
                            chunk = chunk_queue.get(timeout=0.2)
                        except queue.Empty:
                            if writer_stop.is_set() and chunk_queue.empty():
                                exited_normally = True
                                break
                            continue
                        if chunk is None:
                            # Cancel path — do NOT flush
                            break
                        meta_acc.update(chunk)
                        part_acc_ref[0].update(chunk)
                        if writer_ref[0] is None:
                            writer_schema[0] = chunk.schema
                            writer_ref[0] = pq.ParquetWriter(
                                tmp1_ref[0], chunk.schema,
                                compression="zstd", compression_level=3,
                            )
                        # If this chunk's schema differs from the writer schema
                        # (e.g. mixed timestamp[ns] / timestamp[ms,UTC] sources),
                        # cast with safe=False so sub-ms precision is truncated
                        # rather than aborting the entire collection run.
                        write_chunk = chunk
                        if not chunk.schema.equals(writer_schema[0], check_metadata=False):
                            write_chunk = chunk.cast(writer_schema[0], safe=False)
                        writer_ref[0].write_table(write_chunk)
                        with ram_lock:
                            ram_used[0] = max(0, ram_used[0] - chunk.nbytes)

                        # Size-based flush
                        if (
                            self._max_output_bytes > 0
                            and os.path.getsize(tmp1_ref[0]) >= self._max_output_bytes
                        ):
                            _flush_and_upload()

                    if exited_normally:
                        _flush_and_upload()

                except Exception as exc:  # noqa: BLE001
                    msg, _, _ = _summarize_exception(exc)
                    writer_error[0] = msg
                    self._cancel_reason = "writer_error"
                    self._cancel_event.set()

            writer_thread = threading.Thread(target=_writer_loop, daemon=True)
            writer_thread.start()

            # ── Producer worker function ───────────────────────────────────
            _exit_requested = [0]
            _exit_lock = threading.Lock()
            _threads: list[threading.Thread] = []
            _threads_lock = threading.Lock()
            _next_id = [1]  # only mutated from coordination loop (single thread)

            scaler = (
                CollectorScaler(max_workers=self._max_workers)
                if self._autoscale else None
            )

            def _producer(wid: int) -> None:
                client = BlobStorageClient(self._conn, self._container_name)
                try:
                    while not self._cancel_event.is_set():
                        with _exit_lock:
                            if _exit_requested[0] > 0:
                                _exit_requested[0] -= 1
                                return
                        try:
                            blob_name = task_queue.get_nowait()
                        except queue.Empty:
                            return
                        self._pause_event.wait()   # block here when paused
                        if self._cancel_event.is_set():
                            # Blob was dequeued but not processed — completed[0] is not
                            # incremented here. The coordination loop handles this correctly
                            # by draining the queue and breaking on cancel.
                            return
                        matched_rows: int = 0
                        try:
                            t0 = time.monotonic()
                            raw = client.download_bytes(blob_name, timeout=DOWNLOAD_TIMEOUT_S)
                            dl_s = time.monotonic() - t0
                            # Predicate pushdown: PyArrow skips row groups at C++ level.
                            # Pass columns= at the same time to avoid decoding unused columns.
                            # Note: if filter_col is absent from a file's schema, PyArrow
                            # raises ArrowInvalid (FieldRef not found) which is caught below
                            # and routed to error_queue like any other per-blob error.
                            table = pq.read_table(
                                io.BytesIO(raw),
                                columns=self._selected_columns,
                                filters=[(self._filter_col, "in", self._filter_values)],
                            )
                            # Guard: some older source files may have been written
                            # before all required columns existed. PyArrow silently
                            # omits missing columns from the result rather than
                            # raising, so we detect the gap explicitly and route it
                            # as a per-file error rather than crashing the writer.
                            missing = _REQUIRED_COLS - set(table.schema.names)
                            if missing:
                                raise ValueError(
                                    f"File uses an older schema — missing required "
                                    f"column(s): {sorted(missing)}. Rows skipped."
                                )
                            matched_rows = table.num_rows
                            filtered = table
                            if scaler is not None:
                                scaler.record_download(len(raw), dl_s, True)
                            if filtered.num_rows > 0:
                                with ram_lock:
                                    ram_used[0] += filtered.nbytes
                                while not self._cancel_event.is_set():
                                    try:
                                        chunk_queue.put(filtered, timeout=0.5)
                                        break
                                    except queue.Full:
                                        continue
                        except Exception as exc:
                            msg, _, _ = _summarize_exception(exc)
                            error_queue.put((blob_name, msg))
                            if scaler is not None:
                                scaler.record_download(0, 0.0, False)
                        finally:
                            with completed_lock:
                                completed[0] += 1
                                done = completed[0]
                            # progress is safe to emit from threads (Qt queued delivery)
                            # file_error uses error_queue to guarantee delivery without an event loop
                            _panel_deleted = False
                            try:
                                self.progress.emit(done, total, blob_name, matched_rows)
                            except RuntimeError as exc:
                                # Happens when the owning QObject (panel closed) was deleted
                                # before the background producer threads drained.
                                if "has been deleted" not in str(exc):
                                    raise
                                _panel_deleted = True
                        # Return OUTSIDE the finally block so no pending exception
                        # is silently suppressed (SyntaxWarning: 'return' in 'finally').
                        if _panel_deleted:
                            return
                finally:
                    client.close()

            def _spawn(count: int) -> None:
                for _ in range(count):
                    wid = _next_id[0]
                    _next_id[0] += 1
                    t = threading.Thread(target=_producer, args=(wid,), daemon=True)
                    with _threads_lock:
                        _threads.append(t)
                    t.start()

            _spawn(self._worker_count)

            # ── Main coordination loop ────────────────────────────────────
            _scale_ticks = [0]
            SCALE_INTERVAL = 5

            while True:
                if self._cancel_event.is_set():
                    while not task_queue.empty():
                        try:
                            task_queue.get_nowait()
                        except queue.Empty:
                            break
                    break

                with completed_lock:
                    done = completed[0]
                if done >= total:
                    break

                # Drain file errors on the main thread for correct Qt signal delivery
                while not error_queue.empty():
                    try:
                        _blob, _msg = error_queue.get_nowait()
                        self.file_error.emit(_blob, _msg)
                    except queue.Empty:
                        break

                if scaler is not None:
                    _scale_ticks[0] += 1
                    if _scale_ticks[0] >= SCALE_INTERVAL:
                        _scale_ticks[0] = 0
                        with ram_lock:
                            current_ram = ram_used[0]
                        new_count, reason = scaler.should_scale(
                            self._worker_count, current_ram, self._ram_limit_bytes
                        )
                        if new_count != self._worker_count:
                            old = self._worker_count
                            self._worker_count = new_count
                            direction = "up" if new_count > old else "down"
                            self.workers_scaled.emit(new_count, old, direction, reason)
                            self.log_message.emit(
                                f"[Collector] Workers {old}→{new_count}: {reason}"
                            )
                            if new_count > old:
                                _spawn(new_count - old)
                            else:
                                with _exit_lock:
                                    _exit_requested[0] += old - new_count

                time.sleep(0.05)

            # Wait for all producers.
            # On cancel: skip the join — producer threads are daemon=True and will
            # be killed when the process exits.  Joining them here could block for
            # up to DOWNLOAD_TIMEOUT_S per thread (hundreds of seconds total with
            # many workers), far exceeding the 5 s window-close wait (Issue 2).
            with _threads_lock:
                all_threads = list(_threads)
            if not self._cancel_event.is_set():
                for t in all_threads:
                    t.join(timeout=5.0)

            # Final drain of any errors buffered during the last tick
            while not error_queue.empty():
                try:
                    _blob, _msg = error_queue.get_nowait()
                    self.file_error.emit(_blob, _msg)
                except queue.Empty:
                    break

            if self._cancel_event.is_set():
                chunk_queue.put(None)
                writer_thread.join(timeout=5.0)
                _emitted[0] = True
                reason = self._cancel_reason or "user"
                if reason == "writer_error" and writer_error[0]:
                    self.log_message.emit(f"Data-Collector writer error: {writer_error[0]}")
                elif reason == "user":
                    self.log_message.emit("Data-Collector cancelled by user.")
                else:
                    self.log_message.emit(f"Data-Collector cancelled ({reason}).")
                self.cancelled.emit()
                return

            # Stop writer
            writer_stop.set()
            writer_thread.join(timeout=300.0)
            if writer_thread.is_alive():
                self.log_message.emit("Data-Collector writer thread timed out — aborting")
                _emitted[0] = True
                self.finished.emit({"rowCount": 0})
                return

            if writer_error[0]:
                self.log_message.emit(f"Data-Collector writer error: {writer_error[0]}")
                _emitted[0] = True
                self.finished.emit({"rowCount": 0})
                return

            if writer_ref[0] is not None:
                writer_ref[0].close()
                writer_ref[0] = None

            if meta_acc.total_rows == 0:
                self.log_message.emit(
                    f"Data-Collector: no rows matched "
                    f"{self._filter_col} {self._filter_values}"
                )
                _emitted[0] = True
                self.finished.emit({"rowCount": 0})
                return

            # Writer handled all uploads — part_names is populated
            self.log_message.emit(
                f"Data-Collector: uploaded {len(part_names)} part(s) → "
                f"{self._output_container} ({meta_acc.total_rows} rows)"
            )
            _emitted[0] = True
            self.finished.emit({
                "rowCount": meta_acc.total_rows,
                "outputBlobs": part_names,
                "outputContainer": self._output_container,
            })

        except Exception as exc:  # noqa: BLE001
            # Catches unhandled errors (e.g. upload failure, metadata rewrite)
            # that would otherwise exit run() silently — leaving the UI frozen.
            self.log_message.emit(f"Data-Collector unexpected error: {exc}")

        finally:
            # Signal and join writer thread before closing/unlinking to avoid
            # racing with an open file handle on Windows.
            writer_stop.set()
            if writer_thread is not None and writer_thread.is_alive():
                writer_thread.join(timeout=5.0)
            if writer_ref[0] is not None:
                try:
                    writer_ref[0].close()
                except Exception:
                    pass
            if source_client is not None:
                source_client.close()
            if output_client is not None and output_client is not source_client:
                output_client.close()
            p = tmp1_ref[0]
            if p is not None:
                try:
                    os.unlink(p)
                except Exception:
                    pass
            # Ensure the UI is never left frozen if an unhandled exception
            # bypassed all normal exit paths without emitting a terminal signal.
            if not _emitted[0]:
                self.finished.emit({"rowCount": 0})
