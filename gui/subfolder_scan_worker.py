"""
Background QThread worker for adaptive two-stage subfolder scanning.

Stage 1 (fast): reads the first + last Parquet footer per subfolder
    to classify as green (done) / red (needs work) / yellow (mixed) / grey (empty).

Stage 2 (deep, yellow only): reads every footer in mixed subfolders to build
    an exact list of files still needing work, enabling TransformWorker to skip
    the listing + download-then-check cycle entirely.
"""
from __future__ import annotations

import concurrent.futures
from dataclasses import dataclass, field
from threading import Event
from typing import Literal

import pyarrow as pa
from PyQt6.QtCore import QThread, pyqtSignal

from parquet_transform.processor import ColumnConfig
from parquet_transform.storage import BlobStorageClient
from parquet_transform.transforms import get_expected_output_type


# ---------------------------------------------------------------------------
# Pure helpers (no Qt, no I/O — easy to test)
# ---------------------------------------------------------------------------

def _check_file(schema: pa.Schema, col_configs: list[ColumnConfig]) -> bool:
    """Return True if every configured column is already at its target type.

    Mirrors the logic of TransformWorker._should_skip_table but operates on a
    pa.Schema directly (no full table download needed).
    """
    for cfg in col_configs:
        expected = get_expected_output_type(cfg.transform)
        if expected is None:
            return False  # unknown transform → can't confirm done
        if cfg.name not in schema.names:
            return False  # column absent → can't confirm done
        actual_type = schema.field(cfg.name).type
        if actual_type != expected:
            return False
    return True


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

SubfolderStatus = Literal["green", "yellow", "red", "grey"]


@dataclass
class SubfolderScanResult:
    """Scan result for a single one-level-deep subfolder.

    Attributes:
        name:           Display name, e.g. "14-02-2026".
        prefix:         Full Azure prefix, e.g. "DataCollector/LiveData/14-02-2026".
        total:          Total parquet files found.
        done:           Files already at target schema (-1 = not yet deep-scanned).
        pending:        Files needing transformation (-1 = not yet deep-scanned).
        status:         "green" / "yellow" / "red" / "grey".
        pending_blobs:  Exact blob names to process (empty list for green/grey;
                        full list for red; filtered list for yellow after deep scan).
        pending_sizes:  Mapping blob_name → size in bytes for pending_blobs
                        (may contain -1 for blobs with unknown size).
    """
    name: str
    prefix: str
    total: int
    done: int
    pending: int
    status: SubfolderStatus
    pending_blobs: list[str] = field(default_factory=list)
    pending_sizes: dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

class SubfolderScanWorker(QThread):
    """Two-stage adaptive subfolder scanner.

    Signals:
        subfolder_scanned(SubfolderScanResult): emitted for each subfolder as
            soon as its final status is known (after Stage 2 for yellow ones).
        progress(int, int): (scanned_so_far, total_subfolders) — Stage 1 ticks.
        scan_complete(): emitted when all stages are done (or cancelled).
        log_message(str): informational messages for the UI log.
        error(str): emitted on unexpected exceptions; scan_complete also fires.
    """

    subfolder_scanned = pyqtSignal(object)   # SubfolderScanResult
    progress = pyqtSignal(int, int)           # scanned, total
    scan_complete = pyqtSignal()
    log_message = pyqtSignal(str)
    error = pyqtSignal(str)

    def __init__(
        self,
        connection_string: str,
        container: str,
        header_prefix: str,
        col_configs: list[ColumnConfig],
        scan_workers: int = 16,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._conn_str = connection_string
        self._container = container
        self._header_prefix = header_prefix
        self._col_configs = col_configs
        self._scan_workers = max(1, scan_workers)
        self._cancel_event = Event()

    def cancel(self) -> None:
        self._cancel_event.set()

    # ------------------------------------------------------------------
    # QThread entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        try:
            self._run()
        except Exception as exc:
            self.error.emit(str(exc))
            self.scan_complete.emit()

    def _run(self) -> None:
        # Discover subfolders (one listing API call)
        client = BlobStorageClient(self._conn_str, self._container)
        subfolder_names = client.list_blob_prefixes(self._header_prefix)

        if not subfolder_names:
            self.log_message.emit("[Scan] No subfolders found under this prefix.")
            self.scan_complete.emit()
            return

        total = len(subfolder_names)
        self.log_message.emit(f"[Scan] Found {total} subfolder(s). Stage 1: first+last sample...")

        # Build full prefixes
        norm = (self._header_prefix.rstrip("/") + "/") if self._header_prefix.strip("/") else ""
        args = [(name, f"{norm}{name}") for name in subfolder_names]

        # ------------------------------------------------------------------
        # Stage 1: parallel first+last footer read per subfolder
        # ------------------------------------------------------------------
        yellow_results: list[SubfolderScanResult] = []
        scanned = 0

        def _stage1_one(name_prefix: tuple[str, str]) -> SubfolderScanResult:
            name, prefix = name_prefix
            # Each thread gets its own client (follows existing TransformWorker pattern)
            c = BlobStorageClient(self._conn_str, self._container)
            blobs_with_sizes = c.list_blobs_with_sizes(prefix)

            if not blobs_with_sizes:
                return SubfolderScanResult(name, prefix, 0, 0, 0, "grey")

            blob_names = [b for b, _ in blobs_with_sizes]
            sizes = {b: s for b, s in blobs_with_sizes}
            n = len(blob_names)

            first = blob_names[0]
            _, first_schema = c.read_parquet_footer(first, known_size=sizes.get(first))
            first_done = _check_file(first_schema, self._col_configs)

            if not first_done:
                # Red: at least the first file needs work.
                # Pass all blobs so TransformWorker can start immediately.
                return SubfolderScanResult(
                    name, prefix, n, 0, n, "red",
                    pending_blobs=blob_names,
                    pending_sizes=sizes,
                )

            if n == 1:
                # Only file is done → green.
                return SubfolderScanResult(name, prefix, 1, 1, 0, "green")

            last = blob_names[-1]
            _, last_schema = c.read_parquet_footer(last, known_size=sizes.get(last))
            last_done = _check_file(last_schema, self._col_configs)

            if last_done:
                # Both endpoints done → assume fully green.
                return SubfolderScanResult(name, prefix, n, n, 0, "green")

            # First done, last not done → mixed. Store all blobs for Stage 2.
            return SubfolderScanResult(
                name, prefix, n, -1, -1, "yellow",
                pending_blobs=blob_names,
                pending_sizes=sizes,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=self._scan_workers) as pool:
            futs = {pool.submit(_stage1_one, arg): arg for arg in args}
            for fut in concurrent.futures.as_completed(futs):
                if self._cancel_event.is_set():
                    break
                scanned += 1
                result = fut.result()
                self.progress.emit(scanned, total)
                if result.status == "yellow":
                    yellow_results.append(result)
                else:
                    self.subfolder_scanned.emit(result)

        if self._cancel_event.is_set():
            self.scan_complete.emit()
            return

        # ------------------------------------------------------------------
        # Stage 2: deep scan for yellow (mixed) subfolders
        # ------------------------------------------------------------------
        if yellow_results:
            self.log_message.emit(
                f"[Scan] Stage 2: deep-scanning {len(yellow_results)} mixed subfolder(s)..."
            )

        for r in yellow_results:
            # Cancellation is checked between subfolders, not between individual files
            # within a subfolder's deep scan. For very large subfolders this may delay
            # shutdown by the time needed to scan one subfolder's full file list.
            if self._cancel_event.is_set():
                break

            c = BlobStorageClient(self._conn_str, self._container)
            pending_sizes_snapshot = r.pending_sizes  # snapshot before closure to avoid late-binding

            def _check_one(blob_name: str) -> tuple[str, bool]:
                known = pending_sizes_snapshot.get(blob_name)
                _, schema = c.read_parquet_footer(blob_name, known_size=known)
                return blob_name, _check_file(schema, self._col_configs)

            done_count = 0
            pending_blobs: list[str] = []
            pending_sizes: dict[str, int] = {}

            with concurrent.futures.ThreadPoolExecutor(max_workers=self._scan_workers) as pool:
                for blob_name, is_done in pool.map(_check_one, r.pending_blobs):
                    if is_done:
                        done_count += 1
                    else:
                        pending_blobs.append(blob_name)
                        pending_sizes[blob_name] = pending_sizes_snapshot.get(blob_name, -1)

            final = SubfolderScanResult(
                r.name, r.prefix, r.total,
                done=done_count,
                pending=len(pending_blobs),
                status="yellow",
                pending_blobs=pending_blobs,
                pending_sizes=pending_sizes,
            )
            self.subfolder_scanned.emit(final)

        self.scan_complete.emit()
