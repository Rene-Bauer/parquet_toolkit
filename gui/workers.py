"""
Background QThread workers for long-running Azure + Parquet operations.

All heavy I/O runs off the main thread so the UI stays responsive.
Workers communicate back to the main thread via Qt signals.
"""
from __future__ import annotations

import io
import traceback

import pyarrow as pa
import pyarrow.parquet as pq
from PyQt6.QtCore import QThread, pyqtSignal

from parquet_transform.processor import ColumnConfig, apply_transforms, compute_output_name
from parquet_transform.storage import BlobStorageClient


class SchemaLoaderWorker(QThread):
    """
    Connects to Azure, counts Parquet blobs, and reads the schema from
    the first file (footer only — fast).
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
        except Exception:
            self.error.emit(traceback.format_exc())


class TransformWorker(QThread):
    """
    Processes all Parquet blobs: download → transform → upload.
    Supports cancellation and dry-run mode.
    """

    # (current_index, total, blob_name)
    progress = pyqtSignal(int, int, str)
    # (blob_name, error_traceback)
    file_error = pyqtSignal(str, str)
    # (files_processed, files_failed)
    finished = pyqtSignal(int, int)
    cancelled = pyqtSignal()

    def __init__(
        self,
        connection_string: str,
        container: str,
        prefix: str,
        col_configs: list[ColumnConfig],
        output_prefix: str | None,
        dry_run: bool = False,
        parent=None,
    ):
        super().__init__(parent)
        self._connection_string = connection_string
        self._container = container
        self._prefix = prefix
        self._col_configs = col_configs
        self._output_prefix = output_prefix
        self._dry_run = dry_run
        self._cancel = False

    def cancel(self) -> None:
        """Request cancellation after the current file finishes."""
        self._cancel = True

    def run(self) -> None:
        try:
            client = BlobStorageClient(self._connection_string, self._container)
            blob_names = client.list_blobs(self._prefix)
        except Exception:
            self.file_error.emit("(connection)", traceback.format_exc())
            self.finished.emit(0, 1)
            return

        total = len(blob_names)
        processed = 0
        failed = 0

        for idx, blob_name in enumerate(blob_names):
            if self._cancel:
                self.cancelled.emit()
                return

            self.progress.emit(idx + 1, total, blob_name)

            try:
                raw = client.download_bytes(blob_name)
                buf = io.BytesIO(raw)
                table = pq.read_table(buf)
                table = apply_transforms(table, self._col_configs)

                output_name = compute_output_name(
                    blob_name, self._prefix, self._output_prefix
                )

                if not self._dry_run:
                    out_buf = io.BytesIO()
                    pq.write_table(table, out_buf)
                    client.upload_bytes(output_name, out_buf.getvalue(), overwrite=True)

                processed += 1
            except Exception:
                failed += 1
                self.file_error.emit(blob_name, traceback.format_exc())

        self.finished.emit(processed, failed)
