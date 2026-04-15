"""
Azure Blob Storage client wrapper.

Provides list, download, and upload operations against a single container.
All Parquet I/O uses in-memory BytesIO buffers — Parquet requires random
access and cannot be streamed sequentially.
"""
from __future__ import annotations

import io
import time

import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient, ContainerClient


def _extract_blob_size(blob) -> int:
    """
    Extract the byte size from an Azure BlobProperties / BlobItem object.

    Different azure-storage-blob versions and storage configurations (ADLS Gen2,
    page blobs, append blobs …) expose the size under different attribute paths.
    We try the most common ones in order and return -1 when none yields a
    positive integer — the caller treats -1 as "size unknown".
    """
    candidates = (
        # Standard attribute on BlobProperties (azure-storage-blob 12.x)
        lambda b: b.size,
        # Nested path used by some SDK versions / BlobItem wrappers
        lambda b: b.properties.size,
        lambda b: b.properties.content_length,
    )
    for getter in candidates:
        try:
            raw = getter(blob)
            if raw is None:
                continue
            value = int(raw)
            if value >= 0:
                return value
        except (AttributeError, TypeError, ValueError):
            continue
    return -1  # sentinel: size unknown


class BlobStorageClient:
    def __init__(self, connection_string: str, container: str) -> None:
        service: BlobServiceClient = BlobServiceClient.from_connection_string(
            connection_string
        )
        self._container: ContainerClient = service.get_container_client(container)

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------

    def list_blobs(self, prefix: str) -> list[str]:
        """
        Return all blob names under *prefix* that end with '.parquet'.
        The prefix is matched as-is by the Azure API (no glob expansion).
        """
        blobs = self._container.list_blobs(name_starts_with=prefix)
        return [b.name for b in blobs if b.name.endswith(".parquet")]

    def list_blobs_with_sizes(self, prefix: str) -> list[tuple[str, int]]:
        """
        Return (blob_name, size_bytes) for all .parquet blobs under *prefix*.
        Size information is included in the same API response at no extra cost.

        Uses -1 as a sentinel when the size cannot be determined (see
        _extract_blob_size for the full fallback chain).  Callers must treat
        -1 as "size unknown" and exclude those blobs from any total-size sum.
        """
        blobs = self._container.list_blobs(name_starts_with=prefix)
        return [
            (b.name, _extract_blob_size(b))
            for b in blobs
            if b.name.endswith(".parquet")
        ]

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def download_bytes(self, blob_name: str, timeout: int = 120) -> bytes:
        """Download a blob to memory and return its raw bytes.

        Enforces a hard wall-clock deadline of ``timeout`` seconds for the
        *entire* download, not just the per-chunk connection setup.  This
        prevents degraded (but non-failing) Azure connections from holding a
        worker thread for hundreds of seconds when the network is saturated.

        The same ``timeout`` value is passed to the Azure SDK for per-request
        (per-chunk) timeouts; the wall-clock deadline catches the cumulative
        case where many small chunks each succeed within the per-chunk limit
        but the total time balloons past acceptable bounds.
        """
        deadline = time.monotonic() + timeout
        blob_client = self._container.get_blob_client(blob_name)
        downloader = blob_client.download_blob(timeout=timeout)
        try:
            chunks: list[bytes] = []
            for chunk in downloader.chunks():
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        f"Download of {blob_name!r} exceeded {timeout}s "
                        f"total wall-clock timeout"
                    )
                chunks.append(chunk)
            return b"".join(chunks)
        except Exception:
            # Silence the SDK's "Incomplete download." print() in __del__:
            # the StorageStreamDownloader prints to stdout when garbage-collected
            # with _download_complete=False. We mark it done here so the message
            # doesn't appear for expected network errors (which are already
            # logged and retried by our own error handling).
            try:
                downloader._download_complete = True
            except Exception:
                pass
            raise

    def read_schema(self, blob_name: str) -> pa.Schema:
        """
        Read only the Parquet schema (footer) of a blob without loading
        the full column data. Suitable for schema inspection before processing.
        """
        raw = self.download_bytes(blob_name)
        buf = io.BytesIO(raw)
        return pq.read_schema(buf)

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    def upload_bytes(
        self,
        blob_name: str,
        data: bytes,
        overwrite: bool = True,
        timeout: int = 300,
    ) -> None:
        """Upload raw bytes to a blob, overwriting by default."""
        blob_client = self._container.get_blob_client(blob_name)
        blob_client.upload_blob(data, overwrite=overwrite, timeout=timeout)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying Azure SDK connection pool."""
        try:
            self._container.close()
        except Exception:
            pass
