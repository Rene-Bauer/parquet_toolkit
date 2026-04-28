"""
Azure Blob Storage client wrapper.

Provides list, download, and upload operations against a single container.
Downloads use in-memory BytesIO buffers (Parquet requires random access).
Uploads support both in-memory bytes (upload_bytes) and streaming from
disk (upload_stream — no full-file RAM load).
"""
from __future__ import annotations

import io
import time

import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient, ContainerClient

try:
    from azure.core.exceptions import ResourceNotFoundError as _ResourceNotFoundError
except ImportError:  # pragma: no cover
    _ResourceNotFoundError = Exception  # type: ignore[assignment,misc]


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

    def list_blob_prefixes(self, prefix: str) -> list[str]:
        """Return direct virtual-subdirectory names under *prefix* (sorted).

        Uses Azure's delimiter-based listing to enumerate virtual directories
        without downloading any blob content.  Returns an empty list if the
        prefix contains no subdirectories (i.e. blobs are flat).

        Example: prefix="archive/", subfolders "26-02-2026" and "26-03-2026"
        returns ["26-02-2026", "26-03-2026"].
        """
        norm = (prefix.rstrip("/") + "/") if prefix.strip("/") else ""
        names: list[str] = []
        for item in self._container.walk_blobs(name_starts_with=norm, delimiter="/"):
            item_name: str = item.name
            # BlobPrefix objects have names ending with "/" and represent
            # virtual directories; BlobItem objects represent actual blobs.
            if item_name.endswith("/") and item_name != norm:
                subfolder = item_name[len(norm):].rstrip("/")
                if subfolder:
                    names.append(subfolder)
        return sorted(names)

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

    def read_parquet_footer(self, blob_name: str) -> tuple[int, pa.Schema]:
        """Read row count and Arrow schema from a blob's Parquet footer.

        Uses two Azure range-download requests (total: footer_size + 8 bytes),
        so this is efficient regardless of the blob's full size.

        Raises RuntimeError if the blob is not a valid Parquet file or if
        the footer cannot be parsed.
        """
        blob_client = self._container.get_blob_client(blob_name)
        size: int = blob_client.get_blob_properties().size
        if size < 8:
            raise RuntimeError(
                f"Blob {blob_name!r} is too small to be a Parquet file ({size} bytes)"
            )

        # Parquet trailer: [4-byte footer_length LE int32][4-byte magic "PAR1"]
        tail = blob_client.download_blob(offset=size - 8, length=8).readall()
        magic = tail[4:]
        if magic != b"PAR1":
            raise RuntimeError(
                f"Blob {blob_name!r} is not a valid Parquet file "
                f"(expected magic b'PAR1', got {magic!r})"
            )

        footer_length = int.from_bytes(tail[:4], "little")
        if footer_length <= 0 or footer_length > size - 8:
            raise RuntimeError(
                f"Blob {blob_name!r}: invalid footer length {footer_length}"
            )

        footer_offset = size - 8 - footer_length
        footer_bytes = blob_client.download_blob(
            offset=footer_offset, length=footer_length + 8
        ).readall()

        buf = io.BytesIO(footer_bytes)
        try:
            metadata = pq.read_metadata(buf)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to parse Parquet footer of {blob_name!r}: {exc}"
            ) from exc

        return metadata.num_rows, metadata.schema.to_arrow_schema()

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

    def upload_stream(
        self,
        blob_name: str,
        file_path: str,
        overwrite: bool = True,
        timeout: int = 600,
        max_concurrency: int = 4,
    ) -> None:
        """Upload a local file to a blob by streaming directly from disk.

        Unlike upload_bytes, this never loads the full file into RAM.
        max_concurrency controls the number of parallel Azure block uploads
        for large files (Azure SDK default is 1).
        """
        blob_client = self._container.get_blob_client(blob_name)
        with open(file_path, "rb") as fh:
            blob_client.upload_blob(
                fh,
                overwrite=overwrite,
                timeout=timeout,
                max_concurrency=max_concurrency,
            )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def delete_blob(self, blob_name: str) -> None:
        """Delete a blob. Silently ignores 404 (blob already gone)."""
        blob_client = self._container.get_blob_client(blob_name)
        try:
            blob_client.delete_blob()
        except _ResourceNotFoundError:
            pass

    def close(self) -> None:
        """Close the underlying Azure SDK connection pool."""
        try:
            self._container.close()
        except Exception:
            pass
