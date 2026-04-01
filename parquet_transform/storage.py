"""
Azure Blob Storage client wrapper.

Provides list, download, and upload operations against a single container.
All Parquet I/O uses in-memory BytesIO buffers — Parquet requires random
access and cannot be streamed sequentially.
"""
from __future__ import annotations

import io
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient, ContainerClient


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

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def download_bytes(self, blob_name: str, timeout: int = 60) -> bytes:
        """Download a blob to memory and return its raw bytes."""
        blob_client = self._container.get_blob_client(blob_name)
        downloader = blob_client.download_blob(timeout=timeout)
        return downloader.readall()

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
        timeout: int = 60,
    ) -> None:
        """Upload raw bytes to a blob, overwriting by default."""
        blob_client = self._container.get_blob_client(blob_name)
        blob_client.upload_blob(data, overwrite=overwrite, timeout=timeout)
