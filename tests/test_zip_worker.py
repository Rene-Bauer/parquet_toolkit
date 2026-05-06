# tests/test_zip_worker.py
"""Integration tests for ZipConverterWorker."""
import io
import sys
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from unittest.mock import MagicMock, patch

from PyQt6.QtWidgets import QApplication
_app = QApplication.instance() or QApplication(sys.argv)


def _make_zip_bytes(*csv_pairs: tuple[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in csv_pairs:
            zf.writestr(name, content)
    return buf.getvalue()


def _run_worker(worker):
    results = {}
    worker.finished.connect(lambda d: results.update(d))
    worker.run()
    return results


def _make_worker(**kwargs):
    from gui.workers import ZipConverterWorker
    defaults = dict(
        connection_string="fake",
        container="c",
        source_prefix="zips/",
        output_prefix="out/",
        max_workers=2,
        autoscale=False,
    )
    defaults.update(kwargs)
    return ZipConverterWorker(**defaults)


def _configure_verify_passthrough(mock_client) -> dict:
    """Make upload verification always pass (captures uploaded Parquet for inspection)."""
    uploaded = {}

    def _capturing_upload(blob_name, file_path, **kwargs):
        try:
            meta = pq.read_metadata(file_path)
            uploaded[blob_name] = (meta.num_rows, meta.schema.to_arrow_schema())
        except Exception:
            pass

    mock_client.upload_stream.side_effect = _capturing_upload
    mock_client.read_parquet_footer.side_effect = (
        lambda blob: uploaded.get(blob, (0, pa.schema([])))
    )
    return uploaded


def test_worker_converts_zip_to_parquet():
    csv_data = "id,val\n1,a\n2,b\n3,c\n"
    zip_bytes = _make_zip_bytes(("data.csv", csv_data))

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = []
    mock_client.list_zip_blobs_with_sizes.return_value = [("zips/data.zip", len(zip_bytes))]
    mock_client.download_bytes.return_value = zip_bytes
    _configure_verify_passthrough(mock_client)

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        results = _run_worker(_make_worker())

    assert results["rowCount"] == 3
    assert mock_client.upload_stream.call_count == 1


def test_worker_output_blob_name_replaces_zip_with_parquet():
    zip_bytes = _make_zip_bytes(("d.csv", "id\n1\n"))

    mock_client = MagicMock()
    mock_client.list_zip_blobs_with_sizes.return_value = [("zips/sub/file.zip", 100)]
    mock_client.download_bytes.return_value = zip_bytes
    _configure_verify_passthrough(mock_client)

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        _run_worker(_make_worker())

    uploaded_blob = mock_client.upload_stream.call_args[0][0]
    assert uploaded_blob == "out/sub/file.parquet"


def test_worker_empty_zip_emits_file_error():
    """ZIP containing no CSV files is reported as a per-file error."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w"):
        pass
    empty_zip = buf.getvalue()

    mock_client = MagicMock()
    mock_client.list_zip_blobs_with_sizes.return_value = [("zips/empty.zip", len(empty_zip))]
    mock_client.download_bytes.return_value = empty_zip

    errors = []
    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = _make_worker()
        worker.file_error.connect(lambda b, e: errors.append(b))
        _run_worker(worker)

    assert "zips/empty.zip" in errors
    assert mock_client.upload_stream.call_count == 0


def test_worker_download_failure_routes_to_file_error():
    mock_client = MagicMock()
    mock_client.list_zip_blobs_with_sizes.return_value = [("zips/f.zip", 100)]
    mock_client.download_bytes.side_effect = ConnectionError("network down")

    errors = []
    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = _make_worker(max_attempts=1)
        worker.file_error.connect(lambda b, e: errors.append(b))
        _run_worker(worker)

    assert "zips/f.zip" in errors


def test_worker_emits_cancelled_when_cancel_called():
    zip_bytes = _make_zip_bytes(("d.csv", "id\n1\n"))
    mock_client = MagicMock()
    mock_client.list_zip_blobs_with_sizes.return_value = [("zips/f.zip", 100)]
    mock_client.download_bytes.return_value = zip_bytes

    cancelled = []
    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = _make_worker()
        worker.cancelled.connect(lambda: cancelled.append(True))
        worker.cancel()
        worker.run()

    assert cancelled == [True]
