# tests/test_collector_worker.py
import io
import sys
import queue
import datetime
import threading
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from unittest.mock import MagicMock, patch, call

from PyQt6.QtWidgets import QApplication
_app = QApplication.instance() or QApplication(sys.argv)


def _ts_ms(year, month, day, hour, minute, second) -> int:
    dt = datetime.datetime(year, month, day, hour, minute, second,
                           tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1000)


def _make_parquet_bytes(sender_uid: str, device_uid: str, n: int = 3) -> bytes:
    table = pa.table({
        "Id": [f"id{i}" for i in range(n)],
        "SenderUid": [sender_uid] * n,
        "DeviceUid": [device_uid] * n,
        "DeviceType": ["T"] * n,
        "DeviceManufacturer": ["M"] * n,
        "TsCreate": pa.array(
            [_ts_ms(2026, 4, 14, 23, 45, 8) + i * 1000 for i in range(n)],
            type=pa.timestamp("ms", tz="UTC"),
        ),
        "MessageVersion": ["1.0"] * n,
        "MessageType": ["X"] * n,
        "Payload": [f"p{i}" for i in range(n)],
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _make_thread_safe_download_mock(*blob_bytes_list):
    """Return a side_effect function that is safe to call from multiple threads."""
    q = queue.Queue()
    for b in blob_bytes_list:
        q.put(b)
    def _side_effect(blob_name, timeout=None):
        return q.get(timeout=5.0)
    return _side_effect


def _run_worker(worker):
    results = {}
    worker.finished.connect(lambda d: results.update(d))
    worker.run()
    return results


def _make_worker(**kwargs):
    from gui.workers import DataCollectorWorker
    defaults = dict(
        connection_string="fake",
        container="c",
        source_prefix="prefix/",
        output_prefix="out/",
        filter_col="SenderUid",
        filter_values=["uid1"],
        max_workers=2,
        ram_limit_mb=512,
        autoscale=False,
    )
    defaults.update(kwargs)
    return DataCollectorWorker(**defaults)


# --- basic functionality ---

def test_worker_combines_rows_from_multiple_blobs():
    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet", "p/f2.parquet"]
    mock_client.download_bytes.side_effect = _make_thread_safe_download_mock(
        _make_parquet_bytes("uid1", "dev1", 2),
        _make_parquet_bytes("uid1", "dev1", 3),
    )

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        results = _run_worker(_make_worker())

    assert results["rowCount"] == 5
    assert mock_client.upload_bytes.call_count == 1


def test_worker_output_blob_name_is_correct():
    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 2)

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        _run_worker(_make_worker())

    blob_name = mock_client.upload_bytes.call_args[0][0]
    assert blob_name == "out/SenderUid_uid1.parquet"


def test_worker_filters_only_matching_rows():
    mixed_table = pa.concat_tables([
        pq.read_table(io.BytesIO(_make_parquet_bytes("uid1", "dev1", 2))),
        pq.read_table(io.BytesIO(_make_parquet_bytes("uid2", "dev1", 3))),
    ])
    buf = io.BytesIO()
    pq.write_table(mixed_table, buf)

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet"]
    mock_client.download_bytes.return_value = buf.getvalue()

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        results = _run_worker(_make_worker())

    assert results["rowCount"] == 2


def test_worker_no_match_skips_upload():
    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid_other", "dev1", 3)

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        results = _run_worker(_make_worker())

    assert results["rowCount"] == 0
    assert mock_client.upload_bytes.call_count == 0


def test_worker_emits_cancelled_when_cancel_called():
    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet", "p/f2.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 1)

    cancelled = []
    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = _make_worker()
        worker.cancelled.connect(lambda: cancelled.append(True))
        worker.cancel()
        worker.run()

    assert cancelled == [True]
    assert mock_client.upload_bytes.call_count == 0


def test_worker_emits_file_error_on_bad_blob_and_continues():
    good_bytes = _make_parquet_bytes("uid1", "dev1", 3)
    call_count = [0]
    lock = threading.Lock()

    def _side_effect(blob_name, timeout=None):
        with lock:
            call_count[0] += 1
            n = call_count[0]
        if n == 1:
            raise Exception("simulated download failure")
        return good_bytes

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/bad.parquet", "p/good.parquet"]
    mock_client.download_bytes.side_effect = _side_effect

    errors = []
    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = _make_worker()
        worker.file_error.connect(lambda b, e: errors.append(b))
        results = _run_worker(worker)

    assert len(errors) == 1
    assert errors[0] == "p/bad.parquet"
    assert results["rowCount"] == 3
    assert mock_client.upload_bytes.call_count == 1


# --- uploaded file has correct metadata ---

def test_worker_output_has_correct_metadata():
    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 2)

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        results = _run_worker(_make_worker())

    assert results["rowCount"] == 2
    uploaded_bytes = mock_client.upload_bytes.call_args[0][1]
    result_table = pq.read_table(io.BytesIO(uploaded_bytes))
    meta = {k.decode(): v.decode() for k, v in result_table.schema.metadata.items()}
    assert meta["recordCount"] == "2"
    assert "+00:00" in meta["dateFrom"]
    assert "+00:00" in meta["dateTo"]


# --- separate output container ---

def test_worker_uses_separate_output_container():
    source_mock = MagicMock()
    source_mock.list_blobs.return_value = ["p/f1.parquet"]
    source_mock.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 2)

    output_mock = MagicMock()

    def _client_factory(conn, container):
        if container == "source-container":
            return source_mock
        return output_mock

    with patch("gui.workers.BlobStorageClient", side_effect=_client_factory):
        worker = _make_worker(container="source-container", output_container="output-container")
        _run_worker(worker)

    assert output_mock.upload_bytes.call_count == 1
    assert source_mock.upload_bytes.call_count == 0
