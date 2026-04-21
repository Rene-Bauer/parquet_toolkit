# tests/test_collector_worker.py
import io
import sys
import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from unittest.mock import MagicMock, patch

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


def _run_worker(worker):
    """Run worker synchronously (bypass QThread.start) and collect finished signal."""
    results = {}
    worker.finished.connect(lambda d: results.update(d))
    worker.run()
    return results


def test_worker_combines_rows_from_multiple_blobs():
    from gui.workers import DataCollectorWorker

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["prefix/f1.parquet", "prefix/f2.parquet"]
    mock_client.download_bytes.side_effect = [
        _make_parquet_bytes("uid1", "dev1", 2),
        _make_parquet_bytes("uid1", "dev1", 3),
    ]

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = DataCollectorWorker(
            connection_string="fake",
            container="c",
            source_prefix="prefix/",
            output_prefix="out/",
            filter_col="SenderUid",
            filter_values=["uid1"],
        )
        results = _run_worker(worker)

    assert results["rowCount"] == 5
    assert mock_client.upload_bytes.call_count == 1


def test_worker_output_blob_name_is_correct():
    from gui.workers import DataCollectorWorker

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["prefix/f1.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 2)

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = DataCollectorWorker(
            connection_string="fake",
            container="c",
            source_prefix="prefix/",
            output_prefix="out/",
            filter_col="SenderUid",
            filter_values=["uid1"],
        )
        _run_worker(worker)

    call_blob_name = mock_client.upload_bytes.call_args[0][0]
    assert call_blob_name == "out/SenderUid_uid1.parquet"


def test_worker_filters_only_matching_rows():
    from gui.workers import DataCollectorWorker

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["prefix/f1.parquet"]
    # File contains uid1 (2 rows) and uid2 (3 rows)
    table = pa.concat_tables([
        pq.read_table(io.BytesIO(_make_parquet_bytes("uid1", "dev1", 2))),
        pq.read_table(io.BytesIO(_make_parquet_bytes("uid2", "dev1", 3))),
    ])
    buf = io.BytesIO()
    pq.write_table(table, buf)
    mock_client.download_bytes.return_value = buf.getvalue()

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = DataCollectorWorker(
            connection_string="fake",
            container="c",
            source_prefix="prefix/",
            output_prefix="out/",
            filter_col="SenderUid",
            filter_values=["uid1"],
        )
        results = _run_worker(worker)

    assert results["rowCount"] == 2


def test_worker_emits_cancelled_when_cancel_called():
    from gui.workers import DataCollectorWorker

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["prefix/f1.parquet", "prefix/f2.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 1)

    cancelled = []
    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = DataCollectorWorker(
            connection_string="fake",
            container="c",
            source_prefix="prefix/",
            output_prefix="out/",
            filter_col="SenderUid",
            filter_values=["uid1"],
        )
        worker.cancelled.connect(lambda: cancelled.append(True))
        worker.cancel()
        worker.run()

    assert cancelled == [True]
    assert mock_client.upload_bytes.call_count == 0


def test_worker_no_match_skips_upload():
    from gui.workers import DataCollectorWorker

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["prefix/f1.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid2", "dev1", 3)

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = DataCollectorWorker(
            connection_string="fake",
            container="c",
            source_prefix="prefix/",
            output_prefix="out/",
            filter_col="SenderUid",
            filter_values=["uid1"],
        )
        results = _run_worker(worker)

    assert results["rowCount"] == 0
    assert mock_client.upload_bytes.call_count == 0


def test_worker_emits_file_error_on_bad_blob_and_continues():
    from gui.workers import DataCollectorWorker

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["prefix/bad.parquet", "prefix/good.parquet"]
    mock_client.download_bytes.side_effect = [
        Exception("simulated download failure"),
        _make_parquet_bytes("uid1", "dev1", 3),
    ]

    errors = []
    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = DataCollectorWorker(
            connection_string="fake",
            container="c",
            source_prefix="prefix/",
            output_prefix="out/",
            filter_col="SenderUid",
            filter_values=["uid1"],
        )
        worker.file_error.connect(lambda blob, err: errors.append((blob, err)))
        results = _run_worker(worker)

    # Bad blob emits file_error but run continues; good blob rows are collected
    assert len(errors) == 1
    assert errors[0][0] == "prefix/bad.parquet"
    assert results["rowCount"] == 3
    assert mock_client.upload_bytes.call_count == 1
