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
    assert mock_client.upload_stream.call_count == 1


def test_worker_output_blob_name_is_correct():
    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 2)

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        _run_worker(_make_worker())

    blob_name = mock_client.upload_stream.call_args[0][0]
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
    assert mock_client.upload_stream.call_count == 0


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
    assert mock_client.upload_stream.call_count == 0


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
    assert mock_client.upload_stream.call_count == 1


# --- uploaded file has correct metadata ---

def test_worker_output_has_correct_metadata():
    """Uploaded file must have Parquet footer metadata (recordCount, dateFrom, dateTo)."""
    import pyarrow.parquet as _pq

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 2)

    captured_tables = []

    def _capture_upload(blob_name, file_path, **kwargs):
        # Read the file while it still exists (before worker deletes it)
        captured_tables.append(_pq.read_table(file_path))

    mock_client.upload_stream.side_effect = _capture_upload

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        results = _run_worker(_make_worker())

    assert results["rowCount"] == 2
    assert len(captured_tables) == 1
    meta = {k.decode(): v.decode() for k, v in captured_tables[0].schema.metadata.items()}
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

    assert output_mock.upload_stream.call_count == 1
    assert source_mock.upload_stream.call_count == 0


def test_selected_columns_stored_on_init():
    """DataCollectorWorker stores selected_columns for use in _producer."""
    import sys
    from PyQt6.QtWidgets import QApplication
    _app = QApplication.instance() or QApplication(sys.argv)

    from gui.workers import DataCollectorWorker
    w = DataCollectorWorker(
        connection_string="",
        container="c",
        source_prefix="",
        output_prefix="out/",
        filter_col="SenderUid",
        filter_values=["uid1"],
        selected_columns=["Id", "SenderUid", "TsCreate"],
    )
    assert w._selected_columns == ["Id", "SenderUid", "TsCreate"]


def test_selected_columns_defaults_to_none():
    import sys
    from PyQt6.QtWidgets import QApplication
    _app = QApplication.instance() or QApplication(sys.argv)

    from gui.workers import DataCollectorWorker
    w = DataCollectorWorker(
        connection_string="",
        container="c",
        source_prefix="",
        output_prefix="out/",
        filter_col="SenderUid",
        filter_values=["uid1"],
    )
    assert w._selected_columns is None


# --- pause / resume ---

def test_pause_event_starts_set():
    """Worker starts in unpaused state."""
    w = _make_worker()
    assert w._pause_event.is_set()


def test_pause_clears_event_and_resume_sets_it():
    w = _make_worker()
    w.pause()
    assert not w._pause_event.is_set()
    w.resume()
    assert w._pause_event.is_set()


def test_cancel_unblocks_paused_worker():
    """cancel() must set the pause event so blocked producers can exit."""
    w = _make_worker()
    w.pause()
    assert not w._pause_event.is_set()
    w.cancel()
    assert w._pause_event.is_set()


def test_pause_resume_signals_emitted():
    """pause() emits paused; resume() emits resumed."""
    w = _make_worker()
    paused = []
    resumed = []
    w.paused.connect(lambda: paused.append(True))
    w.resumed.connect(lambda: resumed.append(True))
    w.pause()
    w.resume()
    assert paused == [True]
    assert resumed == [True]


def test_paused_worker_stops_emitting_progress():
    """Producers should block on _pause_event.wait() and stop reporting progress."""
    import threading
    import time
    from unittest.mock import patch, MagicMock

    BLOB_SLEEP = 0.1   # slow enough that blobs are clearly in-flight or blocked

    # Track download start times to verify blocking: if the worker is truly
    # paused, no new downloads should begin during the pause window.
    download_times = []
    download_lock = threading.Lock()

    def _slow_download(blob_name, timeout=None):
        with download_lock:
            download_times.append(time.monotonic())
        time.sleep(BLOB_SLEEP)
        return _make_parquet_bytes("uid1", "dev1", 1)

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = [f"p/f{i}.parquet" for i in range(8)]
    mock_client.download_bytes.side_effect = _slow_download

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        worker = _make_worker(max_workers=2)

        t = threading.Thread(target=worker.run, daemon=True)
        t.start()

        # Let at least one blob start downloading
        time.sleep(BLOB_SLEEP * 1.5)
        pause_time = time.monotonic()
        worker.pause()

        # Allow any blobs whose download had already started before pause() to
        # finish (they are past the _pause_event.wait() gate).  Then take a
        # snapshot: no *new* download should have begun after pause_time.
        time.sleep(BLOB_SLEEP * 2.5)
        with download_lock:
            snapshot = list(download_times)

        # Hold the pause for another full download window to confirm nothing
        # new starts.
        time.sleep(BLOB_SLEEP * 2.0)
        with download_lock:
            final_during_pause = list(download_times)

        worker.resume()
        t.join(timeout=5.0)

    with download_lock:
        all_times = list(download_times)

    # Must have started at least one download before pausing
    downloads_before_pause = [ts for ts in all_times if ts < pause_time]
    assert len(downloads_before_pause) > 0, "no downloads started before pause"

    # No new download should start after the drain window ends (snapshot == final_during_pause)
    assert snapshot == final_during_pause, (
        f"new downloads started during pause: {len(final_during_pause) - len(snapshot)}"
    )

    # After resume the worker must finish all 8 blobs
    assert len(all_times) == 8, f"only {len(all_times)} of 8 blobs downloaded"


def test_predicate_pushdown_calls_read_table_with_filters():
    """_producer must use pq.read_table filters= instead of post-read Python filter."""
    import pyarrow.parquet as pq
    from unittest.mock import patch, MagicMock

    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 2)

    read_table_calls = []
    _orig_read_table = pq.read_table

    def _spy_read_table(source, **kwargs):
        read_table_calls.append(kwargs)
        return _orig_read_table(source, **kwargs)

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        with patch("gui.workers.pq.read_table", side_effect=_spy_read_table):
            results = _run_worker(_make_worker())

    assert results["rowCount"] == 2
    assert len(read_table_calls) >= 1
    producer_calls = [c for c in read_table_calls if "filters" in c]
    assert len(producer_calls) >= 1, "pq.read_table in _producer must pass filters="
    call_kwargs = producer_calls[0]
    flt = call_kwargs["filters"]
    assert any("SenderUid" in str(f) for f in flt), f"Expected SenderUid filter, got {flt}"
    assert "columns" in call_kwargs, "pq.read_table in _producer must pass columns="


# --- max_output_bytes ---

def test_max_output_bytes_defaults_to_zero():
    w = _make_worker()
    assert w._max_output_bytes == 0


def test_max_output_bytes_stored_on_init():
    w = _make_worker(max_output_bytes=5 * 1024 ** 3)
    assert w._max_output_bytes == 5 * 1024 ** 3


def test_writer_splits_output_when_max_output_bytes_exceeded():
    """With a tiny max_output_bytes, each blob produces a separate _part_NNN upload."""
    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet", "p/f2.parquet"]
    mock_client.download_bytes.side_effect = _make_thread_safe_download_mock(
        _make_parquet_bytes("uid1", "dev1", 5),
        _make_parquet_bytes("uid1", "dev1", 5),
    )

    upload_calls = []

    def _capture_upload(blob_name, file_path, **kwargs):
        upload_calls.append(blob_name)

    mock_client.upload_stream.side_effect = _capture_upload

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        # max_output_bytes=1 forces a flush after every write
        results = _run_worker(_make_worker(max_output_bytes=1))

    assert results["rowCount"] == 10
    # At least 2 part files uploaded
    assert len(upload_calls) >= 2
    # All uploads must use _part_NNN naming
    for name in upload_calls:
        assert "_part_" in name, f"expected _part_ in blob name, got {name!r}"
    # Parts are numbered sequentially from 001
    assert any("_part_001" in n for n in upload_calls)
    assert any("_part_002" in n for n in upload_calls)
    # result includes outputBlobs list
    assert "outputBlobs" in results
    assert set(results["outputBlobs"]) == set(upload_calls)


def test_writer_no_splitting_when_max_output_bytes_zero():
    """Default (max_output_bytes=0) produces a single upload without _part_ suffix."""
    mock_client = MagicMock()
    mock_client.list_blobs.return_value = ["p/f1.parquet"]
    mock_client.download_bytes.return_value = _make_parquet_bytes("uid1", "dev1", 3)

    upload_calls = []
    def _capture(blob_name, file_path, **kwargs):
        upload_calls.append(blob_name)
    mock_client.upload_stream.side_effect = _capture

    with patch("gui.workers.BlobStorageClient", return_value=mock_client):
        results = _run_worker(_make_worker())

    assert results["rowCount"] == 3
    assert len(upload_calls) == 1
    assert "_part_" not in upload_calls[0]
    assert "outputBlobs" in results
    assert results["outputBlobs"] == upload_calls
