"""Tests for SchemaLoaderWorker — parallel listing + schema read."""
import io
from unittest.mock import MagicMock, call, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gui.workers import SchemaLoaderWorker, _merge_schemas


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_schema_bytes() -> bytes:
    table = pa.table({"a": pa.array([1, 2], type=pa.int64())})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _make_client_mock(blobs_with_sizes, schema: pa.Schema | None = None):
    """Return a BlobStorageClient mock configured for SchemaLoaderWorker."""
    mock = MagicMock()
    mock.list_blobs_with_sizes.return_value = blobs_with_sizes
    if schema is not None:
        mock.read_schema.return_value = schema
    mock.list_first_parquet_blob.return_value = (
        blobs_with_sizes[0][0] if blobs_with_sizes else None
    )
    return mock


def _run_worker(worker: SchemaLoaderWorker):
    """Run worker synchronously and collect emitted signals."""
    results = {}
    worker.schema_loaded.connect(
        lambda schema, n, total, unknown: results.update(
            schema=schema, count=n, total_bytes=total, unknown=unknown
        )
    )
    worker.error.connect(lambda msg: results.update(error=msg))
    worker.run()
    return results


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_schema_loader_emits_schema_and_blob_count():
    schema = pa.schema([("x", pa.int32()), ("y", pa.string())])
    blobs = [("data/a.parquet", 1024), ("data/b.parquet", 2048)]

    c_list = _make_client_mock(blobs)
    c_schema = _make_client_mock(blobs, schema=schema)
    clients = [c_list, c_schema]

    with patch("gui.workers.BlobStorageClient", side_effect=clients):
        worker = SchemaLoaderWorker("conn", "container", "data/")
        results = _run_worker(worker)

    assert "error" not in results
    assert results["count"] == 2
    assert results["total_bytes"] == 3072
    assert results["unknown"] == []
    assert results["schema"].equals(schema, check_metadata=False)


def test_schema_loader_uses_two_clients():
    """Two BlobStorageClient instances must be created (one per I/O thread)."""
    schema = pa.schema([("z", pa.float64())])
    blobs = [("pre/c.parquet", 512)]

    c_list = _make_client_mock(blobs)
    c_schema = _make_client_mock(blobs, schema=schema)
    clients = [c_list, c_schema]

    with patch("gui.workers.BlobStorageClient", side_effect=clients) as mock_ctor:
        worker = SchemaLoaderWorker("conn", "container", "pre/")
        _run_worker(worker)

    assert mock_ctor.call_count == 2


def test_schema_loader_closes_both_clients_on_success():
    schema = pa.schema([("v", pa.bool_())])
    blobs = [("p/f.parquet", 100)]

    c_list = _make_client_mock(blobs)
    c_schema = _make_client_mock(blobs, schema=schema)
    clients = [c_list, c_schema]

    with patch("gui.workers.BlobStorageClient", side_effect=clients):
        worker = SchemaLoaderWorker("conn", "container", "p/")
        _run_worker(worker)

    c_list.close.assert_called_once()
    c_schema.close.assert_called_once()


def test_schema_loader_closes_both_clients_on_error():
    """Both clients must be closed even when listing raises."""
    c_list = MagicMock()
    c_list.list_blobs_with_sizes.side_effect = RuntimeError("network error")
    c_schema = MagicMock()
    clients = [c_list, c_schema]

    with patch("gui.workers.BlobStorageClient", side_effect=clients):
        worker = SchemaLoaderWorker("conn", "container", "p/")
        results = _run_worker(worker)

    assert "error" in results
    c_list.close.assert_called_once()
    c_schema.close.assert_called_once()


def test_schema_loader_emits_error_when_no_blobs():
    blobs = []
    c_list = _make_client_mock(blobs)
    c_schema = _make_client_mock(blobs)
    c_schema.list_first_parquet_blob.return_value = None
    clients = [c_list, c_schema]

    with patch("gui.workers.BlobStorageClient", side_effect=clients):
        worker = SchemaLoaderWorker("conn", "container", "empty/")
        results = _run_worker(worker)

    assert "error" in results
    assert "schema_loaded" not in results or "schema" not in results


def test_schema_loader_separates_known_and_unknown_sizes():
    schema = pa.schema([("k", pa.int16())])
    blobs = [
        ("d/a.parquet", 500),
        ("d/b.parquet", -1),   # unknown size
        ("d/c.parquet", 300),
    ]
    c_list = _make_client_mock(blobs)
    c_schema = _make_client_mock(blobs, schema=schema)
    c_schema.list_first_parquet_blob.return_value = "d/a.parquet"
    clients = [c_list, c_schema]

    with patch("gui.workers.BlobStorageClient", side_effect=clients):
        worker = SchemaLoaderWorker("conn", "container", "d/")
        results = _run_worker(worker)

    assert "error" not in results
    assert results["count"] == 3
    assert results["total_bytes"] == 800  # 500 + 300 only
    assert results["unknown"] == ["d/b.parquet"]


# ---------------------------------------------------------------------------
# _merge_schemas
# ---------------------------------------------------------------------------

def test_merge_schemas_single_schema_returned_unchanged():
    schema = pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.string())])
    result = _merge_schemas([schema])
    assert result.equals(schema, check_metadata=False)


def test_merge_schemas_unions_fields_across_schemas():
    s1 = pa.schema([pa.field("ts", pa.timestamp("ms", tz="UTC")), pa.field("val", pa.int32())])
    s2 = pa.schema([pa.field("ts", pa.timestamp("ms", tz="UTC")), pa.field("Id", pa.binary(16))])
    result = _merge_schemas([s1, s2])
    assert set(result.names) == {"ts", "val", "Id"}


def test_merge_schemas_first_seen_type_wins_for_duplicate_names():
    s1 = pa.schema([pa.field("x", pa.int32())])
    s2 = pa.schema([pa.field("x", pa.string())])   # different type for same name
    result = _merge_schemas([s1, s2])
    assert result.field("x").type == pa.int32()    # s1 wins


def test_merge_schemas_empty_list_raises_value_error():
    with pytest.raises(ValueError, match="at least one"):
        _merge_schemas([])
