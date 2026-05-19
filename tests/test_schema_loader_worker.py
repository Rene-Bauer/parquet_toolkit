"""Tests for SchemaLoaderWorker — parallel listing + subfolder schema sampling."""
import collections
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from gui.workers import SchemaLoaderWorker, _merge_schemas


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client_mock(
    blobs_with_sizes,
    schema: pa.Schema | None = None,
    subfolder_names: list[str] | None = None,
):
    """Return a BlobStorageClient mock for SchemaLoaderWorker.

    - list_blobs_with_sizes  → blobs_with_sizes
    - list_blob_prefixes     → subfolder_names (default: [] = flat prefix)
    - list_first_parquet_blob → first blob name (or None)
    - read_parquet_footer    → (0, schema)
    """
    mock = MagicMock()
    mock.list_blobs_with_sizes.return_value = blobs_with_sizes
    mock.list_blob_prefixes.return_value = subfolder_names or []
    if schema is not None:
        mock.read_parquet_footer.return_value = (0, schema)
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
# Tests: flat prefix (no subfolders) — existing behaviour preserved
# ---------------------------------------------------------------------------

def test_schema_loader_emits_schema_and_blob_count():
    schema = pa.schema([pa.field("x", pa.int32()), pa.field("y", pa.string())])
    blobs = [("data/a.parquet", 1024), ("data/b.parquet", 2048)]

    c_list = _make_client_mock(blobs)
    c_schema = _make_client_mock(blobs, schema=schema)   # list_blob_prefixes → []
    clients = [c_list, c_schema]

    with patch("gui.workers.BlobStorageClient", side_effect=clients):
        worker = SchemaLoaderWorker("conn", "container", "data/")
        results = _run_worker(worker)

    assert "error" not in results
    assert results["count"] == 2
    assert results["total_bytes"] == 3072
    assert results["unknown"] == []
    assert results["schema"].equals(schema, check_metadata=False)


def test_schema_loader_uses_two_clients_for_flat_prefix():
    """Two BlobStorageClient instances created for a flat prefix (no subfolders)."""
    schema = pa.schema([pa.field("z", pa.float64())])
    blobs = [("pre/c.parquet", 512)]

    c_list = _make_client_mock(blobs)
    c_schema = _make_client_mock(blobs, schema=schema)
    clients = [c_list, c_schema]

    with patch("gui.workers.BlobStorageClient", side_effect=clients) as mock_ctor:
        worker = SchemaLoaderWorker("conn", "container", "pre/")
        _run_worker(worker)

    assert mock_ctor.call_count == 2


def test_schema_loader_closes_both_clients_on_success():
    schema = pa.schema([pa.field("v", pa.bool_())])
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


def test_schema_loader_separates_known_and_unknown_sizes():
    schema = pa.schema([pa.field("k", pa.int16())])
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
# Tests: subfolder-based sampling (the new behaviour)
# ---------------------------------------------------------------------------

def test_schema_loader_merges_schemas_from_subfolders():
    """When subfolders exist, schema is sampled from the first file in each
    subfolder and merged so columns only present in some subfolders appear."""
    # 2025 subfolder: no Id column
    schema_2025 = pa.schema([pa.field("ts", pa.timestamp("ms", tz="UTC")), pa.field("val", pa.int32())])
    # 2026 subfolder: has Id column (unconverted)
    schema_2026 = pa.schema([pa.field("ts", pa.timestamp("ms", tz="UTC")), pa.field("Id", pa.binary(16))])

    blobs = [("livedata/2025/a.parquet", 100), ("livedata/2026/b.parquet", 200)]

    c_list = MagicMock()
    c_list.list_blobs_with_sizes.return_value = blobs

    c_schema = MagicMock()
    c_schema.list_blob_prefixes.return_value = ["2025", "2026"]

    # Per-subfolder clients — deque.popleft() is thread-safe in CPython
    schemas_queue = collections.deque([schema_2025, schema_2026])

    call_idx = [0]

    def client_factory(conn, container):
        i = call_idx[0]
        call_idx[0] += 1
        if i == 0:
            return c_list
        if i == 1:
            return c_schema
        # Subsequent calls are per-subfolder clients created inside _read_one
        m = MagicMock()
        m.list_first_parquet_blob.return_value = "sf/first.parquet"
        m.read_parquet_footer.return_value = (0, schemas_queue.popleft())
        return m

    with patch("gui.workers.BlobStorageClient", side_effect=client_factory):
        worker = SchemaLoaderWorker("conn", "container", "livedata/")
        results = _run_worker(worker)

    assert "error" not in results
    col_names = set(results["schema"].names)
    assert "ts" in col_names
    assert "val" in col_names    # from 2025 subfolder
    assert "Id" in col_names    # from 2026 subfolder — this is what was missing before


def test_schema_loader_emits_error_when_all_subfolder_reads_fail():
    """When every per-subfolder read raises, an error signal is emitted (not a crash)."""
    blobs = [("livedata/2025/a.parquet", 100), ("livedata/2026/b.parquet", 200)]

    c_list = MagicMock()
    c_list.list_blobs_with_sizes.return_value = blobs

    c_schema = MagicMock()
    c_schema.list_blob_prefixes.return_value = ["2025", "2026"]

    call_idx = [0]

    def client_factory(conn, container):
        i = call_idx[0]
        call_idx[0] += 1
        if i == 0:
            return c_list
        if i == 1:
            return c_schema
        # Per-subfolder clients all raise on read_parquet_footer
        m = MagicMock()
        m.list_first_parquet_blob.return_value = "sf/first.parquet"
        m.read_parquet_footer.side_effect = RuntimeError("Azure auth failure")
        return m

    with patch("gui.workers.BlobStorageClient", side_effect=client_factory):
        worker = SchemaLoaderWorker("conn", "container", "livedata/")
        results = _run_worker(worker)

    assert "error" in results


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
