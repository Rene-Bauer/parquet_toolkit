# Upload-Robustheit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two-step upload safety to the Data Collector: (1) delete a partially-written output blob on upload failure, and (2) verify row-count and schema after every successful upload, retrying once on mismatch.

**Architecture:** Two sequential tasks. Task 1 adds two storage primitives (`delete_blob`, `read_parquet_footer`) to `BlobStorageClient` with full test coverage. Task 2 wires three new module-level helpers into `DataCollectorWorker._flush_and_upload()` — the single place that calls `upload_stream`. No new signals, no new UI. Each task commits independently.

**Tech Stack:** Python 3.10+, pyarrow, azure-storage-blob, pytest

---

## File Map

| File | Change |
|---|---|
| `parquet_transform/storage.py` | Add `delete_blob()`, `read_parquet_footer()` |
| `gui/workers.py` | Add `_try_delete_blob()`, `_attempt_upload_verify()`, `_upload_verify_with_retry()`; modify `_flush_and_upload()` inner function |
| `tests/test_storage.py` | Add tests for new storage methods |
| `tests/test_collector_worker.py` | Add tests for upload+verify flow |

---

## Task 1: Storage primitives — `delete_blob` and `read_parquet_footer`

**Files:**
- Modify: `parquet_transform/storage.py`
- Modify: `tests/test_storage.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_storage.py`:

```python
# ---------------------------------------------------------------------------
# delete_blob
# ---------------------------------------------------------------------------

def test_delete_blob_calls_sdk():
    from unittest.mock import MagicMock, patch

    mock_blob_client = MagicMock()
    mock_container = MagicMock()
    mock_container.get_blob_client.return_value = mock_blob_client

    client = BlobStorageClient.__new__(BlobStorageClient)
    client._container = mock_container

    client.delete_blob("some/path.parquet")

    mock_container.get_blob_client.assert_called_once_with("some/path.parquet")
    mock_blob_client.delete_blob.assert_called_once()


def test_delete_blob_swallows_resource_not_found():
    """delete_blob must not raise when the blob does not exist."""
    from unittest.mock import MagicMock
    try:
        from azure.core.exceptions import ResourceNotFoundError
    except ImportError:
        pytest.skip("azure-core not installed")

    mock_blob_client = MagicMock()
    mock_blob_client.delete_blob.side_effect = ResourceNotFoundError("not found")
    mock_container = MagicMock()
    mock_container.get_blob_client.return_value = mock_blob_client

    client = BlobStorageClient.__new__(BlobStorageClient)
    client._container = mock_container

    client.delete_blob("nonexistent.parquet")  # must not raise


# ---------------------------------------------------------------------------
# read_parquet_footer
# ---------------------------------------------------------------------------

def _make_parquet_bytes_storage(n: int = 3) -> bytes:
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pa.table({"x": pa.array(list(range(n)), type=pa.int32()),
                      "y": pa.array(["a"] * n, type=pa.string())})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _make_range_mock(data: bytes):
    """Return a mock container whose get_blob_client supports range downloads."""
    from unittest.mock import MagicMock

    mock_blob_client = MagicMock()
    mock_props = MagicMock()
    mock_props.size = len(data)
    mock_blob_client.get_blob_properties.return_value = mock_props

    def fake_download_blob(offset, length):
        chunk = data[offset: offset + length]
        downloader = MagicMock()
        downloader.readall.return_value = chunk
        return downloader

    mock_blob_client.download_blob.side_effect = fake_download_blob

    mock_container = MagicMock()
    mock_container.get_blob_client.return_value = mock_blob_client
    return mock_container


def test_read_parquet_footer_returns_correct_row_count_and_schema():
    import pyarrow as pa

    data = _make_parquet_bytes_storage(n=7)

    client = BlobStorageClient.__new__(BlobStorageClient)
    client._container = _make_range_mock(data)

    num_rows, schema = client.read_parquet_footer("test.parquet")

    assert num_rows == 7
    expected = pa.schema([("x", pa.int32()), ("y", pa.string())])
    assert schema.equals(expected, check_metadata=False)


def test_read_parquet_footer_raises_on_bad_magic():
    """Blob whose last 4 bytes are not PAR1 → RuntimeError."""
    bad_data = b"\x00" * 100 + b"NOPE"

    client = BlobStorageClient.__new__(BlobStorageClient)
    client._container = _make_range_mock(bad_data)

    with pytest.raises(RuntimeError, match="PAR1"):
        client.read_parquet_footer("bad.parquet")


def test_read_parquet_footer_raises_when_blob_too_small():
    """Blob smaller than 8 bytes → RuntimeError."""
    client = BlobStorageClient.__new__(BlobStorageClient)
    client._container = _make_range_mock(b"tiny")

    with pytest.raises(RuntimeError, match="too small"):
        client.read_parquet_footer("tiny.parquet")
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_storage.py::test_delete_blob_calls_sdk tests/test_storage.py::test_read_parquet_footer_returns_correct_row_count_and_schema -v
```

Expected: FAIL — `AttributeError: 'BlobStorageClient' object has no attribute 'delete_blob'`

- [ ] **Step 3: Add `delete_blob` to `parquet_transform/storage.py`**

Add to the import block at the top (after the existing azure import line):

```python
from azure.storage.blob import BlobServiceClient, ContainerClient
```

Change to:

```python
from azure.storage.blob import BlobServiceClient, ContainerClient

try:
    from azure.core.exceptions import ResourceNotFoundError as _ResourceNotFoundError
except ImportError:  # pragma: no cover
    _ResourceNotFoundError = Exception  # type: ignore[assignment,misc]
```

Then add `delete_blob` at the bottom of the `# Lifecycle` section, before `close()`:

```python
    def delete_blob(self, blob_name: str) -> None:
        """Delete a blob. Silently ignores 404 (blob already gone)."""
        blob_client = self._container.get_blob_client(blob_name)
        try:
            blob_client.delete_blob()
        except _ResourceNotFoundError:
            pass
```

- [ ] **Step 4: Add `read_parquet_footer` to `parquet_transform/storage.py`**

Add to the `# Download` section, directly after `read_schema`:

```python
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
```

- [ ] **Step 5: Run all storage tests**

```bash
pytest tests/test_storage.py -v
```

Expected: all tests PASS.

- [ ] **Step 6: Run full suite**

```bash
pytest -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add parquet_transform/storage.py tests/test_storage.py
git commit -m "feat: add BlobStorageClient.delete_blob and read_parquet_footer"
```

---

## Task 2: Wire upload verification into DataCollectorWorker

**Files:**
- Modify: `gui/workers.py`
- Modify: `tests/test_collector_worker.py`

### Background for implementer

`DataCollectorWorker.run()` contains a closure `_flush_and_upload()` (around line 1230) that:
1. Calls `rewrite_with_metadata(current_tmp, rw_tmp, final_meta)` to write final Parquet with merged metadata
2. Calls `output_client.upload_stream(out_name, rw_tmp, timeout=UPLOAD_TIMEOUT_S)`
3. Appends `out_name` to `part_names`

The closure has access to `part_acc_ref[0]` (a `MetadataAccumulator`) and `writer_schema[0]` (the `pa.Schema` of the written chunks). Both are needed for verification.

The three new helpers go at **module level** in `workers.py`, near the existing helpers (`_format_bytes`, `_summarize_exception`, etc.) before the class definitions. They are not closures.

- [ ] **Step 1: Write failing tests**

Append to `tests/test_collector_worker.py`:

```python
# ---------------------------------------------------------------------------
# upload verify helpers
# ---------------------------------------------------------------------------

def _make_storage_client_mock(parquet_bytes: bytes) -> "MagicMock":
    """Return a mock BlobStorageClient that verifies against the given parquet_bytes."""
    import io
    import pyarrow.parquet as pq

    meta = pq.read_metadata(io.BytesIO(parquet_bytes))
    actual_rows = meta.num_rows
    actual_schema = meta.schema.to_arrow_schema()

    mock = MagicMock()
    mock.read_parquet_footer.return_value = (actual_rows, actual_schema)
    return mock


def test_upload_verify_success_no_retry():
    """On matching row count + schema, upload succeeds in one attempt."""
    from gui.workers import _upload_verify_with_retry
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({"x": pa.array([1, 2, 3], type=pa.int32())})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    parquet_bytes = buf.getvalue()

    client = _make_storage_client_mock(parquet_bytes)
    schema = table.schema

    # Should not raise
    _upload_verify_with_retry(client, "out/result.parquet", "/fake/path.parquet", 3, schema)

    assert client.upload_stream.call_count == 1
    assert client.delete_blob.call_count == 0


def test_upload_verify_row_count_mismatch_retries_then_succeeds():
    """First verify returns wrong row count → delete + retry → second verify ok."""
    from gui.workers import _upload_verify_with_retry
    import pyarrow as pa

    schema = pa.schema([("x", pa.int32())])
    client = MagicMock()
    # First verify: wrong count; second verify: correct
    client.read_parquet_footer.side_effect = [
        (99, schema),   # mismatch
        (3, schema),    # correct after retry
    ]

    _upload_verify_with_retry(client, "out/result.parquet", "/fake/path.parquet", 3, schema)

    assert client.upload_stream.call_count == 2
    assert client.delete_blob.call_count == 1  # cleanup before retry


def test_upload_verify_both_attempts_fail_raises():
    """Two consecutive row-count mismatches → RuntimeError, blob cleaned up twice."""
    from gui.workers import _upload_verify_with_retry
    import pyarrow as pa
    import pytest

    schema = pa.schema([("x", pa.int32())])
    client = MagicMock()
    client.read_parquet_footer.return_value = (99, schema)  # always wrong

    with pytest.raises(RuntimeError, match="verification failed"):
        _upload_verify_with_retry(client, "out/result.parquet", "/fake/path.parquet", 3, schema)

    assert client.upload_stream.call_count == 2
    assert client.delete_blob.call_count == 2


def test_upload_failure_triggers_delete_then_retry_succeeds():
    """Upload raises on first attempt → delete → retry succeeds → no further delete."""
    from gui.workers import _upload_verify_with_retry
    import pyarrow as pa

    schema = pa.schema([("x", pa.int32())])
    client = MagicMock()
    client.upload_stream.side_effect = [OSError("network error"), None]  # fail then succeed
    client.read_parquet_footer.return_value = (3, schema)

    _upload_verify_with_retry(client, "out/result.parquet", "/fake/path.parquet", 3, schema)

    assert client.upload_stream.call_count == 2
    assert client.delete_blob.call_count == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_collector_worker.py::test_upload_verify_success_no_retry tests/test_collector_worker.py::test_upload_verify_row_count_mismatch_retries_then_succeeds -v
```

Expected: FAIL — `ImportError: cannot import name '_upload_verify_with_retry' from 'gui.workers'`

- [ ] **Step 3: Add three module-level helpers to `gui/workers.py`**

Add these three functions at module level, directly after the existing helper functions (`_format_bytes`, `_format_duration`, `_summarize_exception`, `_first_line`) and before the first class definition. Search for the block ending with `def _first_line(...)` and insert after it.

```python
def _try_delete_blob(client: "BlobStorageClient", blob_name: str) -> None:
    """Delete blob silently — used for best-effort cleanup after upload failure."""
    try:
        client.delete_blob(blob_name)
    except Exception:
        pass


def _attempt_upload_verify(
    client: "BlobStorageClient",
    blob_name: str,
    file_path: str,
    expected_rows: int,
    expected_schema: pa.Schema,
    timeout: int,
) -> Exception | None:
    """Single upload + verify cycle. Returns None on success, Exception on any failure.

    Cleans up the blob (best-effort delete) before returning a failure so the
    caller can retry with a clean slate.
    """
    try:
        client.upload_stream(blob_name, file_path, timeout=timeout)
    except Exception as exc:
        _try_delete_blob(client, blob_name)
        return exc

    try:
        actual_rows, actual_schema = client.read_parquet_footer(blob_name)
    except Exception as exc:
        _try_delete_blob(client, blob_name)
        return exc

    if actual_rows != expected_rows:
        _try_delete_blob(client, blob_name)
        return RuntimeError(
            f"Upload verification failed for {blob_name!r}: "
            f"expected {expected_rows} rows, got {actual_rows}"
        )

    if not actual_schema.equals(expected_schema, check_metadata=False):
        _try_delete_blob(client, blob_name)
        return RuntimeError(
            f"Upload verification failed for {blob_name!r}: schema mismatch — "
            f"expected {expected_schema}, got {actual_schema}"
        )

    return None


def _upload_verify_with_retry(
    client: "BlobStorageClient",
    blob_name: str,
    file_path: str,
    expected_rows: int,
    expected_schema: pa.Schema,
    timeout: int = UPLOAD_TIMEOUT_S,
) -> None:
    """Upload file_path to blob_name and verify row count + schema.

    Retries once on any failure (upload error or verification mismatch).
    Deletes the blob before each retry so the next attempt starts clean.
    Raises RuntimeError if both attempts fail.
    """
    last_exc: Exception | None = None
    for _ in range(2):
        last_exc = _attempt_upload_verify(
            client, blob_name, file_path, expected_rows, expected_schema, timeout
        )
        if last_exc is None:
            return
    raise RuntimeError(
        f"Upload of {blob_name!r} failed after 2 attempts: {last_exc}"
    ) from last_exc
```

- [ ] **Step 4: Run tests to verify helpers pass**

```bash
pytest tests/test_collector_worker.py::test_upload_verify_success_no_retry tests/test_collector_worker.py::test_upload_verify_row_count_mismatch_retries_then_succeeds tests/test_collector_worker.py::test_upload_verify_both_attempts_fail_raises tests/test_collector_worker.py::test_upload_failure_triggers_delete_then_retry_succeeds -v
```

Expected: all four PASS.

- [ ] **Step 5: Wire `_upload_verify_with_retry` into `_flush_and_upload()` in `gui/workers.py`**

Inside `DataCollectorWorker.run()`, locate `_flush_and_upload()`. Find this exact block:

```python
                    rewrite_with_metadata(current_tmp, rw_tmp, final_meta)
                    output_client.upload_stream(
                        out_name, rw_tmp,
                        timeout=UPLOAD_TIMEOUT_S,
                    )
                    part_names.append(out_name)
```

Replace with:

```python
                    rewrite_with_metadata(current_tmp, rw_tmp, final_meta)
                    _upload_verify_with_retry(
                        output_client,
                        out_name,
                        rw_tmp,
                        expected_rows=part_acc_ref[0].total_rows,
                        expected_schema=writer_schema[0],
                    )
                    part_names.append(out_name)
```

- [ ] **Step 6: Run full test suite**

```bash
pytest -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add gui/workers.py tests/test_collector_worker.py
git commit -m "feat: verify row count and schema after each collector upload, retry once on mismatch"
```

---

## Self-Review

**Spec coverage:**
- ✅ #9 Teilupload-Cleanup (Data Collector only): `_try_delete_blob` called on every failure path in `_attempt_upload_verify`; blob deleted before retry and before raising
- ✅ #7 Post-Upload-Verifikation (Data Collector only, always active): row-count + schema checked after every `upload_stream` call; mismatch treated as failure
- ✅ Verification uses efficient range download (footer only, not full blob)
- ✅ One retry on any failure (upload error OR verification mismatch)
- ✅ Both attempts fail → RuntimeError propagates up through `_flush_and_upload` → caught by existing `except Exception` in `_writer_loop` → `self._cancel_reason = "writer_error"` → UI shows error
- ✅ Schema comparison uses `check_metadata=False` (ignores footer metadata added by `rewrite_with_metadata`)
- ✅ `delete_blob` silences `ResourceNotFoundError` (blob may already be gone)

**Placeholder scan:** None.

**Type consistency:**
- `_upload_verify_with_retry` signature matches all four call sites in tests ✅
- `read_parquet_footer` returns `tuple[int, pa.Schema]`; `_attempt_upload_verify` unpacks as `actual_rows, actual_schema` ✅
- `part_acc_ref[0].total_rows` is `int`; `writer_schema[0]` is `pa.Schema | None` — `None` only before first write, and `_flush_and_upload` returns early when `total_rows == 0` (line 1232), so `writer_schema[0]` is always set by the time verify runs ✅
