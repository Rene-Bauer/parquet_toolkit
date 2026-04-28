"""Tests for parquet_transform.storage — _extract_blob_size (no Azure connection needed)."""
import pytest

from parquet_transform.storage import _extract_blob_size, BlobStorageClient


class _BlobWithSize:
    def __init__(self, size):
        self.size = size


class _BlobWithPropertiesSize:
    class _Props:
        def __init__(self, size):
            self.size = size
    def __init__(self, size):
        self.properties = self._Props(size)


class _BlobWithPropertiesContentLength:
    class _Props:
        def __init__(self, length):
            self.content_length = length
    def __init__(self, length):
        self.properties = self._Props(length)


class _BlobWithNoUsableAttribute:
    pass


class _BlobWithNoneSize:
    size = None


class _BlobWithNegativeSize:
    size = -5


def test_upload_stream_opens_file_and_calls_upload_blob(tmp_path):
    """upload_stream reads from the file path and passes a file handle, not bytes."""
    from unittest.mock import MagicMock

    src = tmp_path / "test.parquet"
    src.write_bytes(b"FAKE_PARQUET_DATA")

    mock_blob_client = MagicMock()
    mock_container = MagicMock()
    mock_container.get_blob_client.return_value = mock_blob_client

    from parquet_transform.storage import BlobStorageClient
    client = BlobStorageClient.__new__(BlobStorageClient)
    client._container = mock_container

    client.upload_stream("output/test.parquet", str(src))

    mock_container.get_blob_client.assert_called_once_with("output/test.parquet")
    mock_blob_client.upload_blob.assert_called_once()
    # First positional arg must be a file-like object (not bytes)
    first_arg = mock_blob_client.upload_blob.call_args[0][0]
    assert hasattr(first_arg, "read"), "upload_blob must receive a file handle, not bytes"
    kwargs = mock_blob_client.upload_blob.call_args[1]
    assert kwargs.get("overwrite") is True
    assert kwargs.get("timeout") == 600
    assert kwargs.get("max_concurrency") == 4


class TestExtractBlobSize:
    def test_reads_size_attribute(self):
        assert _extract_blob_size(_BlobWithSize(1024)) == 1024

    def test_reads_properties_size(self):
        assert _extract_blob_size(_BlobWithPropertiesSize(512)) == 512

    def test_reads_properties_content_length(self):
        assert _extract_blob_size(_BlobWithPropertiesContentLength(256)) == 256

    def test_returns_minus_one_when_no_attribute_found(self):
        assert _extract_blob_size(_BlobWithNoUsableAttribute()) == -1

    def test_returns_minus_one_when_size_is_none(self):
        assert _extract_blob_size(_BlobWithNoneSize()) == -1

    def test_zero_size_is_valid(self):
        assert _extract_blob_size(_BlobWithSize(0)) == 0

    def test_prefers_size_attribute_over_properties(self):
        """When both .size and .properties.size exist, .size takes priority."""
        class _Both:
            size = 100
            class properties:
                size = 999
        assert _extract_blob_size(_Both()) == 100

    def test_negative_size_falls_through_to_next_candidate(self):
        """A negative .size should not be returned; fall back to next candidate."""
        assert _extract_blob_size(_BlobWithNegativeSize()) == -1


def test_list_blob_prefixes_returns_sorted_subfolder_names():
    from unittest.mock import MagicMock, patch

    mock_prefix_a = MagicMock()
    mock_prefix_a.name = "archive/26-02-2026/"
    mock_prefix_b = MagicMock()
    mock_prefix_b.name = "archive/26-03-2026/"
    mock_blob = MagicMock()
    mock_blob.name = "archive/loose_file.parquet"  # a blob, not a prefix — must be ignored

    mock_container = MagicMock()
    mock_container.walk_blobs.return_value = [mock_prefix_b, mock_blob, mock_prefix_a]

    with patch("parquet_transform.storage.BlobServiceClient") as mock_svc:
        mock_svc.from_connection_string.return_value.get_container_client.return_value = mock_container
        client = BlobStorageClient("fake_conn", "mycontainer")
        result = client.list_blob_prefixes("archive/")

    assert result == ["26-02-2026", "26-03-2026"]
    mock_container.walk_blobs.assert_called_once_with(
        name_starts_with="archive/", delimiter="/"
    )


def test_list_blob_prefixes_empty_when_no_subfolders():
    from unittest.mock import MagicMock, patch

    mock_blob = MagicMock()
    mock_blob.name = "archive/somefile.parquet"

    mock_container = MagicMock()
    mock_container.walk_blobs.return_value = [mock_blob]

    with patch("parquet_transform.storage.BlobServiceClient") as mock_svc:
        mock_svc.from_connection_string.return_value.get_container_client.return_value = mock_container
        client = BlobStorageClient("fake_conn", "mycontainer")
        result = client.list_blob_prefixes("archive/")

    assert result == []


def test_list_blob_prefixes_strips_trailing_slash_from_input():
    """Passing prefix without trailing slash must behave identically."""
    from unittest.mock import MagicMock, patch

    mock_prefix = MagicMock()
    mock_prefix.name = "archive/26-02-2026/"

    mock_container = MagicMock()
    mock_container.walk_blobs.return_value = [mock_prefix]

    with patch("parquet_transform.storage.BlobServiceClient") as mock_svc:
        mock_svc.from_connection_string.return_value.get_container_client.return_value = mock_container
        client = BlobStorageClient("fake_conn", "mycontainer")
        result = client.list_blob_prefixes("archive")  # no trailing slash

    assert result == ["26-02-2026"]


# ---------------------------------------------------------------------------
# delete_blob
# ---------------------------------------------------------------------------

def test_delete_blob_calls_sdk():
    from unittest.mock import MagicMock

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
