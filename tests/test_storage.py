"""Tests for parquet_transform.storage — _extract_blob_size (no Azure connection needed)."""
import pytest

from parquet_transform.storage import _extract_blob_size


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
