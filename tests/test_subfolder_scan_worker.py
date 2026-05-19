"""Unit tests for SubfolderScanWorker helpers — no Azure connection needed."""
import pyarrow as pa
import pytest

from parquet_transform.processor import ColumnConfig
from gui.subfolder_scan_worker import _check_file


def _cfg(name: str, transform: str) -> ColumnConfig:
    return ColumnConfig(name=name, transform=transform)


def test_check_file_all_at_target_returns_true():
    schema = pa.schema([
        pa.field("Id", pa.string()),
        pa.field("CreatedAt", pa.timestamp("ms", tz="UTC")),
    ])
    configs = [_cfg("Id", "binary16_to_uuid"), _cfg("CreatedAt", "timestamp_ns_to_ms_utc")]
    assert _check_file(schema, configs) is True


def test_check_file_one_column_wrong_type_returns_false():
    schema = pa.schema([
        pa.field("Id", pa.large_binary()),   # still binary — not transformed
        pa.field("CreatedAt", pa.timestamp("ms", tz="UTC")),
    ])
    configs = [_cfg("Id", "binary16_to_uuid"), _cfg("CreatedAt", "timestamp_ns_to_ms_utc")]
    assert _check_file(schema, configs) is False


def test_check_file_column_missing_from_schema_is_skipped():
    """A configured column absent from a file is treated as 'nothing to transform here'.

    2025 files don't have Id; once the user configures Id → binary16_to_uuid, those
    older files should still be considered done (not re-processed for a column they
    don't have).
    """
    schema = pa.schema([pa.field("Other", pa.string())])
    configs = [_cfg("Id", "binary16_to_uuid")]
    assert _check_file(schema, configs) is True


def test_check_file_absent_column_with_present_wrong_type_returns_false():
    """When one configured column is absent (skip) but another is present and wrong type,
    the file still needs processing."""
    schema = pa.schema([pa.field("CreatedAt", pa.timestamp("ns"))])  # no Id; CreatedAt wrong type
    configs = [_cfg("Id", "binary16_to_uuid"), _cfg("CreatedAt", "timestamp_ns_to_ms_utc")]
    assert _check_file(schema, configs) is False


def test_check_file_absent_column_with_present_correct_type_returns_true():
    """When one configured column is absent (skip) and the present one is already at
    target type, the file is done."""
    schema = pa.schema([pa.field("CreatedAt", pa.timestamp("ms", tz="UTC"))])  # no Id; ts done
    configs = [_cfg("Id", "binary16_to_uuid"), _cfg("CreatedAt", "timestamp_ns_to_ms_utc")]
    assert _check_file(schema, configs) is True


def test_check_file_unknown_transform_returns_false():
    schema = pa.schema([pa.field("Id", pa.string())])
    configs = [_cfg("Id", "nonexistent_transform")]
    assert _check_file(schema, configs) is False


def test_check_file_empty_configs_returns_true():
    """No columns configured → nothing to check → treat file as done."""
    schema = pa.schema([pa.field("Id", pa.string())])
    assert _check_file(schema, []) is True


def test_check_file_fixed_size_binary_not_done():
    """extension<arrow.uuid> / fixed_size_binary[16] is not string → not done."""
    schema = pa.schema([pa.field("Id", pa.binary(16))])
    configs = [_cfg("Id", "binary16_to_uuid")]
    assert _check_file(schema, configs) is False
