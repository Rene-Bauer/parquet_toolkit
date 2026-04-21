# tests/test_collector.py
import datetime
import pyarrow as pa
import pytest
from parquet_transform.collector import filter_table_by_ids, build_metadata, make_output_blob_name


def _ts_ms(year, month, day, hour, minute, second) -> int:
    dt = datetime.datetime(year, month, day, hour, minute, second,
                           tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1000)


def _make_table() -> pa.Table:
    return pa.table({
        "Id": ["a", "b", "c", "d"],
        "SenderUid": ["uid1", "uid2", "uid1", "uid3"],
        "DeviceUid": ["dev1", "dev1", "dev2", "dev2"],
        "DeviceType": ["T", "T", "T", "T"],
        "DeviceManufacturer": ["M", "M", "M", "M"],
        "TsCreate": pa.array([
            _ts_ms(2026, 4, 14, 23, 45,  8),
            _ts_ms(2026, 4, 14, 23, 46,  0),
            _ts_ms(2026, 4, 14, 23, 46, 44),
            _ts_ms(2026, 4, 14, 23, 47,  0),
        ], type=pa.timestamp("ms", tz="UTC")),
        "MessageVersion": ["1.0", "1.0", "1.0", "1.0"],
        "MessageType": ["X", "X", "X", "X"],
        "Payload": ["p1", "p2", "p3", "p4"],
    })


# --- filter_table_by_ids ---

def test_filter_single_sender_uid():
    result = filter_table_by_ids(_make_table(), "SenderUid", ["uid1"])
    assert result.num_rows == 2
    assert set(result["SenderUid"].to_pylist()) == {"uid1"}


def test_filter_multiple_sender_uids():
    result = filter_table_by_ids(_make_table(), "SenderUid", ["uid1", "uid3"])
    assert result.num_rows == 3
    assert set(result["SenderUid"].to_pylist()) == {"uid1", "uid3"}


def test_filter_device_uid():
    result = filter_table_by_ids(_make_table(), "DeviceUid", ["dev2"])
    assert result.num_rows == 2
    assert set(result["DeviceUid"].to_pylist()) == {"dev2"}


def test_filter_no_match_returns_empty():
    result = filter_table_by_ids(_make_table(), "SenderUid", ["no_such"])
    assert result.num_rows == 0


def test_filter_preserves_schema():
    original = _make_table()
    result = filter_table_by_ids(original, "SenderUid", ["uid1"])
    assert result.schema.names == original.schema.names


# --- build_metadata ---

def test_build_metadata_record_count():
    meta = build_metadata(_make_table())
    assert meta["recordCount"] == "4"


def test_build_metadata_date_from_is_minimum():
    meta = build_metadata(_make_table())
    assert meta["dateFrom"] == "04/14/2026 23:45:08 +00:00"


def test_build_metadata_date_to_is_maximum():
    meta = build_metadata(_make_table())
    assert meta["dateTo"] == "04/14/2026 23:47:00 +00:00"


def test_build_metadata_device_ids_contains_all_triples():
    meta = build_metadata(_make_table())
    device_ids = meta["deviceIds"]
    assert "uid1 dev1 1.0" in device_ids
    assert "uid1 dev2 1.0" in device_ids
    assert "uid2 dev1 1.0" in device_ids
    assert "uid3 dev2 1.0" in device_ids


def test_build_metadata_batch_number():
    meta = build_metadata(_make_table())
    assert meta["batchNumber"] == "1"


def test_build_metadata_empty_table_raises():
    empty = _make_table().slice(0, 0)
    with pytest.raises(ValueError, match="empty"):
        build_metadata(empty)


# --- make_output_blob_name ---

def test_make_output_blob_name_single_id():
    name = make_output_blob_name("out/collected", "SenderUid", ["uid1"])
    assert name == "out/collected/SenderUid_uid1.parquet"


def test_make_output_blob_name_multiple_ids():
    name = make_output_blob_name("out/collected", "DeviceUid", ["dev1", "dev2"])
    assert name == "out/collected/DeviceUid_dev1_dev2.parquet"


def test_make_output_blob_name_strips_trailing_slash():
    name = make_output_blob_name("out/collected/", "SenderUid", ["uid1"])
    assert name == "out/collected/SenderUid_uid1.parquet"
