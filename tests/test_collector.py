# tests/test_collector.py
import datetime
import pyarrow as pa
import pytest
from parquet_transform.collector import (
    filter_table_by_ids,
    build_metadata,
    make_output_blob_name,
    MetadataAccumulator,
    rewrite_with_metadata,
)


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


def test_make_output_blob_name_replaces_spaces_with_underscores():
    name = make_output_blob_name("out/", "SenderUid", ["100413 156412 1.0"])
    assert name == "out/SenderUid_100413_156412_1.0.parquet"


def test_make_output_blob_name_multiple_ids_with_spaces():
    name = make_output_blob_name("out/", "SenderUid", ["100413 156412 1.0", "100412 141978 1.0"])
    assert name == "out/SenderUid_100413_156412_1.0_100412_141978_1.0.parquet"


def test_make_output_blob_name_strips_trailing_newline():
    """Filter values read from Parquet may carry a trailing newline or CR."""
    name = make_output_blob_name("out/", "DeviceUid", ["1800A_10.0.1.111\n"])
    assert name == "out/DeviceUid_1800A_10.0.1.111.parquet"


def test_make_output_blob_name_replaces_colon_and_other_invalid_chars():
    """Colons, pipes, question-marks, etc. are not valid in Azure blob names."""
    name = make_output_blob_name("out/", "DeviceUid", ["device:10.0.1.1", "uid|2"])
    assert name == "out/DeviceUid_device_10.0.1.1_uid_2.parquet"


def test_make_output_blob_name_preserves_dots_hyphens_underscores():
    """Dots, hyphens, and underscores inside IDs must be kept as-is."""
    name = make_output_blob_name("out/", "DeviceUid", ["1800A_10.0.1.111"])
    assert name == "out/DeviceUid_1800A_10.0.1.111.parquet"


def test_make_output_blob_name_long_ids_uses_hash():
    """When joined IDs exceed _MAX_IDS_PART_LEN the name uses a hash stub."""
    from parquet_transform.collector import _MAX_IDS_PART_LEN
    # Build enough IDs to exceed the threshold
    ids = [f"device_{i:04d}_10.0.1.{i % 256}" for i in range(30)]
    name = make_output_blob_name("out/", "DeviceUid", ids)
    # Must be a valid parquet path
    assert name.endswith(".parquet")
    # The ids part must use the hash stub format: Nids_<hex>
    segment = name.split("/")[-1]          # filename only
    ids_part = segment[len("DeviceUid_"):-len(".parquet")]
    assert ids_part.startswith(f"{len(ids)}ids_")
    # Must be well within Azure's 1 024-byte limit
    assert len(name) < 1024


def test_make_output_blob_name_long_ids_hash_is_deterministic():
    """Same set of IDs must always produce the same hash stub."""
    ids = [f"device_{i:04d}" for i in range(30)]
    name1 = make_output_blob_name("out/", "DeviceUid", ids)
    name2 = make_output_blob_name("out/", "DeviceUid", ids)
    assert name1 == name2


def test_make_output_blob_name_long_ids_hash_is_order_independent():
    """Hash stub must be the same regardless of the order IDs are supplied."""
    ids = [f"device_{i:04d}" for i in range(30)]
    import random
    shuffled = ids[:]
    random.shuffle(shuffled)
    assert make_output_blob_name("out/", "DeviceUid", ids) == \
           make_output_blob_name("out/", "DeviceUid", shuffled)


def test_build_metadata_missing_column_raises():
    table = pa.table({"Id": ["a"], "SenderUid": ["uid1"]})  # missing required cols
    with pytest.raises(ValueError, match="missing required columns"):
        build_metadata(table)


def test_filter_table_missing_column_raises():
    with pytest.raises(ValueError, match="not found in table"):
        filter_table_by_ids(_make_table(), "NonExistentCol", ["x"])


def test_make_output_blob_name_empty_ids_raises():
    with pytest.raises(ValueError, match="must not be empty"):
        make_output_blob_name("out/", "SenderUid", [])


def test_make_output_blob_name_part_zero_raises():
    with pytest.raises(ValueError, match="positive integer"):
        make_output_blob_name("out/", "SenderUid", ["uid1"], part=0)


def test_make_output_blob_name_with_part_number():
    name = make_output_blob_name("out/collected", "SenderUid", ["uid1"], part=1)
    assert name == "out/collected/SenderUid_uid1_part_001.parquet"


def test_make_output_blob_name_with_part_number_zero_padded():
    name = make_output_blob_name("out/", "SenderUid", ["uid1"], part=42)
    assert name == "out/SenderUid_uid1_part_042.parquet"


def test_make_output_blob_name_part_none_unchanged():
    """Passing part=None must produce the same result as before (backward compat)."""
    without = make_output_blob_name("out/collected", "SenderUid", ["uid1"])
    with_none = make_output_blob_name("out/collected", "SenderUid", ["uid1"], part=None)
    assert without == with_none == "out/collected/SenderUid_uid1.parquet"


# --- MetadataAccumulator ---

def test_accumulator_single_chunk():
    acc = MetadataAccumulator()
    acc.update(_make_table())
    meta = acc.to_metadata()
    assert meta["recordCount"] == "4"
    assert meta["dateFrom"] == "04/14/2026 23:45:08 +00:00"
    assert meta["dateTo"] == "04/14/2026 23:47:00 +00:00"
    assert "uid1 dev1 1.0" in meta["deviceIds"]
    assert meta["batchNumber"] == "1"


def test_accumulator_multiple_chunks_combines_correctly():
    acc = MetadataAccumulator()
    table = _make_table()
    acc.update(table.slice(0, 2))   # rows 0-1: uid1, uid2
    acc.update(table.slice(2, 2))   # rows 2-3: uid1, uid3
    meta = acc.to_metadata()
    assert meta["recordCount"] == "4"
    assert meta["dateFrom"] == "04/14/2026 23:45:08 +00:00"
    assert meta["dateTo"] == "04/14/2026 23:47:00 +00:00"


def test_accumulator_date_range_spans_chunks():
    acc = MetadataAccumulator()
    table = _make_table()
    acc.update(table.slice(1, 2))   # rows 1-2 (middle timestamps)
    acc.update(table.slice(0, 1))   # row 0 (earliest)
    acc.update(table.slice(3, 1))   # row 3 (latest)
    meta = acc.to_metadata()
    assert meta["dateFrom"] == "04/14/2026 23:45:08 +00:00"
    assert meta["dateTo"] == "04/14/2026 23:47:00 +00:00"


def test_accumulator_empty_raises():
    acc = MetadataAccumulator()
    with pytest.raises(ValueError, match="No rows"):
        acc.to_metadata()


def test_accumulator_total_rows_property():
    acc = MetadataAccumulator()
    assert acc.total_rows == 0
    acc.update(_make_table())
    assert acc.total_rows == 4


def test_accumulator_update_matches_reference_output():
    """Vectorised update() must produce the same deviceIds as the loop-based version."""
    table = _make_table()  # 4 rows, 4 distinct triples

    acc = MetadataAccumulator()
    acc.update(table)
    meta = acc.to_metadata()

    # Reference: compute expected triples the old slow way
    expected_triples = sorted({
        f"{s} {d} {v}"
        for s, d, v in zip(
            table.column("SenderUid").to_pylist(),
            table.column("DeviceUid").to_pylist(),
            table.column("MessageVersion").to_pylist(),
        )
    })
    assert meta["deviceIds"] == ",".join(expected_triples)


def test_accumulator_update_ignores_null_triples():
    """Rows where any of SenderUid/DeviceUid/MessageVersion is null must not
    cause None to enter the triples set (which would crash to_metadata's sort)."""
    ts = _ts_ms(2026, 4, 14, 23, 45, 8)
    table = pa.table({
        "Id": ["a", "b"],
        "SenderUid": pa.array(["uid1", None], type=pa.string()),
        "DeviceUid": pa.array(["dev1", "dev1"], type=pa.string()),
        "DeviceType": ["T", "T"],
        "DeviceManufacturer": ["M", "M"],
        "TsCreate": pa.array([ts, ts], type=pa.timestamp("ms", tz="UTC")),
        "MessageVersion": pa.array(["1.0", "1.0"], type=pa.string()),
        "MessageType": ["X", "X"],
        "Payload": ["p1", "p2"],
    })
    acc = MetadataAccumulator()
    acc.update(table)
    meta = acc.to_metadata()   # must not raise TypeError
    assert "uid1 dev1 1.0" in meta["deviceIds"]
    assert "None" not in meta["deviceIds"]


def test_accumulator_null_ts_create_does_not_crash():
    """Chunk where TsCreate is entirely null (schema-filled) must not crash
    and must still count rows. A subsequent chunk with real timestamps
    must produce the correct date range."""
    ts_real = _ts_ms(2026, 4, 14, 23, 45, 8)

    # chunk 1: TsCreate all-null (schema-filled for an old file)
    old_chunk = pa.table({
        "Id": ["a", "b"],
        "SenderUid": pa.array(["uid1", "uid1"], type=pa.string()),
        "DeviceUid": pa.array(["dev1", "dev1"], type=pa.string()),
        "DeviceType": ["T", "T"],
        "DeviceManufacturer": ["M", "M"],
        "TsCreate": pa.array([None, None], type=pa.timestamp("ms", tz="UTC")),
        "MessageVersion": pa.array([None, None], type=pa.string()),
        "MessageType": ["X", "X"],
        "Payload": ["p1", "p2"],
    })

    # chunk 2: fully populated
    new_chunk = pa.table({
        "Id": ["c"],
        "SenderUid": pa.array(["uid2"], type=pa.string()),
        "DeviceUid": pa.array(["dev2"], type=pa.string()),
        "DeviceType": ["T"],
        "DeviceManufacturer": ["M"],
        "TsCreate": pa.array([ts_real], type=pa.timestamp("ms", tz="UTC")),
        "MessageVersion": pa.array(["1.0"], type=pa.string()),
        "MessageType": ["X"],
        "Payload": ["p3"],
    })

    acc = MetadataAccumulator()
    acc.update(old_chunk)   # must not raise
    acc.update(new_chunk)
    meta = acc.to_metadata()

    assert meta["recordCount"] == "3"
    # timestamp range comes entirely from the second chunk
    assert meta["dateFrom"] == "04/14/2026 23:45:08 +00:00"
    assert meta["dateTo"] == "04/14/2026 23:45:08 +00:00"
    # null MessageVersion rows are excluded from triples; uid2 row is present
    assert "uid2 dev2 1.0" in meta["deviceIds"]
    assert "None" not in meta["deviceIds"]


def test_accumulator_all_null_ts_falls_back_to_epoch():
    """When every chunk has null TsCreate, to_metadata must not crash and
    must use the epoch sentinel for dateFrom/dateTo."""
    chunk = pa.table({
        "Id": ["a"],
        "SenderUid": pa.array(["uid1"], type=pa.string()),
        "DeviceUid": pa.array(["dev1"], type=pa.string()),
        "DeviceType": ["T"],
        "DeviceManufacturer": ["M"],
        "TsCreate": pa.array([None], type=pa.timestamp("ms", tz="UTC")),
        "MessageVersion": pa.array(["1.0"], type=pa.string()),
        "MessageType": ["X"],
        "Payload": ["p1"],
    })
    acc = MetadataAccumulator()
    acc.update(chunk)
    meta = acc.to_metadata()   # must not raise

    assert meta["recordCount"] == "1"
    assert meta["dateFrom"] == "01/01/1970 00:00:00 +00:00"
    assert meta["dateTo"] == "01/01/1970 00:00:00 +00:00"


# --- rewrite_with_metadata ---

def test_rewrite_with_metadata_produces_valid_parquet(tmp_path):
    import pyarrow.parquet as pq

    src = tmp_path / "src.parquet"
    dst = tmp_path / "dst.parquet"
    pq.write_table(_make_table(), str(src))

    rewrite_with_metadata(str(src), str(dst), {"recordCount": "4", "batchNumber": "1"})

    result = pq.read_table(str(dst))
    assert result.num_rows == 4
    meta = result.schema.metadata
    assert meta[b"recordCount"] == b"4"


def test_rewrite_with_metadata_preserves_rows(tmp_path):
    import pyarrow.parquet as pq

    src = tmp_path / "src.parquet"
    dst = tmp_path / "dst.parquet"
    original = _make_table()
    pq.write_table(original, str(src))

    rewrite_with_metadata(str(src), str(dst), {"recordCount": "4"})

    result = pq.read_table(str(dst))
    assert result["SenderUid"].to_pylist() == original["SenderUid"].to_pylist()


def test_rewrite_preserves_existing_metadata_and_overwrites(tmp_path):
    import pyarrow.parquet as pq

    src = tmp_path / "src.parquet"
    dst = tmp_path / "dst.parquet"
    table = _make_table().replace_schema_metadata({"existingKey": "existingVal", "recordCount": "old"})
    pq.write_table(table, str(src))

    rewrite_with_metadata(str(src), str(dst), {"recordCount": "4"})

    result = pq.read_table(str(dst))
    meta = {k.decode(): v.decode() for k, v in result.schema.metadata.items()}
    assert meta["existingKey"] == "existingVal"   # preserved
    assert meta["recordCount"] == "4"             # overwritten
