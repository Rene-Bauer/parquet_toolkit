"""Tests for parquet_transform.transforms — registry and built-in transforms."""
import uuid

import pyarrow as pa
import pytest

import parquet_transform.transforms as tr


# ---------------------------------------------------------------------------
# Helper: build an extension<arrow.uuid> type and array
# ---------------------------------------------------------------------------

class _ArrowUUIDType(pa.ExtensionType):
    """Minimal replica of the arrow.uuid extension type used by external tools
    (Spark, DuckDB, etc.) that write Parquet with extension<arrow.uuid> columns.
    Storage type is fixed_size_binary[16] with standard RFC-4122 byte order.
    """
    def __init__(self) -> None:
        super().__init__(pa.binary(16), "arrow.uuid")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):  # noqa: ANN
        return cls()


try:
    pa.register_extension_type(_ArrowUUIDType())
except pa.lib.ArrowKeyError:
    pass  # already registered (e.g. when pytest re-imports the module)

_ARROW_UUID_TYPE = _ArrowUUIDType()


def _make_uuid_extension_array(values: list[str | None]) -> pa.Array:
    """Create an extension<arrow.uuid> array from a list of UUID strings / None."""
    raw: list[bytes | None] = [
        uuid.UUID(v).bytes if v is not None else None
        for v in values
    ]
    storage = pa.array(raw, type=pa.binary(16))
    return pa.ExtensionArray.from_storage(_ARROW_UUID_TYPE, storage)


# ---------------------------------------------------------------------------
# Registry mechanics
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_list_transforms_returns_name_and_display_name(self):
        transforms = tr.list_transforms()
        assert isinstance(transforms, list)
        for name, display_name in transforms:
            assert isinstance(name, str) and name
            assert isinstance(display_name, str) and display_name

    def test_get_returns_callable(self):
        name, _ = tr.list_transforms()[0]
        fn = tr.get(name)
        assert callable(fn)

    def test_get_unknown_raises_key_error(self):
        with pytest.raises(KeyError, match="unknown_transform_xyz"):
            tr.get("unknown_transform_xyz")

    def test_register_decorator_adds_to_registry(self):
        @tr.register("_test_transform", "Test Transform", applicable_types=["int64"])
        def _dummy(array, params):
            return array

        assert "_test_transform" in dict(tr.list_transforms())
        fn = tr.get("_test_transform")
        assert fn is _dummy

    def test_register_with_no_applicable_types(self):
        @tr.register("_test_no_types", "Test No Types")
        def _dummy2(array, params):
            return array

        assert "_test_no_types" in dict(tr.list_transforms())


# ---------------------------------------------------------------------------
# get_suggested
# ---------------------------------------------------------------------------

class TestGetSuggested:
    def test_suggests_binary16_to_uuid_for_fixed_size_binary_16(self):
        arrow_type = pa.binary(16)
        result = tr.get_suggested(arrow_type)
        assert result == "binary16_to_uuid"

    def test_suggests_timestamp_transform_for_ns_timestamp(self):
        arrow_type = pa.timestamp("ns")
        result = tr.get_suggested(arrow_type)
        assert result == "timestamp_ns_to_ms_utc"

    def test_returns_none_for_unrecognised_type(self):
        result = tr.get_suggested(pa.int32())
        assert result is None

    def test_returns_none_for_utf8_type(self):
        result = tr.get_suggested(pa.utf8())
        assert result is None

    def test_suggests_binary16_to_uuid_for_extension_uuid(self):
        result = tr.get_suggested(_ARROW_UUID_TYPE)   # extension<arrow.uuid>
        assert result == "binary16_to_uuid"


# ---------------------------------------------------------------------------
# binary16_to_uuid
# ---------------------------------------------------------------------------

class TestBinary16ToUuid:
    def _make_array(self, values: list) -> pa.Array:
        return pa.array(values, type=pa.binary(16))

    def test_converts_bytes_to_uuid_string(self):
        raw = uuid.UUID("12345678-1234-5678-1234-567812345678").bytes
        result = tr.binary16_to_uuid(self._make_array([raw]), params={})
        assert result[0].as_py() == "12345678-1234-5678-1234-567812345678"

    def test_output_type_is_utf8_string(self):
        raw = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").bytes
        result = tr.binary16_to_uuid(self._make_array([raw]), params={})
        assert result.type == pa.string()

    def test_none_values_are_preserved(self):
        result = tr.binary16_to_uuid(pa.array([None], type=pa.binary(16)), params={})
        assert result[0].as_py() is None

    def test_mixed_none_and_values(self):
        raw = uuid.UUID("00000000-0000-0000-0000-000000000001").bytes
        result = tr.binary16_to_uuid(pa.array([None, raw], type=pa.binary(16)), params={})
        assert result[0].as_py() is None
        assert result[1].as_py() == "00000000-0000-0000-0000-000000000001"

    def test_all_null_array(self):
        result = tr.binary16_to_uuid(pa.array([None, None], type=pa.binary(16)), params={})
        assert all(v.as_py() is None for v in result)

    def test_multiple_values_converted_correctly(self):
        ids = [
            "12345678-1234-5678-1234-567812345678",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ]
        raw_list = [uuid.UUID(u).bytes for u in ids]
        result = tr.binary16_to_uuid(self._make_array(raw_list), params={})
        assert [v.as_py() for v in result] == ids

    # --- extension<arrow.uuid> input (the "unknown type" from production) ---

    def test_extension_uuid_type_is_converted(self):
        """Plain ExtensionArray: extension<arrow.uuid> → UUID string."""
        uid = "12345678-1234-5678-1234-567812345678"
        array = _make_uuid_extension_array([uid])
        result = tr.binary16_to_uuid(array, params={})
        assert result.type == pa.string()
        assert result[0].as_py() == uid

    def test_extension_uuid_chunked_array_is_converted(self):
        """ChunkedArray of extension<arrow.uuid> (as returned by table.column()).

        This is the most common production code path: apply_transforms receives
        table.column('Id') which is always a ChunkedArray, never a plain Array.
        """
        import pyarrow.parquet as pq
        uid = "12345678-1234-5678-1234-567812345678"
        array = _make_uuid_extension_array([uid, None])
        table = pa.table({"Id": array})
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf)
        table2 = pq.read_table(pa.BufferReader(buf.getvalue()))
        col = table2.column("Id")   # ChunkedArray
        assert isinstance(col, pa.ChunkedArray), "precondition: must be ChunkedArray"
        result = tr.binary16_to_uuid(col, params={})
        assert result.type == pa.string()
        assert result[0].as_py() == uid
        assert result[1].as_py() is None

    def test_extension_uuid_null_preserved(self):
        array = _make_uuid_extension_array([None])
        result = tr.binary16_to_uuid(array, params={})
        assert result[0].as_py() is None

    def test_extension_uuid_mixed_none_and_values(self):
        uid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        array = _make_uuid_extension_array([None, uid])
        result = tr.binary16_to_uuid(array, params={})
        assert result[0].as_py() is None
        assert result[1].as_py() == uid

    def test_extension_uuid_multiple_values(self):
        ids = [
            "12345678-1234-5678-1234-567812345678",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ]
        array = _make_uuid_extension_array(ids)
        result = tr.binary16_to_uuid(array, params={})
        assert [v.as_py() for v in result] == ids

    def test_string_input_is_returned_unchanged(self):
        uid = "12345678-1234-5678-1234-567812345678"
        array = pa.array([uid], type=pa.string())
        result = tr.binary16_to_uuid(array, params={})
        assert result.type == pa.string()
        assert result[0].as_py() == uid

    def test_large_string_input_is_cast_to_string(self):
        uid = "12345678-1234-5678-1234-567812345678"
        array = pa.array([uid], type=pa.large_utf8())
        result = tr.binary16_to_uuid(array, params={})
        assert result.type == pa.string()
        assert result[0].as_py() == uid

    def test_integer_input_falls_back_to_str_representation(self):
        """Any castable type (e.g. int64) must not raise — produces str digits."""
        array = pa.array([42, None, 7], type=pa.int64())
        result = tr.binary16_to_uuid(array, params={})
        assert result.type == pa.string()
        assert result[0].as_py() == "42"
        assert result[1].as_py() is None
        assert result[2].as_py() == "7"

    def test_bool_input_falls_back_gracefully(self):
        """Boolean values are converted to their string representation."""
        array = pa.array([True, False, None], type=pa.bool_())
        result = tr.binary16_to_uuid(array, params={})
        assert result.type == pa.string()
        assert result[2].as_py() is None


# ---------------------------------------------------------------------------
# timestamp_ns_to_ms_utc
# ---------------------------------------------------------------------------

class TestTimestampNsToMsUtc:
    def test_output_type_is_timestamp_ms_utc(self):
        array = pa.array([0], type=pa.timestamp("ns"))
        result = tr.timestamp_ns_to_ms_utc(array, params={})
        assert result.type == pa.timestamp("ms", tz="UTC")

    def test_nanoseconds_are_truncated_to_milliseconds(self):
        # 1_000_500 ns → 1 ms (truncated, not rounded)
        array = pa.array([1_000_500], type=pa.timestamp("ns"))
        result = tr.timestamp_ns_to_ms_utc(array, params={})
        assert result[0].as_py().microsecond == 1000  # 1 ms = 1000 µs

    def test_zero_epoch_stays_zero(self):
        array = pa.array([0], type=pa.timestamp("ns"))
        result = tr.timestamp_ns_to_ms_utc(array, params={})
        assert result[0].as_py().timestamp() == 0.0

    def test_null_values_preserved(self):
        array = pa.array([None], type=pa.timestamp("ns"))
        result = tr.timestamp_ns_to_ms_utc(array, params={})
        assert result[0].as_py() is None
