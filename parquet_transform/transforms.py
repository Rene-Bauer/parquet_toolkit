"""
Transformation registry for Parquet column type changes.

To add a new transform:
  1. Write a function (array: pa.Array, params: dict) -> pa.Array
  2. Decorate it with @register(name, display_name, applicable_types=[...])
  3. It will automatically appear in the UI dropdown.
"""
from __future__ import annotations

import uuid
from typing import Callable

import numpy as np
import pyarrow as pa
from pyarrow import types as pa_types


TransformFn = Callable[[pa.Array, dict], pa.Array]

_UUID_STRING_TYPE = pa.string()
_TIMESTAMP_MS_UTC_TYPE = pa.timestamp("ms", tz="UTC")

_REGISTRY: dict[str, tuple[TransformFn, str, list[str] | None]] = {}
# key -> (fn, display_name, applicable_type_strings or None=all)

# ---------------------------------------------------------------------------
# Expected output types — used by workers to decide whether a file is already
# at its target schema and can be skipped or counted as "done".
# ---------------------------------------------------------------------------

_EXPECTED_OUTPUT_TYPES: dict[str, pa.DataType] = {
    "binary16_to_uuid": _UUID_STRING_TYPE,
    "timestamp_ns_to_ms_utc": _TIMESTAMP_MS_UTC_TYPE,
}


def get_expected_output_type(transform_name: str) -> pa.DataType | None:
    """Return the expected Arrow output DataType for a registered transform, or None.

    Returns None for unknown transform names and for transforms that have no
    fixed output type (e.g. passthrough transforms).  Callers that receive None
    should treat the column as untransformable for skip-checking purposes.
    """
    return _EXPECTED_OUTPUT_TYPES.get(transform_name)


def register(name: str, display_name: str, applicable_types: list[str] | None = None):
    """
    Decorator that registers a transform function.

    Args:
        name: Registry key used internally (e.g. "binary16_to_uuid").
        display_name: Human-readable label shown in the UI dropdown.
        applicable_types: List of Arrow type strings for auto-suggest
            (e.g. ["fixed_size_binary[16]"]). None means always shown.
    """
    def decorator(fn: TransformFn) -> TransformFn:
        _REGISTRY[name] = (fn, display_name, applicable_types)
        return fn
    return decorator


def list_transforms() -> list[tuple[str, str]]:
    """Return [(name, display_name)] for all registered transforms."""
    return [(k, v[1]) for k, v in _REGISTRY.items()]


def get(name: str) -> TransformFn:
    """Return the transform function for the given registry key."""
    if name not in _REGISTRY:
        available = list(_REGISTRY.keys())
        raise KeyError(f"Unknown transform '{name}'. Available: {available}")
    return _REGISTRY[name][0]


def get_suggested(arrow_type: pa.DataType) -> str | None:
    """
    Return the registry key of the best matching transform for the given
    Arrow type, or None if no auto-suggestion exists.

    Extension types (e.g. extension<arrow.uuid>) are matched both by their
    full string representation and by their extension_name so the pattern
    "arrow.uuid" hits without needing to embed "extension<...>" in every rule.
    """
    type_str = str(arrow_type)
    # For extension types also expose the bare extension name for pattern matching
    extension_name = getattr(arrow_type, "extension_name", None)

    for key, (_, _, applicable_types) in _REGISTRY.items():
        if applicable_types is None:
            continue
        for pattern in applicable_types:
            if pattern in type_str:
                return key
            if extension_name and pattern in extension_name:
                return key
    return None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_HEX_CHARS = np.frombuffer(b"0123456789abcdef", dtype=np.uint8)


def _fixed_binary16_to_uuid_strings(array: pa.Array) -> pa.Array:
    """Convert a fixed_size_binary[16] PyArrow array to UUID strings.

    Uses numpy for the hex-encoding step so that the heavy work runs in C
    and releases the GIL — keeping the Qt main thread responsive when many
    parallel worker threads call this simultaneously.

    Output type: pa.string()  (UTF-8, UUID canonical format with dashes)
    """
    # table.column() always returns a ChunkedArray — combine into a single
    # contiguous Array so we can take a zero-copy numpy view of the buffer.
    if isinstance(array, pa.ChunkedArray):
        array = array.combine_chunks()

    n = len(array)
    if n == 0:
        return pa.array([], type=_UUID_STRING_TYPE)

    # Zero-copy view of the raw byte buffer, accounting for slice offsets.
    buf = array.buffers()[1]
    offset = array.offset
    raw = np.frombuffer(buf, dtype=np.uint8)[offset * 16:(offset + n) * 16]
    raw = raw.reshape(n, 16)  # (N, 16) — each row is one UUID

    # Vectorised hex encoding in C (GIL released by numpy).
    hi = _HEX_CHARS[raw >> 4]   # high nibbles  → (N, 16) ASCII bytes
    lo = _HEX_CHARS[raw & 0x0F] # low nibbles   → (N, 16) ASCII bytes

    # Interleave hi/lo into (N, 32) hex-char array.
    hex32 = np.empty((n, 32), dtype=np.uint8)
    hex32[:, 0::2] = hi
    hex32[:, 1::2] = lo

    # Insert dashes: UUID layout is 8-4-4-4-12 hex chars.
    dash = ord("-")
    uuid36 = np.empty((n, 36), dtype=np.uint8)
    uuid36[:, :8]  = hex32[:, :8]
    uuid36[:, 8]   = dash
    uuid36[:, 9:13]  = hex32[:, 8:12]
    uuid36[:, 13]  = dash
    uuid36[:, 14:18] = hex32[:, 12:16]
    uuid36[:, 18]  = dash
    uuid36[:, 19:23] = hex32[:, 16:20]
    uuid36[:, 23]  = dash
    uuid36[:, 24:]   = hex32[:, 20:]

    # Convert each 36-byte row to a Python str — tobytes()+decode is C-level.
    uuid_bytes = uuid36.tobytes()  # flat bytes, length N*36
    strings: list[str | None] = [
        uuid_bytes[i * 36:(i + 1) * 36].decode("ascii")
        for i in range(n)
    ]

    # Re-apply null mask if the array has nulls.
    if array.null_count > 0:
        null_buf = array.buffers()[0]
        null_bytes = np.frombuffer(null_buf, dtype=np.uint8)
        for i in range(n):
            abs_i = offset + i
            if not (null_bytes[abs_i >> 3] >> (abs_i & 7)) & 1:
                strings[i] = None

    return pa.array(strings, type=_UUID_STRING_TYPE)


# ---------------------------------------------------------------------------
# Built-in transforms
# ---------------------------------------------------------------------------

@register(
    "binary16_to_uuid",
    "→ String (UUID-Format)",
    applicable_types=["fixed_size_binary[16]", "arrow.uuid", "extension"],
)
def binary16_to_uuid(array: pa.Array, params: dict) -> pa.Array:
    """
    Convert any UUID-like column to plain UTF-8 string in UUID format
    "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".

    Handled input types (in order):
      • pa.string() / pa.large_string()     — returned as-is / cast
      • extension<arrow.uuid>               — unwrap storage → byte conversion
      • any other extension type            — unwrap storage and recurse
      • fixed_size_binary[16]               — byte-level UUID conversion
      • anything else                       — attempt pa.cast to string

    Output type: pa.string() = Arrow utf8
    Parquet encoding: BYTE_ARRAY with STRING/UTF8 logical annotation
    Spark type: StringType — natively readable by Databricks Autoloader.
    """
    arr_type = array.type

    # ── Already a string type ────────────────────────────────────────────────
    if pa_types.is_string(arr_type):
        return array
    if pa_types.is_large_string(arr_type):
        return array.cast(_UUID_STRING_TYPE, safe=False)

    # ── Arrow extension types (e.g. extension<arrow.uuid>) ───────────────────
    # PyArrow stores extension<arrow.uuid> as fixed_size_binary[16] underneath.
    # Unwrap to storage and recurse so all subsequent logic applies.
    #
    # Key subtlety: table.column() returns a ChunkedArray, which does NOT have
    # a .storage attribute — only a plain ExtensionArray has it.  We must call
    # combine_chunks() first to collapse the ChunkedArray into one ExtensionArray.
    if isinstance(arr_type, pa.ExtensionType):
        # ChunkedArray path (most common — table.column() always returns this)
        if isinstance(array, pa.ChunkedArray):
            return binary16_to_uuid(array.combine_chunks(), params)
        # Plain ExtensionArray path
        if hasattr(array, "storage"):
            return binary16_to_uuid(array.storage, params)
        # Extension without accessible storage — fall through to generic path
        pass

    # ── fixed_size_binary[16] — standard UUID byte layout ───────────────────
    # Vectorised via numpy so the hex-encoding step runs in C and releases
    # the GIL, keeping the Qt main thread responsive under heavy parallelism.
    if pa_types.is_fixed_size_binary(arr_type) and arr_type.byte_width == 16:
        return _fixed_binary16_to_uuid_strings(array)

    # ── Generic fallback: cast whatever type is present to string ────────────
    # Level 1: Arrow-native cast (fast, zero-copy for many types)
    try:
        return array.cast(_UUID_STRING_TYPE, safe=False)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        pass

    # Level 2: element-by-element Python str() — works for literally any type
    results: list[str | None] = [
        None if item.as_py() is None else str(item.as_py())
        for item in array
    ]
    return pa.array(results, type=_UUID_STRING_TYPE)


@register(
    "timestamp_ns_to_ms_utc",
    "→ timestamp[ms, UTC] (Spark)",
    applicable_types=["timestamp[ns]"],
)
def timestamp_ns_to_ms_utc(array: pa.Array, params: dict) -> pa.Array:
    """
    Cast timestamp[ns] to timestamp[ms, UTC].

    Forces UTC regardless of the original timezone.
    PyArrow writes TIMESTAMP(isAdjustedToUTC=True, unit=MILLIS) to Parquet,
    which Spark / Databricks Autoloader reads as TimestampType (UTC-based).

    Nanosecond and microsecond precision is truncated (not rounded) to milliseconds.
    """
    if array.type.equals(_TIMESTAMP_MS_UTC_TYPE):
        return array
    return array.cast(_TIMESTAMP_MS_UTC_TYPE, safe=False)
