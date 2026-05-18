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

import pyarrow as pa
from pyarrow import types as pa_types


TransformFn = Callable[[pa.Array, dict], pa.Array]

_UUID_STRING_TYPE = pa.string()
_TIMESTAMP_MS_UTC_TYPE = pa.timestamp("ms", tz="UTC")

_REGISTRY: dict[str, tuple[TransformFn, str, list[str] | None]] = {}
# key -> (fn, display_name, applicable_type_strings or None=all)


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
    if pa_types.is_fixed_size_binary(arr_type) and arr_type.byte_width == 16:
        results: list[str | None] = []
        for item in array:
            if item is None or item.as_py() is None:
                results.append(None)
            else:
                raw: bytes = item.as_py()
                results.append(str(uuid.UUID(bytes=raw)))
        return pa.array(results, type=_UUID_STRING_TYPE)

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
