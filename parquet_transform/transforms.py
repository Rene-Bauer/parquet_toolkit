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


TransformFn = Callable[[pa.Array, dict], pa.Array]

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
    """
    type_str = str(arrow_type)
    for key, (_, _, applicable_types) in _REGISTRY.items():
        if applicable_types is None:
            continue
        for pattern in applicable_types:
            if pattern in type_str:
                return key
    return None


# ---------------------------------------------------------------------------
# Built-in transforms
# ---------------------------------------------------------------------------

@register(
    "binary16_to_uuid",
    "→ String (UUID-Format)",
    applicable_types=["fixed_size_binary[16]"],
)
def binary16_to_uuid(array: pa.Array, params: dict) -> pa.Array:
    """
    Convert fixed_size_binary[16] to plain UTF-8 string in UUID format
    "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".

    Output type: pa.string() = Arrow utf8
    Parquet encoding: BYTE_ARRAY with STRING/UTF8 logical annotation
    Spark type: StringType — natively readable by Databricks Autoloader.
    """
    results: list[str | None] = []
    for item in array:
        if item is None or item.as_py() is None:
            results.append(None)
        else:
            raw: bytes = item.as_py()
            results.append(str(uuid.UUID(bytes=raw)))
    return pa.array(results, type=pa.string())


@register(
    "timestamp_ns_to_ms",
    "→ timestamp[ms] (Spark)",
    applicable_types=["timestamp[ns]"],
)
def timestamp_ns_to_ms(array: pa.Array, params: dict) -> pa.Array:
    """
    Cast timestamp[ns] to timestamp[ms], preserving the original timezone.

    If the source column has no timezone (tz=None), the output also has none.
    PyArrow then writes TIMESTAMP(isAdjustedToUTC=False, unit=MILLIS) to Parquet,
    which Spark / Databricks Autoloader reads as a local (non-UTC) timestamp.

    Nanosecond and microsecond precision is truncated (not rounded) to milliseconds.
    """
    tz = array.type.tz  # None if no timezone was set — do not force UTC
    target = pa.timestamp("ms", tz=tz)
    return array.cast(target, safe=False)  # safe=False: truncate sub-ms precision, don't raise


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
    return array.cast(pa.timestamp("ms", tz="UTC"), safe=False)
