from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

_REQUIRED_COLS = {"TsCreate", "SenderUid", "DeviceUid", "MessageVersion"}


def filter_table_by_ids(table: pa.Table, filter_col: str, filter_values: list[str]) -> pa.Table:
    """Return rows where filter_col matches any value in filter_values."""
    if filter_col not in table.schema.names:
        raise ValueError(
            f"filter_col '{filter_col}' not found in table. "
            f"Available columns: {table.schema.names}"
        )
    value_set = pa.array(filter_values, type=pa.string())
    mask = pc.is_in(table.column(filter_col), value_set=value_set)
    return table.filter(mask)


def build_metadata(table: pa.Table) -> dict[str, str]:
    """Recalculate Parquet file metadata from table contents."""
    missing = _REQUIRED_COLS - set(table.schema.names)
    if missing:
        raise ValueError(f"build_metadata: missing required columns: {sorted(missing)}")
    if table.num_rows == 0:
        raise ValueError("Cannot build metadata for empty table")

    ts_col = _normalize_timestamp(table.column("TsCreate"))
    ts_min = pc.min(ts_col).as_py()
    ts_max = pc.max(ts_col).as_py()

    def _fmt(dt) -> str:
        return dt.strftime("%m/%d/%Y %H:%M:%S +00:00")

    sender_col = table.column("SenderUid").to_pylist()
    device_col = table.column("DeviceUid").to_pylist()
    version_col = table.column("MessageVersion").to_pylist()
    triples = sorted({f"{s} {d} {v}" for s, d, v in zip(sender_col, device_col, version_col)})

    return {
        "recordCount": str(table.num_rows),
        "dateFrom": _fmt(ts_min),
        "dateTo": _fmt(ts_max),
        "deviceIds": ",".join(triples),
        "batchNumber": "1",
    }


def make_output_blob_name(
    output_prefix: str,
    filter_col: str,
    filter_values: list[str],
    part: int | None = None,
) -> str:
    """Generate output blob path.

    Without part:  {output_prefix}/{filter_col}_{id1_id2...}.parquet
    With part:     {output_prefix}/{filter_col}_{id1_id2...}_part_NNN.parquet
    """
    if not filter_values:
        raise ValueError("filter_values must not be empty")
    prefix = output_prefix.rstrip("/")
    ids_part = "_".join(v.replace(" ", "_") for v in filter_values)
    base = f"{prefix}/{filter_col}_{ids_part}"
    if part is not None:
        return f"{base}_part_{part:03d}.parquet"
    return f"{base}.parquet"


class MetadataAccumulator:
    """Incrementally tracks metadata values across multiple filtered chunks."""

    def __init__(self) -> None:
        self._total_rows: int = 0
        self._min_ts = None
        self._max_ts = None
        self._triples: set[str] = set()

    @property
    def total_rows(self) -> int:
        return self._total_rows

    def update(self, chunk: pa.Table) -> None:
        if chunk.num_rows == 0:
            return
        self._total_rows += chunk.num_rows
        ts_col = _normalize_timestamp(chunk.column("TsCreate"))
        chunk_min = pc.min(ts_col).as_py()
        chunk_max = pc.max(ts_col).as_py()
        if self._min_ts is None or chunk_min < self._min_ts:
            self._min_ts = chunk_min
        if self._max_ts is None or chunk_max > self._max_ts:
            self._max_ts = chunk_max
        for s, d, v in zip(
            chunk.column("SenderUid").to_pylist(),
            chunk.column("DeviceUid").to_pylist(),
            chunk.column("MessageVersion").to_pylist(),
        ):
            self._triples.add(f"{s} {d} {v}")

    def to_metadata(self) -> dict[str, str]:
        if self._total_rows == 0:
            raise ValueError("No rows accumulated — cannot build metadata")

        def _fmt(dt) -> str:
            return dt.strftime("%m/%d/%Y %H:%M:%S +00:00")

        return {
            "recordCount": str(self._total_rows),
            "dateFrom": _fmt(self._min_ts),
            "dateTo": _fmt(self._max_ts),
            "deviceIds": ",".join(sorted(self._triples)),
            "batchNumber": "1",
        }


def rewrite_with_metadata(src_path: str, dst_path: str, metadata: dict[str, str]) -> None:
    """Stream-copy a Parquet file to dst_path, merging metadata into the schema footer.

    Existing metadata keys in src are preserved; keys in *metadata* overwrite them.
    Uses iter_batches so peak RAM = one row group, not the full file.
    """
    import pyarrow.parquet as _pq

    pf = _pq.ParquetFile(src_path)
    existing = pf.schema_arrow.metadata or {}
    decoded: dict[str, str] = {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in existing.items()
    }
    decoded.update(metadata)
    schema_with_meta = pf.schema_arrow.with_metadata(decoded)
    with _pq.ParquetWriter(dst_path, schema_with_meta, compression="zstd", compression_level=3) as w:
        for batch in pf.iter_batches():
            w.write_batch(batch)


def _normalize_timestamp(column: pa.ChunkedArray) -> pa.ChunkedArray:
    if not pa.types.is_timestamp(column.type):
        raise ValueError("TsCreate must be a timestamp column")
    target_tz = "UTC"
    ts = column
    if ts.type.tz is None:
        ts = pc.assume_timezone(ts, target_tz)
    elif ts.type.tz != target_tz:
        ts = pc.convert_timezone(ts, target_tz)
    return ts.cast(pa.timestamp("ms", tz=target_tz), safe=False)
