"""
Core processing logic: download → transform → upload.

This module is UI-agnostic. Progress and log callbacks are injected so
the same code can be driven by the GUI workers or a plain script.
"""
from __future__ import annotations

import io
from dataclasses import dataclass, field
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_transform import transforms as tr
from parquet_transform.storage import BlobStorageClient


@dataclass
class ColumnConfig:
    """Specifies a single column transformation to apply."""
    name: str
    transform: str  # registry key from transforms module
    params: dict = field(default_factory=dict)


def apply_transforms(
    table: pa.Table,
    col_configs: list[ColumnConfig],
    log_fn: Callable[[str], None] | None = None,
) -> pa.Table:
    """
    Apply a list of column transformations to a PyArrow Table.

    Columns not present in the table are skipped with a warning.
    All other columns and their order are preserved unchanged.
    """
    for cfg in col_configs:
        if cfg.name not in table.schema.names:
            if log_fn:
                log_fn(f"  Warning: column '{cfg.name}' not found — skipping")
            continue
        transform_fn = tr.get(cfg.transform)
        col_idx = table.schema.get_field_index(cfg.name)
        new_col = transform_fn(table.column(cfg.name), cfg.params)
        table = table.set_column(col_idx, cfg.name, new_col)
    return table


def process_blob(
    client: BlobStorageClient,
    blob_name: str,
    col_configs: list[ColumnConfig],
    output_blob_name: str,
    dry_run: bool = False,
    log_fn: Callable[[str], None] | None = None,
) -> None:
    """
    Download one Parquet blob, transform it, and upload the result.

    Args:
        client: Connected BlobStorageClient.
        blob_name: Source blob path inside the container.
        col_configs: Transformations to apply.
        output_blob_name: Destination blob path (may equal blob_name for in-place).
        dry_run: If True, transform but do not upload.
        log_fn: Optional callback for progress messages.
    """
    raw = client.download_bytes(blob_name)
    buf = io.BytesIO(raw)
    table = pq.read_table(buf)

    table = apply_transforms(table, col_configs, log_fn=log_fn)

    if dry_run:
        if log_fn:
            log_fn(f"  [DRY RUN] would upload to: {output_blob_name}")
        return

    out_buf = io.BytesIO()
    pq.write_table(table, out_buf)
    client.upload_bytes(output_blob_name, out_buf.getvalue(), overwrite=True)


def compute_output_name(
    blob_name: str,
    input_prefix: str,
    output_prefix: str | None,
) -> str:
    """
    Compute the destination blob name.

    If *output_prefix* is None, returns *blob_name* unchanged (in-place).
    Otherwise, replaces the leading *input_prefix* with *output_prefix*,
    preserving the relative path beneath the prefix.
    """
    if output_prefix is None:
        return blob_name
    relative = blob_name[len(input_prefix):]
    return output_prefix.rstrip("/") + "/" + relative.lstrip("/")
