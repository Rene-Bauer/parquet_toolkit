"""
CSV → Parquet conversion utilities.

All functions are pure (no Azure, no Qt) and operate on in-memory bytes.
This module is the only place in the codebase that deals with ZIP archives
and CSV parsing — everything else works with Parquet.
"""
from __future__ import annotations

import io
import zipfile

import pyarrow as pa
import pyarrow.csv as pa_csv


def extract_csv_tables(
    zip_bytes: bytes,
    delimiter: str = ",",
    encoding: str = "utf-8",
) -> list[pa.Table]:
    """Open *zip_bytes* as a ZIP archive and parse every .csv entry.

    Returns one ``pa.Table`` per CSV file in the archive, in the order they
    appear in the ZIP's central directory.  Non-CSV entries are silently
    skipped.  An empty ZIP or one with no CSV files returns an empty list.

    Args:
        zip_bytes: Raw bytes of the ZIP archive (as returned by
            ``BlobStorageClient.download_bytes``).
        delimiter: Column delimiter character used in every CSV.
        encoding: Character encoding of the CSV text.  Passed to PyArrow's
            ``ReadOptions``; common values: ``"utf-8"``, ``"latin-1"``,
            ``"cp1252"``.

    Raises:
        zipfile.BadZipFile: *zip_bytes* is not a valid ZIP archive.
        pa.ArrowInvalid: A CSV file cannot be parsed with the given options.
    """
    tables: list[pa.Table] = []
    read_options = pa_csv.ReadOptions(encoding=encoding)
    parse_options = pa_csv.ParseOptions(delimiter=delimiter)

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        for name in csv_names:
            with zf.open(name) as f:
                content = f.read()
            table = pa_csv.read_csv(
                io.BytesIO(content),
                read_options=read_options,
                parse_options=parse_options,
            )
            tables.append(table)

    return tables


def merge_tables(tables: list[pa.Table]) -> pa.Table:
    """Concatenate *tables* into a single table.

    Raises:
        ValueError: Tables have incompatible schemas.
        pa.ArrowInvalid: PyArrow cannot reconcile column types.
    """
    if not tables:
        raise ValueError("merge_tables called with an empty list")
    if len(tables) == 1:
        return tables[0]

    first_schema = tables[0].schema
    for i, t in enumerate(tables[1:], 1):
        if not t.schema.equals(first_schema):
            raise ValueError(
                f"Cannot merge: CSV #{i} schema differs from CSV #0.\n"
                f"  CSV #0 : {first_schema}\n"
                f"  CSV #{i}: {t.schema}"
            )
    return pa.concat_tables(tables)


def compute_zip_output_name(
    source_blob: str,
    source_prefix: str,
    output_prefix: str,
) -> str:
    """Derive an output Parquet blob name from a source ZIP blob name.

    Strips *source_prefix* from the start of *source_blob*, replaces the
    ``.zip`` extension with ``.parquet``, and prepends *output_prefix*.

    Trailing slashes in both prefix arguments are normalised so callers do
    not need to be careful about them.

    Examples::

        compute_zip_output_name(
            "archive/2026-03/daily.zip", "archive/", "converted/"
        )
        # → "converted/2026-03/daily.parquet"

        compute_zip_output_name(
            "other/data.zip", "archive/", "out/"
        )
        # → "out/other/data.parquet"  (blob not under prefix → full path kept)
    """
    norm_src_prefix = source_prefix.rstrip("/") + "/"
    norm_out_prefix = output_prefix.rstrip("/")

    if source_blob.startswith(norm_src_prefix):
        relative = source_blob[len(norm_src_prefix):]
    else:
        relative = source_blob

    base = relative[:-4] if relative.lower().endswith(".zip") else relative
    return f"{norm_out_prefix}/{base}.parquet"
