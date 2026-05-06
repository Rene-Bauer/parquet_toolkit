"""Tests for parquet_transform.csv_converter."""
import io
import zipfile
import pyarrow as pa
import pytest
from parquet_transform.csv_converter import (
    extract_csv_tables,
    merge_tables,
    compute_zip_output_name,
)


def _make_zip(*csv_pairs: tuple[str, str]) -> bytes:
    """Helper: build a ZIP containing named CSV files from (filename, content) pairs."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in csv_pairs:
            zf.writestr(name, content)
    return buf.getvalue()


def test_extract_single_csv():
    csv_data = "id,name,value\n1,Alice,10\n2,Bob,20\n"
    zipped = _make_zip(("data.csv", csv_data))
    tables = extract_csv_tables(zipped)
    assert len(tables) == 1
    assert tables[0].num_rows == 2
    assert tables[0].schema.names == ["id", "name", "value"]


def test_extract_multiple_csvs():
    csv1 = "id,val\n1,a\n2,b\n"
    csv2 = "id,val\n3,c\n"
    zipped = _make_zip(("a.csv", csv1), ("b.csv", csv2))
    tables = extract_csv_tables(zipped)
    assert len(tables) == 2


def test_extract_ignores_non_csv_entries():
    csv_data = "id,val\n1,x\n"
    zipped = _make_zip(("data.csv", csv_data), ("readme.txt", "ignore me"))
    tables = extract_csv_tables(zipped)
    assert len(tables) == 1


def test_extract_empty_zip_returns_empty_list():
    buf = io.BytesIO()
    zipfile.ZipFile(buf, "w").close()
    tables = extract_csv_tables(buf.getvalue())
    assert tables == []


def test_extract_semicolon_delimiter():
    csv_data = "id;name\n1;Alice\n"
    zipped = _make_zip(("data.csv", csv_data))
    tables = extract_csv_tables(zipped, delimiter=";")
    assert tables[0].schema.names == ["id", "name"]
    assert tables[0].num_rows == 1


def test_extract_latin1_encoding():
    csv_data = "name\nMüller\n".encode("latin-1")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("data.csv", csv_data)
    tables = extract_csv_tables(buf.getvalue(), encoding="latin-1")
    assert tables[0]["name"][0].as_py() == "Müller"


def test_merge_single_table_returns_same():
    t = pa.table({"id": [1, 2], "val": ["a", "b"]})
    result = merge_tables([t])
    assert result is t


def test_merge_compatible_tables():
    t1 = pa.table({"id": [1], "val": ["a"]})
    t2 = pa.table({"id": [2], "val": ["b"]})
    merged = merge_tables([t1, t2])
    assert merged.num_rows == 2


def test_merge_incompatible_raises():
    t1 = pa.table({"id": [1]})
    t2 = pa.table({"name": ["x"]})
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        merge_tables([t1, t2])


def test_compute_zip_output_name_strips_prefix_and_changes_ext():
    result = compute_zip_output_name(
        source_blob="archive/2026-03/daily.zip",
        source_prefix="archive/",
        output_prefix="converted/",
    )
    assert result == "converted/2026-03/daily.parquet"


def test_compute_zip_output_name_blob_not_under_prefix():
    """When blob is not under source_prefix, use the full blob name as relative path."""
    result = compute_zip_output_name(
        source_blob="other/data.zip",
        source_prefix="archive/",
        output_prefix="out/",
    )
    assert result == "out/other/data.parquet"


def test_compute_zip_output_name_trailing_slash_normalised():
    r1 = compute_zip_output_name("arc/f.zip", "arc/", "out/")
    r2 = compute_zip_output_name("arc/f.zip", "arc",  "out")
    assert r1 == r2
