# tests/test_subfolder_checkpoint.py
import pytest
from pathlib import Path
from parquet_transform.checkpoint import SubfolderCheckpoint


def _cp(tmp_path: Path) -> SubfolderCheckpoint:
    """Helper: fresh checkpoint in tmp_path."""
    return SubfolderCheckpoint.load_or_create(
        container="mycontainer",
        source_prefix="archive/",
        filter_col="SenderUid",
        filter_values=["uid1", "uid2"],
        output_prefix="collected/",
        output_container="mycontainer",
        _checkpoint_dir=tmp_path,
    )


def test_new_checkpoint_defaults(tmp_path):
    cp = _cp(tmp_path)
    assert cp.next_part == 1
    assert cp.total_rows == 0
    assert cp.done_count == 0
    assert not cp.is_done("26-02-2026")


def test_mark_done_updates_state(tmp_path):
    cp = _cp(tmp_path)
    cp.mark_done("26-02-2026", parts_produced=1, rows=500)
    assert cp.is_done("26-02-2026")
    assert cp.next_part == 2
    assert cp.total_rows == 500
    assert cp.done_count == 1


def test_mark_done_zero_parts_does_not_advance_counter(tmp_path):
    """Subfolder with no matches: parts_produced=0 → next_part unchanged."""
    cp = _cp(tmp_path)
    cp.mark_done("26-02-2026", parts_produced=0, rows=0)
    assert cp.is_done("26-02-2026")
    assert cp.next_part == 1   # unchanged
    assert cp.total_rows == 0


def test_mark_done_multi_part_subfolder(tmp_path):
    cp = _cp(tmp_path)
    cp.mark_done("26-02-2026", parts_produced=3, rows=9000)
    assert cp.next_part == 4
    assert cp.total_rows == 9000


def test_mark_done_accumulates_across_subfolders(tmp_path):
    cp = _cp(tmp_path)
    cp.mark_done("26-02-2026", parts_produced=1, rows=500)
    cp.mark_done("26-03-2026", parts_produced=2, rows=1200)
    assert cp.next_part == 4
    assert cp.total_rows == 1700
    assert cp.done_count == 2


def test_mark_done_idempotent(tmp_path):
    """Calling mark_done twice for the same subfolder must not double-count."""
    cp = _cp(tmp_path)
    cp.mark_done("26-02-2026", parts_produced=1, rows=500)
    cp.mark_done("26-02-2026", parts_produced=1, rows=500)  # duplicate
    assert cp.done_count == 1
    assert cp.next_part == 2
    assert cp.total_rows == 500


def test_checkpoint_persists_across_loads(tmp_path):
    """mark_done writes atomically; a new load_or_create reads the saved state."""
    cp1 = _cp(tmp_path)
    cp1.mark_done("26-02-2026", parts_produced=2, rows=1000)

    cp2 = _cp(tmp_path)   # reload from disk
    assert cp2.is_done("26-02-2026")
    assert cp2.next_part == 3
    assert cp2.total_rows == 1000


def test_checkpoint_corrupt_file_raises(tmp_path):
    cp = _cp(tmp_path)
    cp.mark_done("26-02-2026", parts_produced=1, rows=100)

    # Corrupt the file
    cp.path.write_text("not json", encoding="utf-8")

    with pytest.raises(RuntimeError, match="corrupt"):
        _cp(tmp_path)


def test_checkpoint_filename_is_deterministic(tmp_path):
    cp_a = _cp(tmp_path)
    cp_b = _cp(tmp_path)
    assert cp_a.path == cp_b.path


def test_different_filter_values_produce_different_files(tmp_path):
    cp_a = SubfolderCheckpoint.load_or_create(
        container="c", source_prefix="p/", filter_col="SenderUid",
        filter_values=["uid1"], output_prefix="out/", output_container="c",
        _checkpoint_dir=tmp_path,
    )
    cp_b = SubfolderCheckpoint.load_or_create(
        container="c", source_prefix="p/", filter_col="SenderUid",
        filter_values=["uid999"], output_prefix="out/", output_container="c",
        _checkpoint_dir=tmp_path,
    )
    assert cp_a.path != cp_b.path


def test_different_filter_cols_produce_different_files(tmp_path):
    cp_a = SubfolderCheckpoint.load_or_create(
        container="c", source_prefix="p/", filter_col="SenderUid",
        filter_values=["uid1"], output_prefix="out/", output_container="c",
        _checkpoint_dir=tmp_path,
    )
    cp_b = SubfolderCheckpoint.load_or_create(
        container="c", source_prefix="p/", filter_col="DeviceUid",
        filter_values=["uid1"], output_prefix="out/", output_container="c",
        _checkpoint_dir=tmp_path,
    )
    assert cp_a.path != cp_b.path
