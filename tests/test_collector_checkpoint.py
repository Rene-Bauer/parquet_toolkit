"""Tests for CollectorRunRecord in parquet_transform.checkpoint."""
import pytest
from parquet_transform.checkpoint import CollectorRunRecord


@pytest.fixture()
def checkpoint_dir(tmp_path, monkeypatch):
    import parquet_transform.checkpoint as cp_module
    monkeypatch.setattr(cp_module, "_CHECKPOINT_DIR", tmp_path)
    return tmp_path


class TestCollectorRunRecord:
    def test_creates_with_none_status(self, checkpoint_dir):
        r = CollectorRunRecord.load_or_create("cont", "prefix/", "SenderUid", ["uid1"])
        assert r.status == "none"
        assert not r.is_complete()
        assert r.output_blob is None
        assert r.row_count == 0

    def test_mark_complete_persists_and_reloads(self, checkpoint_dir):
        r = CollectorRunRecord.load_or_create("cont", "prefix/", "SenderUid", ["uid1"])
        r.mark_complete("out/result.parquet", 42)
        # reload from disk
        r2 = CollectorRunRecord.load_or_create("cont", "prefix/", "SenderUid", ["uid1"])
        assert r2.is_complete()
        assert r2.output_blob == "out/result.parquet"
        assert r2.row_count == 42

    def test_mark_in_progress_clears_previous_result(self, checkpoint_dir):
        r = CollectorRunRecord.load_or_create("cont", "prefix/", "SenderUid", ["uid1"])
        r.mark_complete("out/result.parquet", 42)
        r.mark_in_progress()
        assert r.status == "in_progress"
        assert r.output_blob is None
        assert r.row_count == 0

    def test_reset_clears_to_none(self, checkpoint_dir):
        r = CollectorRunRecord.load_or_create("cont", "prefix/", "SenderUid", ["uid1"])
        r.mark_complete("out/result.parquet", 42)
        r.reset()
        assert r.status == "none"
        assert not r.is_complete()
        assert r.output_blob is None

    def test_record_path_is_order_independent_for_filter_values(self, checkpoint_dir):
        p1 = CollectorRunRecord.record_path("cont", "prefix/", "SenderUid", ["uid2", "uid1"])
        p2 = CollectorRunRecord.record_path("cont", "prefix/", "SenderUid", ["uid1", "uid2"])
        assert p1 == p2

    def test_different_filter_gives_different_path(self, checkpoint_dir):
        p1 = CollectorRunRecord.record_path("cont", "prefix/", "SenderUid", ["uid1"])
        p2 = CollectorRunRecord.record_path("cont", "prefix/", "DeviceUid", ["uid1"])
        assert p1 != p2

    def test_corrupt_file_raises_runtime_error(self, checkpoint_dir):
        r = CollectorRunRecord.load_or_create("cont", "prefix/", "SenderUid", ["uid1"])
        path = CollectorRunRecord.record_path("cont", "prefix/", "SenderUid", ["uid1"])
        path.write_text("not valid json", encoding="utf-8")
        with pytest.raises(RuntimeError, match="corrupt"):
            CollectorRunRecord.load_or_create("cont", "prefix/", "SenderUid", ["uid1"])


class TestAtomicWriteConcurrency:
    """_atomic_write must survive concurrent calls from multiple threads
    targeting the same destination file (the scenario that caused the
    Windows PermissionError crash in the field)."""

    def test_concurrent_writes_produce_valid_json(self, checkpoint_dir):
        """Many threads writing to the same path must not corrupt the file."""
        import threading
        from parquet_transform.checkpoint import _atomic_write

        target = checkpoint_dir / "concurrent_test.json"
        errors = []

        def _writer(n: int) -> None:
            try:
                _atomic_write(target, {"writer": n})
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_writer, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Concurrent writes raised: {errors}"
        # File must be valid JSON after all writers finish
        import json
        data = json.loads(target.read_text(encoding="utf-8"))
        assert "writer" in data

    def test_no_orphaned_tmp_files_after_concurrent_writes(self, checkpoint_dir):
        """Unique temp names mean each write cleans up its own .tmp on success."""
        import threading
        from parquet_transform.checkpoint import _atomic_write

        target = checkpoint_dir / "orphan_test.json"

        def _writer(n: int) -> None:
            _atomic_write(target, {"writer": n})

        threads = [threading.Thread(target=_writer, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        tmp_files = list(checkpoint_dir.glob("*.tmp"))
        assert tmp_files == [], f"Orphaned .tmp files found: {tmp_files}"
