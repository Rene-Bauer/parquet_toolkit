"""Tests for RunCheckpoint and FailedList in parquet_transform.checkpoint."""
import json
import threading
from pathlib import Path

import pytest

from parquet_transform.checkpoint import FailedList, RunCheckpoint


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def checkpoint_dir(tmp_path, monkeypatch):
    """Redirect _CHECKPOINTS_DIR to a temp directory for all tests."""
    import parquet_transform.checkpoint as cp_module
    monkeypatch.setattr(cp_module, "_CHECKPOINTS_DIR", tmp_path)
    return tmp_path


# ---------------------------------------------------------------------------
# RunCheckpoint
# ---------------------------------------------------------------------------

class TestRunCheckpointPath:
    def test_path_contains_container_and_prefix(self, checkpoint_dir):
        path = RunCheckpoint.checkpoint_path("mycontainer", "raw/events/2026/03/")
        assert "mycontainer" in path.name
        assert path.suffix == ".json"
        assert "checkpoint" in path.name

    def test_path_sanitizes_special_chars(self, checkpoint_dir):
        path = RunCheckpoint.checkpoint_path("my-container", "raw/events/")
        assert "/" not in path.name
        assert "-" not in path.name


class TestRunCheckpointLoadOrCreate:
    def test_creates_new_checkpoint_when_no_file(self, checkpoint_dir):
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        assert cp.cursor is None
        assert not cp.is_complete()

    def test_loads_existing_checkpoint(self, checkpoint_dir):
        path = RunCheckpoint.checkpoint_path("c", "p/")
        path.write_text(json.dumps({
            "container": "c", "prefix": "p/", "output_prefix": None,
            "status": "in_progress", "created_at": "2026-01-01T00:00:00",
            "updated_at": "2026-01-01T00:00:00", "cursor": "p/file-0042.parquet",
        }), encoding="utf-8")
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        assert cp.cursor == "p/file-0042.parquet"

    def test_loads_complete_status(self, checkpoint_dir):
        path = RunCheckpoint.checkpoint_path("c", "p/")
        path.write_text(json.dumps({
            "container": "c", "prefix": "p/", "output_prefix": None,
            "status": "complete", "created_at": "2026-01-01T00:00:00",
            "updated_at": "2026-01-01T00:00:00", "cursor": "p/last.parquet",
        }), encoding="utf-8")
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        assert cp.is_complete()


class TestRunCheckpointShouldSkip:
    def test_no_cursor_skips_nothing(self, checkpoint_dir):
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        assert not cp.should_skip("p/a.parquet")

    def test_skips_blobs_at_or_before_cursor(self, checkpoint_dir):
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        cp.advance_cursor("p/b.parquet")
        assert cp.should_skip("p/a.parquet")
        assert cp.should_skip("p/b.parquet")
        assert not cp.should_skip("p/c.parquet")

    def test_does_not_skip_blobs_after_cursor(self, checkpoint_dir):
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        cp.advance_cursor("p/2026/03/01/part-0010.parquet")
        assert not cp.should_skip("p/2026/03/02/part-0001.parquet")


class TestRunCheckpointAdvanceCursor:
    def test_cursor_advances_to_later_blob(self, checkpoint_dir):
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        cp.advance_cursor("p/b.parquet")
        assert cp.cursor == "p/b.parquet"

    def test_cursor_does_not_go_backward(self, checkpoint_dir):
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        cp.advance_cursor("p/c.parquet")
        cp.advance_cursor("p/a.parquet")
        assert cp.cursor == "p/c.parquet"

    def test_advance_cursor_writes_to_disk(self, checkpoint_dir):
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        cp.advance_cursor("p/x.parquet")
        path = RunCheckpoint.checkpoint_path("c", "p/")
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["cursor"] == "p/x.parquet"

    def test_advance_cursor_is_thread_safe(self, checkpoint_dir):
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        blobs = [f"p/file-{i:04d}.parquet" for i in range(100)]
        threads = [
            threading.Thread(target=cp.advance_cursor, args=(b,))
            for b in blobs
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert cp.cursor == "p/file-0099.parquet"


class TestRunCheckpointMarkComplete:
    def test_mark_complete_sets_status(self, checkpoint_dir):
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        cp.mark_complete()
        assert cp.is_complete()

    def test_mark_complete_persists_to_disk(self, checkpoint_dir):
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        cp.mark_complete()
        path = RunCheckpoint.checkpoint_path("c", "p/")
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["status"] == "complete"


class TestRunCheckpointReset:
    def test_reset_clears_cursor_and_status(self, checkpoint_dir):
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        cp.advance_cursor("p/x.parquet")
        cp.mark_complete()
        cp.reset()
        assert cp.cursor is None
        assert not cp.is_complete()

    def test_reset_persists_to_disk(self, checkpoint_dir):
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        cp.advance_cursor("p/x.parquet")
        cp.reset()
        path = RunCheckpoint.checkpoint_path("c", "p/")
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["cursor"] is None
        assert data["status"] == "in_progress"


# ---------------------------------------------------------------------------
# FailedList
# ---------------------------------------------------------------------------

class TestFailedListPath:
    def test_path_contains_failed_suffix(self, checkpoint_dir):
        path = FailedList.failed_list_path("c", "p/")
        assert "failed" in path.name
        assert path.suffix == ".json"

    def test_different_path_from_checkpoint(self, checkpoint_dir):
        cp_path = RunCheckpoint.checkpoint_path("c", "p/")
        fl_path = FailedList.failed_list_path("c", "p/")
        assert cp_path != fl_path


class TestFailedListLoadOrCreate:
    def test_creates_empty_list_when_no_file(self, checkpoint_dir):
        fl = FailedList.load_or_create("c", "p/")
        assert fl.entries == []
        assert fl.corrupt_count == 0
        assert fl.network_count == 0

    def test_loads_existing_entries(self, checkpoint_dir):
        path = FailedList.failed_list_path("c", "p/")
        path.write_text(json.dumps({
            "container": "c", "prefix": "p/",
            "created_at": "2026-01-01T00:00:00", "updated_at": "2026-01-01T00:00:00",
            "entries": [
                {"name": "p/bad.parquet", "type": "corrupt",
                 "reason": "bad magic", "failed_at": "2026-01-01T00:00:00"},
            ],
        }), encoding="utf-8")
        fl = FailedList.load_or_create("c", "p/")
        assert len(fl.entries) == 1
        assert fl.corrupt_count == 1


class TestFailedListAddOrUpdate:
    def test_adds_new_entry(self, checkpoint_dir):
        fl = FailedList.load_or_create("c", "p/")
        fl.add_or_update("p/bad.parquet", "corrupt", "Invalid magic bytes")
        assert len(fl.entries) == 1
        assert fl.entries[0]["name"] == "p/bad.parquet"
        assert fl.entries[0]["type"] == "corrupt"

    def test_updates_existing_entry(self, checkpoint_dir):
        fl = FailedList.load_or_create("c", "p/")
        fl.add_or_update("p/bad.parquet", "corrupt", "reason1")
        fl.add_or_update("p/bad.parquet", "network", "reason2")
        assert len(fl.entries) == 1
        assert fl.entries[0]["type"] == "network"
        assert fl.entries[0]["reason"] == "reason2"

    def test_persists_to_disk(self, checkpoint_dir):
        fl = FailedList.load_or_create("c", "p/")
        fl.add_or_update("p/bad.parquet", "corrupt", "oops")
        path = FailedList.failed_list_path("c", "p/")
        data = json.loads(path.read_text(encoding="utf-8"))
        assert len(data["entries"]) == 1

    def test_counts_by_type(self, checkpoint_dir):
        fl = FailedList.load_or_create("c", "p/")
        fl.add_or_update("p/a.parquet", "corrupt", "r1")
        fl.add_or_update("p/b.parquet", "network", "r2")
        fl.add_or_update("p/c.parquet", "network", "r3")
        assert fl.corrupt_count == 1
        assert fl.network_count == 2

    def test_is_thread_safe(self, checkpoint_dir):
        fl = FailedList.load_or_create("c", "p/")
        blobs = [f"p/file-{i:04d}.parquet" for i in range(50)]
        threads = [
            threading.Thread(target=fl.add_or_update, args=(b, "network", "err"))
            for b in blobs
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(fl.entries) == 50


class TestFailedListRemove:
    def test_removes_existing_entry(self, checkpoint_dir):
        fl = FailedList.load_or_create("c", "p/")
        fl.add_or_update("p/bad.parquet", "corrupt", "oops")
        fl.remove("p/bad.parquet")
        assert fl.entries == []

    def test_remove_nonexistent_is_noop(self, checkpoint_dir):
        fl = FailedList.load_or_create("c", "p/")
        fl.remove("p/ghost.parquet")  # must not raise
        assert fl.entries == []

    def test_remove_persists_to_disk(self, checkpoint_dir):
        fl = FailedList.load_or_create("c", "p/")
        fl.add_or_update("p/bad.parquet", "corrupt", "oops")
        fl.remove("p/bad.parquet")
        path = FailedList.failed_list_path("c", "p/")
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["entries"] == []


class TestFailedListClear:
    def test_clear_removes_all_entries(self, checkpoint_dir):
        fl = FailedList.load_or_create("c", "p/")
        fl.add_or_update("p/a.parquet", "corrupt", "r")
        fl.add_or_update("p/b.parquet", "network", "r")
        fl.clear()
        assert fl.entries == []

    def test_blob_names_returns_all_names(self, checkpoint_dir):
        fl = FailedList.load_or_create("c", "p/")
        fl.add_or_update("p/a.parquet", "corrupt", "r")
        fl.add_or_update("p/b.parquet", "network", "r")
        assert set(fl.blob_names()) == {"p/a.parquet", "p/b.parquet"}
