# Checkpoint / Resume + Failed List Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a cursor-based local checkpoint system that resumes interrupted transform runs and a separate persistent failed-file list for tracking and retrying failed/corrupt files.

**Architecture:** A new `parquet_transform/checkpoint.py` module provides two thread-safe classes (`RunCheckpoint`, `FailedList`) that write atomic JSON files to a local `checkpoints/` folder. `TransformWorker` receives both objects as optional parameters and calls them directly from worker threads. The GUI checks for existing checkpoints before starting a run and shows dialogs to resume, start fresh, or retry failed files.

**Tech Stack:** Python stdlib (`json`, `threading`, `os`, `pathlib`, `re`, `datetime`), PyQt6 `QMessageBox` for dialogs.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `parquet_transform/checkpoint.py` | **Create** | `RunCheckpoint` + `FailedList` — all checkpoint logic, no Qt |
| `tests/test_checkpoint.py` | **Create** | Tests for both classes |
| `gui/workers.py` | **Modify** | Accept `checkpoint` + `failed_list` params, call them on success/failure/end |
| `gui/main_window.py` | **Modify** | Show resume/retry dialogs, pass objects to worker |
| `.gitignore` | **Modify** | Add `checkpoints/` entry |
| `checkpoints/` | **Create folder** | Runtime data, gitignored |

---

## Task 1: Folder setup + .gitignore

**Files:**
- Create: `checkpoints/.keep` (empty, just to create the folder)
- Modify: `.gitignore`

- [ ] **Step 1: Create the checkpoints folder with a .keep file**

```bash
mkdir -p checkpoints
touch checkpoints/.keep
```

- [ ] **Step 2: Add checkpoints/ to .gitignore**

Open `.gitignore` and append at the end:

```
# Local run checkpoints (contain blob paths, not for version control)
checkpoints/
```

- [ ] **Step 3: Commit**

```bash
git add checkpoints/.keep .gitignore
git commit -m "chore: add checkpoints/ folder (gitignored) for run resume data"
```

---

## Task 2: Write failing tests for RunCheckpoint

**Files:**
- Create: `tests/test_checkpoint.py`

- [ ] **Step 1: Create the test file**

```python
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
        # Write a checkpoint file manually
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
        blobs = ["p/a.parquet", "p/b.parquet", "p/c.parquet"]
        assert not cp.should_skip("p/a.parquet", blobs)

    def test_skips_blobs_at_or_before_cursor(self, checkpoint_dir):
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        cp.advance_cursor("p/b.parquet")
        blobs = ["p/a.parquet", "p/b.parquet", "p/c.parquet"]
        assert cp.should_skip("p/a.parquet", blobs)
        assert cp.should_skip("p/b.parquet", blobs)
        assert not cp.should_skip("p/c.parquet", blobs)

    def test_does_not_skip_blobs_after_cursor(self, checkpoint_dir):
        cp = RunCheckpoint.load_or_create("c", "p/", None)
        cp.advance_cursor("p/2026/03/01/part-0010.parquet")
        blobs = ["p/2026/03/01/part-0010.parquet", "p/2026/03/02/part-0001.parquet"]
        assert not cp.should_skip("p/2026/03/02/part-0001.parquet", blobs)


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
        # Cursor must be the lexicographically largest blob
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
```

- [ ] **Step 2: Run tests to confirm they all fail**

```bash
cd D:/Coding/parquet_schema_modificator
pytest tests/test_checkpoint.py -v 2>&1 | head -30
```

Expected: `ImportError: cannot import name 'FailedList' from 'parquet_transform.checkpoint'` (module does not exist yet).

- [ ] **Step 3: Commit the test file**

```bash
git add tests/test_checkpoint.py
git commit -m "test: add failing tests for RunCheckpoint and FailedList"
```

---

## Task 3: Implement checkpoint.py

**Files:**
- Create: `parquet_transform/checkpoint.py`

- [ ] **Step 1: Create the module**

```python
"""
Local checkpoint and failed-file tracking for transform runs.

Two independent, thread-safe classes:
- RunCheckpoint  — cursor-based resume (one file per container+prefix)
- FailedList     — persistent failed-file log (separate file, same key)

Both write atomically via temp-file + os.replace to survive crashes.
"""
from __future__ import annotations

import json
import os
import re
import threading
from datetime import datetime
from pathlib import Path

# Resolved relative to this file so it always points to repo root/checkpoints/
_CHECKPOINTS_DIR: Path = Path(__file__).parent.parent / "checkpoints"


def _sanitize_key(container: str, prefix: str) -> str:
    """Build a filesystem-safe key: container__prefix (special chars → _)."""
    c = re.sub(r"[^a-zA-Z0-9]", "_", container)
    p = re.sub(r"[^a-zA-Z0-9]", "_", prefix)
    return f"{c}__{p}"


def _atomic_write(path: Path, data: dict) -> None:
    """Write *data* as JSON to *path* atomically via a sibling .tmp file."""
    _CHECKPOINTS_DIR.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# RunCheckpoint
# ---------------------------------------------------------------------------

class RunCheckpoint:
    """
    Cursor-based checkpoint for a single transform run.

    The cursor is the blob name of the last successfully processed file.
    On resume, all blobs that sort at or before the cursor are skipped.
    Blob names from Azure are alphabetically sorted, which matches the
    chronological folder structure (2026/03/01/10/...).
    """

    def __init__(self, path: Path, data: dict) -> None:
        self._path = path
        self._data = data
        self._lock = threading.Lock()

    @staticmethod
    def checkpoint_path(container: str, prefix: str) -> Path:
        """Return the .json path for this container+prefix key."""
        return _CHECKPOINTS_DIR / f"{_sanitize_key(container, prefix)}__checkpoint.json"

    @staticmethod
    def load_or_create(
        container: str,
        prefix: str,
        output_prefix: str | None,
    ) -> "RunCheckpoint":
        """Load an existing checkpoint or create a fresh in_progress one."""
        path = RunCheckpoint.checkpoint_path(container, prefix)
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            now = _now()
            data = {
                "container": container,
                "prefix": prefix,
                "output_prefix": output_prefix,
                "status": "in_progress",
                "created_at": now,
                "updated_at": now,
                "cursor": None,
            }
        return RunCheckpoint(path, data)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def is_complete(self) -> bool:
        """True when the run finished without cancellation."""
        return self._data.get("status") == "complete"

    def should_skip(self, blob_name: str, all_blobs: list[str]) -> bool:
        """True if *blob_name* is at or before the cursor in the sorted listing."""
        cursor = self._data.get("cursor")
        if cursor is None:
            return False
        return blob_name <= cursor

    @property
    def cursor(self) -> str | None:
        return self._data.get("cursor")

    # ------------------------------------------------------------------
    # Mutations (all thread-safe, all write to disk)
    # ------------------------------------------------------------------

    def advance_cursor(self, blob_name: str) -> None:
        """Move cursor forward if *blob_name* sorts later than the current cursor."""
        with self._lock:
            current = self._data.get("cursor")
            if current is None or blob_name > current:
                self._data["cursor"] = blob_name
                self._data["updated_at"] = _now()
                _atomic_write(self._path, self._data)

    def mark_complete(self) -> None:
        """Mark the run as fully complete."""
        with self._lock:
            self._data["status"] = "complete"
            self._data["updated_at"] = _now()
            _atomic_write(self._path, self._data)

    def reset(self) -> None:
        """Clear cursor and status — use before a fresh-start run."""
        with self._lock:
            self._data["status"] = "in_progress"
            self._data["cursor"] = None
            self._data["updated_at"] = _now()
            _atomic_write(self._path, self._data)


# ---------------------------------------------------------------------------
# FailedList
# ---------------------------------------------------------------------------

class FailedList:
    """
    Persistent log of files that failed (corrupt or network) across runs.

    Entries are deduplicated by blob name — re-running a failed file that
    succeeds removes the entry via remove(); renewed failures update it.
    Never auto-reset — the user controls clearing via clear() or GUI action.
    """

    def __init__(self, path: Path, data: dict) -> None:
        self._path = path
        self._data = data
        self._lock = threading.Lock()

    @staticmethod
    def failed_list_path(container: str, prefix: str) -> Path:
        """Return the .json path for this container+prefix key."""
        return _CHECKPOINTS_DIR / f"{_sanitize_key(container, prefix)}__failed.json"

    @staticmethod
    def load_or_create(container: str, prefix: str) -> "FailedList":
        """Load an existing failed list or create an empty one."""
        path = FailedList.failed_list_path(container, prefix)
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            now = _now()
            data = {
                "container": container,
                "prefix": prefix,
                "created_at": now,
                "updated_at": now,
                "entries": [],
            }
        return FailedList(path, data)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    @property
    def entries(self) -> list[dict]:
        """Snapshot of all failed entries."""
        return list(self._data["entries"])

    @property
    def corrupt_count(self) -> int:
        return sum(1 for e in self._data["entries"] if e.get("type") == "corrupt")

    @property
    def network_count(self) -> int:
        return sum(1 for e in self._data["entries"] if e.get("type") == "network")

    def blob_names(self) -> list[str]:
        """All failed blob names — use to re-add to a blob list for retry."""
        return [e["name"] for e in self._data["entries"]]

    # ------------------------------------------------------------------
    # Mutations (all thread-safe, all write to disk)
    # ------------------------------------------------------------------

    def add_or_update(self, blob_name: str, type: str, reason: str) -> None:
        """Record a failure. Updates an existing entry if the blob already failed."""
        with self._lock:
            entries = self._data["entries"]
            for entry in entries:
                if entry["name"] == blob_name:
                    entry["type"] = type
                    entry["reason"] = reason
                    entry["failed_at"] = _now()
                    break
            else:
                entries.append({
                    "name": blob_name,
                    "type": type,
                    "reason": reason,
                    "failed_at": _now(),
                })
            self._data["updated_at"] = _now()
            _atomic_write(self._path, self._data)

    def remove(self, blob_name: str) -> None:
        """Remove entry for *blob_name* (e.g. after a successful retry)."""
        with self._lock:
            before = len(self._data["entries"])
            self._data["entries"] = [
                e for e in self._data["entries"] if e["name"] != blob_name
            ]
            if len(self._data["entries"]) != before:
                self._data["updated_at"] = _now()
                _atomic_write(self._path, self._data)

    def clear(self) -> None:
        """Remove all entries."""
        with self._lock:
            self._data["entries"] = []
            self._data["updated_at"] = _now()
            _atomic_write(self._path, self._data)
```

- [ ] **Step 2: Run the tests**

```bash
pytest tests/test_checkpoint.py -v
```

Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add parquet_transform/checkpoint.py
git commit -m "feat: add RunCheckpoint and FailedList classes"
```

---

## Task 4: Integrate checkpoint into TransformWorker

**Files:**
- Modify: `gui/workers.py`

The worker gets two new optional constructor parameters. It uses them at three points: after listing (filter blobs), after each success (advance cursor + remove from failed list), and at run end (mark complete). Failed files are recorded from `finalize_failure`.

- [ ] **Step 1: Add import at top of workers.py**

Find the existing imports block at the top of `gui/workers.py` and add:

```python
from parquet_transform.checkpoint import FailedList, RunCheckpoint
```

- [ ] **Step 2: Add parameters to TransformWorker.__init__**

Find the `__init__` signature (line ~191) and add two parameters before `parent=None`:

```python
    def __init__(
        self,
        connection_string: str,
        container: str,
        prefix: str,
        col_configs: list[ColumnConfig],
        output_prefix: str | None,
        dry_run: bool = False,
        worker_count: int = 1,
        max_attempts: int = 1,
        blob_names: list[str] | None = None,
        blob_sizes: dict[str, int] | None = None,
        autoscale: bool = False,
        checkpoint: RunCheckpoint | None = None,
        failed_list: FailedList | None = None,
        retry_failed: bool = False,
        parent=None,
    ):
```

Then store them at the end of `__init__` (after `self._autoscale = autoscale`):

```python
        self._checkpoint = checkpoint
        self._failed_list = failed_list
        self._retry_failed = retry_failed
```

- [ ] **Step 3: Filter blob list after listing, and add failed files if retry requested**

In `run()`, find the block after blob listing is complete and `total = len(blob_names)` is set (around line ~258). Insert immediately after `total = len(blob_names)`:

```python
        # Checkpoint: skip already-processed blobs on resume
        if self._checkpoint and blob_names:
            before = len(blob_names)
            blob_names = [b for b in blob_names if not self._checkpoint.should_skip(b, blob_names)]
            skipped = before - len(blob_names)
            if skipped > 0:
                self.file_error.emit(
                    "(checkpoint)",
                    f"[Checkpoint] Resuming — skipping {skipped} already-processed file(s)"
                )

        # Failed list: prepend previously failed blobs for retry (deduplicated)
        if self._retry_failed and self._failed_list:
            failed_names = self._failed_list.blob_names()
            existing = set(blob_names)
            extra = [n for n in failed_names if n not in existing]
            blob_names = extra + blob_names
            if extra:
                for name in extra:
                    entry_type = next(
                        (e["type"] for e in self._failed_list.entries if e["name"] == name),
                        "unknown",
                    )
                    self.file_error.emit(
                        "(checkpoint)",
                        f"[Failed List] Retrying previously failed file: {name} ({entry_type})"
                    )

        total = len(blob_names)
```

> **Note:** Remove or replace the original `total = len(blob_names)` line that was already there — it is now set at the end of this block.

- [ ] **Step 4: Call advance_cursor and failed_list.remove in finalize_success**

Find the `finalize_success` inner function (around line ~299). Add at the end of its body, after `completed_so_far = completed`:

```python
            if self._checkpoint:
                self._checkpoint.advance_cursor(blob_name)
            if self._failed_list:
                self._failed_list.remove(blob_name)
```

- [ ] **Step 5: Call failed_list.add_or_update in finalize_failure**

Find the `finalize_failure` inner function (around line ~310). Add at the end of its body, after `completed_so_far = completed`:

```python
            if self._failed_list:
                ftype = "network" if retriable else "corrupt"
                self._failed_list.add_or_update(blob_name, ftype, short_error)
                self.file_error.emit(
                    "(checkpoint)",
                    f"[Failed List] Recorded {ftype} failure: {blob_name}"
                )
```

- [ ] **Step 6: Mark checkpoint complete at run end**

Find the `self.finished.emit(...)` call at the end of `run()` (around line ~505). Insert before it:

```python
        if self._checkpoint and not self._cancel_event.is_set():
            self._checkpoint.mark_complete()
            self.file_error.emit("(checkpoint)", "[Checkpoint] Run marked as complete")
```

- [ ] **Step 7: Run existing tests to confirm nothing is broken**

```bash
pytest tests/ -v
```

Expected: all existing tests pass (worker changes are additive — no existing behavior changed).

- [ ] **Step 8: Commit**

```bash
git add gui/workers.py
git commit -m "feat: integrate RunCheckpoint and FailedList into TransformWorker"
```

---

## Task 5: GUI — checkpoint and failed-list dialogs

**Files:**
- Modify: `gui/main_window.py`

The checkpoint check happens in `_start_next_prefix` (per-prefix, before calling `_start_transform`). The `_start_transform` method receives two new optional parameters and passes them to the worker.

- [ ] **Step 1: Add imports to main_window.py**

Find the existing imports block. Add `QMessageBox` to the QtWidgets import and add the checkpoint import:

```python
from PyQt6.QtWidgets import (
    QButtonGroup,
    QCheckBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QSpinBox,
    QStatusBar,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from parquet_transform.checkpoint import FailedList, RunCheckpoint
```

- [ ] **Step 2: Add a helper method _resolve_checkpoint to MainWindow**

Add this method to `MainWindow` (e.g. just before `_start_next_prefix`):

```python
    def _resolve_checkpoint(
        self, container: str, prefix: str, output_prefix: str | None
    ) -> tuple[RunCheckpoint, FailedList, bool]:
        """
        Check for an existing checkpoint, show dialogs as needed, and return
        (checkpoint, failed_list, retry_failed).

        Dialog logic:
        - in_progress checkpoint → ask Resume / Start Fresh
        - complete checkpoint    → ask Start Fresh (or abort)
        - failed entries present on resume → ask Retry Failed Files
        """
        cp = RunCheckpoint.load_or_create(container, prefix, output_prefix)
        fl = FailedList.load_or_create(container, prefix)
        retry_failed = False

        if cp.is_complete():
            msg = QMessageBox(self)
            msg.setWindowTitle("Previous Run Complete")
            msg.setText(
                f"The previous run for prefix '{prefix}' completed successfully.\n\n"
                "Start a fresh run?"
            )
            fresh_btn = msg.addButton("Start Fresh", QMessageBox.ButtonRole.AcceptRole)
            msg.addButton("Cancel", QMessageBox.ButtonRole.RejectRole)
            msg.exec()
            if msg.clickedButton() != fresh_btn:
                return cp, fl, False  # caller checks cp.is_complete() to abort
            cp.reset()

        elif cp.cursor is not None:
            # in_progress with a cursor — previous run was interrupted
            msg = QMessageBox(self)
            msg.setWindowTitle("Resume Previous Run")
            msg.setText(
                f"A previous run for prefix '{prefix}' was interrupted.\n"
                f"Last processed: {cp.cursor}\n\n"
                "Resume from where it left off, or start fresh?"
            )
            resume_btn = msg.addButton("Resume", QMessageBox.ButtonRole.AcceptRole)
            fresh_btn = msg.addButton("Start Fresh", QMessageBox.ButtonRole.DestructiveRole)
            msg.exec()
            if msg.clickedButton() == fresh_btn:
                cp.reset()
                fl.clear()

        # Failed-list dialog: shown only when resuming (cursor present after above)
        if fl.entries and cp.cursor is not None:
            total_failed = len(fl.entries)
            msg = QMessageBox(self)
            msg.setWindowTitle("Previously Failed Files")
            msg.setText(
                f"{total_failed} file(s) failed in a previous run "
                f"({fl.corrupt_count} corrupt, {fl.network_count} network).\n\n"
                "Retry them in this run?"
            )
            retry_btn = msg.addButton("Retry Failed Files", QMessageBox.ButtonRole.AcceptRole)
            msg.addButton("Skip", QMessageBox.ButtonRole.RejectRole)
            msg.exec()
            retry_failed = (msg.clickedButton() == retry_btn)

        return cp, fl, retry_failed
```

- [ ] **Step 3: Call _resolve_checkpoint from _start_next_prefix**

Find `_start_next_prefix` (around line ~527). It currently ends with:

```python
        self._start_transform(blob_names=None, prefix_override=prefix)
```

Replace that last line with:

```python
        conn = self._conn_str_edit.text().strip()
        container = self._container_edit.text().strip()
        output_prefix: str | None = None
        if self._newprefix_radio.isChecked():
            output_prefix = self._output_prefix_edit.text().strip() or None

        checkpoint, failed_list, retry_failed = self._resolve_checkpoint(
            container, prefix, output_prefix
        )

        # If user cancelled the "complete" dialog, abort this prefix
        if checkpoint.is_complete() and checkpoint.cursor is not None:
            self._log_info(f"[Checkpoint] Skipping prefix '{prefix}' — already complete.")
            self._current_prefix_index += 1
            self._start_next_prefix()
            return

        self._start_transform(
            blob_names=None,
            prefix_override=prefix,
            checkpoint=checkpoint,
            failed_list=failed_list,
            retry_failed=retry_failed,
        )
```

- [ ] **Step 4: Add checkpoint/failed_list parameters to _start_transform**

Find the `_start_transform` method signature (around line ~547):

```python
    def _start_transform(
        self,
        blob_names: list[str] | None = None,
        blob_sizes: dict[str, int] | None = None,
        prefix_override: str | None = None,
    ) -> None:
```

Change to:

```python
    def _start_transform(
        self,
        blob_names: list[str] | None = None,
        blob_sizes: dict[str, int] | None = None,
        prefix_override: str | None = None,
        checkpoint: RunCheckpoint | None = None,
        failed_list: FailedList | None = None,
        retry_failed: bool = False,
    ) -> None:
```

Then find the `TransformWorker(...)` constructor call (around line ~586) and add the three new keyword arguments before the closing paren:

```python
        self._transform_worker = TransformWorker(
            connection_string=conn,
            container=container,
            prefix=prefix,
            col_configs=col_configs,
            output_prefix=output_prefix,
            dry_run=self._dry_run,
            worker_count=worker_count,
            max_attempts=max_attempts,
            blob_names=blob_names,
            blob_sizes=blob_sizes,
            autoscale=autoscale,
            checkpoint=checkpoint,
            failed_list=failed_list,
            retry_failed=retry_failed,
        )
```

- [ ] **Step 5: Smoke-test the application**

```bash
cd D:/Coding/parquet_schema_modificator
python main.py
```

Expected: application opens without errors. No dialogs appear when starting a fresh run on a prefix that has no checkpoint file yet.

- [ ] **Step 6: Run all tests**

```bash
pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add gui/main_window.py
git commit -m "feat: add checkpoint resume and failed-list dialogs to GUI"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| Cursor-based checkpoint file per container+prefix | Task 3 |
| Separate failed list file | Task 3 |
| `checkpoints/` folder, gitignored | Task 1 |
| Atomic writes | Task 3 (`_atomic_write`) |
| `should_skip` filters blobs at run start | Task 4 Step 3 |
| `advance_cursor` on success | Task 4 Step 4 |
| `failed_list.remove` on successful retry | Task 4 Step 4 |
| `failed_list.add_or_update` on corrupt/network failure | Task 4 Step 5 |
| `mark_complete` at run end | Task 4 Step 6 |
| Resume dialog (in_progress checkpoint) | Task 5 Step 2 |
| Fresh-start dialog (complete checkpoint) | Task 5 Step 2 |
| Retry failed files dialog | Task 5 Step 2 |
| Log messages in English with `[Checkpoint]` / `[Failed List]` prefix | Tasks 4 + 5 |
| Thread safety | Task 3 (locks) + Task 2 (tests) |

All spec requirements covered. No placeholders. Type signatures consistent across tasks.
