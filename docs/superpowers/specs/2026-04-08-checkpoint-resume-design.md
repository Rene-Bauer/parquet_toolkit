# Checkpoint / Resume + Failed List Feature Design

**Date:** 2026-04-08  
**Status:** Approved

---

## Overview

Two independent but co-implemented features:

1. **Checkpoint (Resume):** Persist a cursor to disk so interrupted runs can resume from where they left off.
2. **Failed List:** Persist a separate log of all files that failed (corrupt or network) across runs, so they can be identified and optionally retried later.

Both files live in `checkpoints/` (gitignored), keyed by container + prefix, but stored as separate files.

---

## Storage

**Location:** `checkpoints/` in the repository root, added to `.gitignore`.

**Two files per run key:**

| File | Purpose |
|---|---|
| `mycontainer__raw_events_2026_03__checkpoint.json` | Cursor for resume |
| `mycontainer__raw_events_2026_03__failed.json` | Persistent failed file log |

**Filename key:** container + prefix with slashes and special characters replaced by `_`. Double underscore separates container from prefix, suffix identifies file type.

**Atomic writes:** All writes use temp file + rename to prevent corruption on crash.

---

## Checkpoint File Format

```json
{
  "container": "my-container",
  "prefix": "raw/events/2026/03/",
  "output_prefix": "processed/events/2026/03/",
  "status": "in_progress",
  "created_at": "2026-04-08T10:00:00",
  "updated_at": "2026-04-08T11:47:32",
  "cursor": "raw/events/2026/03/01/10/part-0042.parquet"
}
```

**`status` values:** `in_progress` | `complete`

**Cursor semantics:** All blobs at or before `cursor` in the sorted Azure listing are skipped on resume. Due to parallel workers, a small number of files near the cursor boundary may be re-processed — safe since transforms are idempotent.

---

## Failed List File Format

```json
{
  "container": "my-container",
  "prefix": "raw/events/2026/03/",
  "created_at": "2026-04-08T10:00:00",
  "updated_at": "2026-04-08T11:47:32",
  "entries": [
    {
      "name": "raw/events/2026/03/01/09/part-0099.parquet",
      "type": "corrupt",
      "reason": "Corrupted Parquet file: Invalid magic bytes",
      "failed_at": "2026-04-08T10:45:12"
    },
    {
      "name": "raw/events/2026/03/01/10/part-0007.parquet",
      "type": "network",
      "reason": "Azure error: Connection timeout after 3 attempts",
      "failed_at": "2026-04-08T11:02:44"
    }
  ]
}
```

**`type` values:** `corrupt` | `network`

The failed list is **never auto-reset** — it accumulates across runs. Entries are deduplicated by blob name (re-running a previously failed file replaces its entry on success, or updates it on renewed failure). The user can clear it manually or via a GUI action.

---

## New Module: `parquet_transform/checkpoint.py`

Pure Python, no Qt dependency. Contains two classes.

### `RunCheckpoint`

```python
class RunCheckpoint:
    @staticmethod
    def checkpoint_path(container: str, prefix: str) -> Path: ...

    @staticmethod
    def load_or_create(
        container: str, prefix: str, output_prefix: str | None
    ) -> "RunCheckpoint": ...

    def is_complete(self) -> bool: ...

    def should_skip(self, blob_name: str, all_blobs: list[str]) -> bool:
        """True if blob_name is at or before cursor in the sorted blob list."""

    def advance_cursor(self, blob_name: str) -> None:
        """Update cursor if blob_name sorts later than current. Thread-safe, atomic write."""

    def mark_complete(self) -> None:
        """Set status=complete and write to disk."""

    def reset(self) -> None:
        """Clear cursor, set status=in_progress, write to disk."""

    @property
    def cursor(self) -> str | None: ...

    @property
    def completed_count(self) -> int:
        """Index of cursor in all_blobs — used in resume dialog."""
```

### `FailedList`

```python
class FailedList:
    @staticmethod
    def failed_list_path(container: str, prefix: str) -> Path: ...

    @staticmethod
    def load_or_create(container: str, prefix: str) -> "FailedList": ...

    def add_or_update(self, blob_name: str, type: str, reason: str) -> None:
        """Add new entry or update existing entry for blob_name. Thread-safe, atomic write."""

    def remove(self, blob_name: str) -> None:
        """Remove entry (e.g. after successful retry). Thread-safe, atomic write."""

    def clear(self) -> None:
        """Remove all entries."""

    @property
    def entries(self) -> list[dict]: ...

    @property
    def corrupt_count(self) -> int: ...

    @property
    def network_count(self) -> int: ...

    def blob_names(self) -> list[str]:
        """All failed blob names — used to re-add to blob list for retry."""
```

---

## Worker Integration (`gui/workers.py`)

`TransformWorker.__init__` receives two new optional parameters:
```python
checkpoint: RunCheckpoint | None = None
failed_list: FailedList | None = None
```

**At run start** (after blob listing):
```python
if checkpoint:
    skipped = sum(1 for b in blob_names if checkpoint.should_skip(b, blob_names))
    blob_names = [b for b in blob_names if not checkpoint.should_skip(b, blob_names)]
    # log: f"[Checkpoint] Resuming — skipping {skipped} already-processed files"
```

**In `finalize_success()`:**
```python
if self._checkpoint:
    self._checkpoint.advance_cursor(blob_name)
if self._failed_list:
    self._failed_list.remove(blob_name)  # clear if previously failed
```

**In `finalize_failure()`** for corrupt (non-retriable) and final network failures:
```python
if self._failed_list:
    ftype = "corrupt" if not retriable else "network"
    self._failed_list.add_or_update(blob_name, type=ftype, reason=short_error)
```

**At run end** (no cancel):
```python
if self._checkpoint:
    self._checkpoint.mark_complete()
    # log: "[Checkpoint] Run marked as complete"
```

---

## GUI Integration (`gui/main_window.py`)

**On "Start" click**, before creating `TransformWorker`:

**Checkpoint dialog flow:**
1. Check `RunCheckpoint.checkpoint_path(container, prefix)`
2. If `in_progress` → dialog: *"A previous run was interrupted (~X files already processed). Resume or start fresh?"*
   - Resume → load checkpoint, pass to worker
   - Start fresh → `checkpoint.reset()`, pass to worker
3. If `complete` → dialog: *"The previous run for this prefix completed successfully. Start fresh?"*
   - Yes → `checkpoint.reset()`, pass to worker
   - No → abort

**Failed list dialog flow** (shown after checkpoint dialog, only on resume):
1. Load `FailedList` for container + prefix
2. If `entries` non-empty → dialog: *"X file(s) failed in a previous run (Y corrupt, Z network). Retry them this run?"*
   - Yes → `failed_list.blob_names()` prepended to blob list passed to worker
   - No → failed files remain in list but are not retried this run

**Log messages (English):**
- `[Checkpoint] Resuming run — skipping 1 234 files already processed`
- `[Checkpoint] Run marked as complete`
- `[Failed List] Recorded corrupt file: raw/.../part-0099.parquet`
- `[Failed List] Recorded network failure: raw/.../part-0007.parquet`
- `[Failed List] Cleared previously failed file: raw/.../part-0007.parquet (succeeded on retry)`

---

## `.gitignore`

Add to repo root `.gitignore` (create if not present):
```
checkpoints/
```

---

## Files Changed

| File | Change |
|---|---|
| `parquet_transform/checkpoint.py` | **New** — `RunCheckpoint` + `FailedList` classes |
| `gui/workers.py` | Add `checkpoint` + `failed_list` parameters |
| `gui/main_window.py` | Resume/fresh-start dialogs, failed-list dialog, pass both to worker |
| `checkpoints/` | New folder (gitignored) |
| `.gitignore` | Add `checkpoints/` entry |
