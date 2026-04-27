# Collector Subfolder Resume — Design

**Date:** 2026-04-27
**Status:** Approved

---

## Problem

The DataCollector processes source prefixes that contain virtual subfolders one-by-one, saving a checkpoint after each completed subfolder. If a run is cancelled while processing a subfolder, that subfolder is not marked done. On the next run it will be rerun from the beginning — correct behaviour — but the panel currently:

1. Shows a generic "Previous run interrupted — Start Fresh?" dialog that gives no information about subfolder progress and misleadingly implies a full reset.
2. Has no "Resume" option; the user must click "Start Fresh", which sounds destructive even though completed subfolders are preserved.
3. Cannot name which subfolder was interrupted, because the checkpoint only stores completed subfolders, not the one in progress.

Additionally, all internal identifiers and user-visible strings use the word "Archive", which does not reflect how the feature is described to users.

---

## Goals

1. Show a clear resume dialog when a subfolder run was interrupted, naming the interrupted subfolder and how many were already completed.
2. Give the user explicit **Resume** / **Start Fresh** / **Cancel** choices, with correct semantics for each.
3. Remove all "Archive" terminology from internal names, log messages, and user-visible strings.

---

## Non-Goals

- Blob-level checkpointing within a subfolder (the interrupted subfolder always reruns from blob 1).
- Any changes to the flat (no-subfolder) run flow.
- UI changes beyond the resume dialog.

---

## Design

### 1. Rename: `ArchiveCheckpoint` → `SubfolderCheckpoint`

A mechanical rename across all files. No logic changes.

| Old name | New name |
|---|---|
| `ArchiveCheckpoint` (class) | `SubfolderCheckpoint` |
| `_ARCHIVE_CHECKPOINT_DIR` | `_SUBFOLDER_CHECKPOINT_DIR` |
| `__archive.json` (filename suffix) | `__subfolder.json` |
| `_run_archive_mode()` | `_run_subfolder_mode()` |
| `_archive_inner` | `_subfolder_inner` |
| `_leaf` parameter | `_is_subfolder_run` |
| `[Archive]` log prefixes | removed |
| `tests/test_archive_checkpoint.py` | `tests/test_subfolder_checkpoint.py` |
| `test_archive_mode_*` test names | `test_subfolder_mode_*` |

**Note on existing checkpoint files:** Any `__archive.json` files already present in `CollectorCheckpoint/` will not be discovered after the rename (the filename suffix changes). Since that directory is gitignored and machine-local, this is acceptable — affected users lose their in-progress checkpoint and the next run restarts from subfolder 1.

---

### 2. `SubfolderCheckpoint` additions (`checkpoint.py`)

Three additions to the class:

#### 2a. `checkpoint_path()` and `load_existing()` static/class methods

Exposes the path without requiring a full `load_or_create`, so the panel can check existence without loading the file.

```python
@staticmethod
def checkpoint_path(
    container: str,
    source_prefix: str,
    filter_col: str,
    filter_values: list[str],
    _checkpoint_dir: Path | None = None,
) -> Path:
    directory = _checkpoint_dir or _SUBFOLDER_CHECKPOINT_DIR
    return SubfolderCheckpoint._make_path(
        container, source_prefix, filter_col, filter_values, directory
    )

@staticmethod
def load_existing(path: Path) -> "SubfolderCheckpoint | None":
    """Load a checkpoint from an existing file for read-only inspection.

    Returns None if the file does not exist.
    Raises RuntimeError if the file is corrupt.
    """
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Subfolder checkpoint is corrupt: {path}\n"
                f"Delete the file to start fresh. Detail: {exc}"
            ) from exc
    return SubfolderCheckpoint(path, data)
```

#### 2b. `mark_in_progress(subfolder: str)` mutation

Called by `_run_subfolder_mode` just before starting each inner run. Atomically writes `"in_progress_subfolder"` to the checkpoint file so the panel can read the interrupted subfolder name on the next start.

```python
def mark_in_progress(self, subfolder: str) -> None:
    with self._lock:
        self._data["in_progress_subfolder"] = subfolder
        self._data["updated_at"] = _now()
        self._write()
```

#### 2c. `in_progress_subfolder` property

```python
@property
def in_progress_subfolder(self) -> str | None:
    return self._data.get("in_progress_subfolder")
```

#### 2d. `mark_done()` clears `in_progress_subfolder`

The existing `mark_done()` adds one line before `self._write()`:

```python
self._data["in_progress_subfolder"] = None
```

This ensures the field is `None` for any fully completed subfolder, and only holds a value when a run was genuinely interrupted.

---

### 3. `_run_subfolder_mode` addition (`workers.py`)

One line added per loop iteration, immediately before `inner.run()`:

```python
checkpoint.mark_in_progress(subfolder)
self._subfolder_inner = inner
inner.run()
```

Log messages drop the `[Archive]` prefix:

| Old | New |
|---|---|
| `[Archive] {n} Subfolder gefunden — …` | `{n} Subfolder gefunden — …` |
| `[Archive] Subfolder {i}/{n}: {sf} — übersprungen` | `Subfolder {i}/{n}: {sf} — übersprungen` |
| `[Archive] Subfolder {i}/{n}: {sf} — wird verarbeitet …` | `Subfolder {i}/{n}: {sf} — wird verarbeitet …` |
| `[Archive] Subfolder {sf} fertig: …` | `Subfolder {sf} fertig: …` |
| `[Archive] Alle {n} Subfolder verarbeitet — …` | `Alle {n} Subfolder verarbeitet — …` |

---

### 4. Resume dialog in `_on_collect()` (`collector_panel.py`)

#### Detection

At the top of `_on_collect()`, before the existing `CollectorRunRecord` dialog block:

```python
_cp_path = SubfolderCheckpoint.checkpoint_path(
    container,
    self._source_edit.text().strip(),
    self._filter_combo.currentText(),
    filter_values,
)
_cp_exists = _cp_path.exists()
```

#### Dialog (shown when `_cp_exists`)

Load the checkpoint to read `done_count` and `in_progress_subfolder`. To avoid passing dummy `output_prefix`/`output_container` values to `load_or_create`, `SubfolderCheckpoint` exposes a `load_existing(path)` class method that reads an existing file without requiring those fields:

```python
try:
    _cp = SubfolderCheckpoint.load_existing(_cp_path)
except RuntimeError:
    _cp = None  # corrupt — fall through to existing flow
```

`load_existing` raises `RuntimeError` on corrupt JSON and returns `None` if the file does not exist.

If `_cp` is valid and (`_cp.done_count > 0` or `_cp.in_progress_subfolder`):

- Suppress the `CollectorRunRecord` in-progress dialog.
- Show:

  > **Previous Run Interrupted**
  >
  > *{N} subfolder(s) completed.*
  > *Interrupted on: `{subfolder}` — resuming will rerun it from the beginning.*
  >
  > (If `in_progress_subfolder` is `None` but `done_count > 0`, show only the first line.)

  Buttons: **Resume** · **Start Fresh** · **Cancel**

#### Button semantics

| Button | Action |
|---|---|
| **Resume** | Proceed to worker creation. `CollectorRunRecord.mark_in_progress()` called as usual. `SubfolderCheckpoint` file untouched — `_run_subfolder_mode` picks it up automatically. |
| **Start Fresh** | `_cp_path.unlink(missing_ok=True)` to delete the checkpoint file. `run_record.reset()`. Proceed to worker creation. |
| **Cancel** | Return immediately. |

If `_cp_exists` is `False`, the existing `CollectorRunRecord` dialog flow runs unchanged.

---

## Files Changed

| File | Change |
|---|---|
| `parquet_transform/checkpoint.py` | Rename `ArchiveCheckpoint` → `SubfolderCheckpoint`, add `checkpoint_path()`, `load_existing()`, `mark_in_progress()`, `in_progress_subfolder`, update `mark_done()` |
| `gui/workers.py` | Rename `_run_archive_mode` → `_run_subfolder_mode`, `_archive_inner` → `_subfolder_inner`, `_leaf` → `_is_subfolder_run`, add `checkpoint.mark_in_progress()` call, clean log messages |
| `gui/collector_panel.py` | Add import, add checkpoint detection + resume dialog in `_on_collect()` |
| `tests/test_archive_checkpoint.py` | Rename to `test_subfolder_checkpoint.py`, update imports and test names |
| `tests/test_collector_worker.py` | Update test names and mock patch paths |

---

## Test Plan

- All existing `SubfolderCheckpoint` tests pass after rename.
- New tests for `mark_in_progress()` and `in_progress_subfolder`:
  - Fresh checkpoint: `in_progress_subfolder` is `None`.
  - After `mark_in_progress("26-03-2026")`: property returns `"26-03-2026"`.
  - After `mark_done("26-03-2026", ...)`: `in_progress_subfolder` is `None`.
  - `checkpoint_path()` returns same path as `_make_path()` for same inputs.
- Panel dialog logic tested via existing `test_collector_panel.py` (mock `SubfolderCheckpoint.checkpoint_path` to return a path that exists/doesn't exist).
- Full `pytest -q` passes.
