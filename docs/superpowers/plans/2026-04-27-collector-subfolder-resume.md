# Collector Subfolder Resume Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the DataCollector's interrupted-run UX: rename `ArchiveCheckpoint` → `SubfolderCheckpoint` (remove user-facing "Archive" lingo), add `mark_in_progress()` so the panel knows which subfolder was interrupted, and show a proper Resume / Start Fresh / Cancel dialog on next run.

**Architecture:** Four sequential tasks. Task 1 is a pure mechanical rename — no logic changes. Tasks 2–3 add new behaviour to `checkpoint.py` and `workers.py` (TDD). Task 4 wires the resume dialog into `collector_panel.py` (TDD). Each task commits independently.

**Tech Stack:** Python 3.10+, PyQt6, json (stdlib), pytest

---

## File Map

| File | Change |
|---|---|
| `parquet_transform/checkpoint.py` | Rename class + constant + filename suffix; add `checkpoint_path()`, `load_existing()`, `mark_in_progress()`, `in_progress_subfolder`; update `mark_done()` |
| `gui/workers.py` | Rename `_run_archive_mode` → `_run_subfolder_mode`, `_archive_inner` → `_subfolder_inner`, `_leaf` → `_is_subfolder_run`; add `checkpoint.mark_in_progress()` call; clean log messages |
| `gui/collector_panel.py` | Add `SubfolderCheckpoint` import; add checkpoint detection + resume dialog in `_on_collect()` |
| `tests/test_archive_checkpoint.py` | Rename file to `test_subfolder_checkpoint.py`; update imports + test names |
| `tests/test_collector_worker.py` | Update test names + mock patch targets |
| `tests/test_collector_panel.py` | Add resume dialog tests |

---

## Task 1: Rename ArchiveCheckpoint → SubfolderCheckpoint

Pure mechanical rename. No logic changes. All existing tests must pass after this task.

**Files:**
- Modify: `parquet_transform/checkpoint.py`
- Modify: `gui/workers.py`
- Modify: `tests/test_collector_worker.py`
- Rename + modify: `tests/test_archive_checkpoint.py` → `tests/test_subfolder_checkpoint.py`

- [ ] **Step 1: Rename the test file**

```bash
git mv tests/test_archive_checkpoint.py tests/test_subfolder_checkpoint.py
```

- [ ] **Step 2: Update `tests/test_subfolder_checkpoint.py` content**

Replace the entire file content:

```python
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
```

- [ ] **Step 3: Update `parquet_transform/checkpoint.py` — rename section header, constant, class, and filename suffix**

Make these four targeted replacements in `checkpoint.py`:

**3a.** Replace the section comment + constant (around line 382–386):
```python
# Old
# ---------------------------------------------------------------------------
# ArchiveCheckpoint
# ---------------------------------------------------------------------------

_ARCHIVE_CHECKPOINT_DIR: Path = Path(__file__).parent.parent / "CollectorCheckpoint"
"""Default directory for archive checkpoints: repo_root/CollectorCheckpoint/"""
```
```python
# New
# ---------------------------------------------------------------------------
# SubfolderCheckpoint
# ---------------------------------------------------------------------------

_SUBFOLDER_CHECKPOINT_DIR: Path = Path(__file__).parent.parent / "CollectorCheckpoint"
"""Default directory for subfolder checkpoints: repo_root/CollectorCheckpoint/"""
```

**3b.** Replace the class declaration and docstring (around line 389–395):
```python
# Old
class ArchiveCheckpoint:
    """
    Per-run checkpoint for archive collection (subfolder-by-subfolder mode).

    Stored in CollectorCheckpoint/ at the repository root (not in the user's
    home directory — archive runs are machine-local and the folder is gitignored).

    Filename is derived deterministically from (container, source_prefix,
    filter_col, filter_values) so the same logical run always maps to the
    same file without the caller needing to track a path.

    Uses atomic writes (temp + os.replace) to survive crashes mid-write.

    Assumes the repository root is writable at runtime; not suitable for read-only installed distributions.
    """
```
```python
# New
class SubfolderCheckpoint:
    """
    Per-run checkpoint for subfolder-by-subfolder collection mode.

    Stored in CollectorCheckpoint/ at the repository root (not in the user's
    home directory — runs are machine-local and the folder is gitignored).

    Filename is derived deterministically from (container, source_prefix,
    filter_col, filter_values) so the same logical run always maps to the
    same file without the caller needing to track a path.

    Uses atomic writes (temp + os.replace) to survive crashes mid-write.

    Assumes the repository root is writable at runtime; not suitable for read-only installed distributions.
    """
```

**3c.** In `_make_path`: replace the filename suffix and the constant reference:
```python
# Old
        return checkpoint_dir / f"{c}__{p}__{fc}__{digest}__archive.json"
```
```python
# New
        return checkpoint_dir / f"{c}__{p}__{fc}__{digest}__subfolder.json"
```

**3d.** In `load_or_create`: replace all three `ArchiveCheckpoint` references and the error message:
```python
# Old (return type annotation)
    ) -> "ArchiveCheckpoint":
```
```python
# New
    ) -> "SubfolderCheckpoint":
```

```python
# Old (path construction call)
        path = ArchiveCheckpoint._make_path(
```
```python
# New
        path = SubfolderCheckpoint._make_path(
```

```python
# Old (error message)
                    f"Archive checkpoint is corrupt and cannot be loaded: {path}\n"
```
```python
# New
                    f"Subfolder checkpoint is corrupt and cannot be loaded: {path}\n"
```

```python
# Old (final return)
        return ArchiveCheckpoint(path, data)
```
```python
# New
        return SubfolderCheckpoint(path, data)
```

**3e.** In `load_or_create`: replace the constant reference:
```python
# Old
        directory = _checkpoint_dir or _ARCHIVE_CHECKPOINT_DIR
```
```python
# New
        directory = _checkpoint_dir or _SUBFOLDER_CHECKPOINT_DIR
```

- [ ] **Step 4: Update `gui/workers.py` — rename all references**

**4a.** Import line (line 22):
```python
# Old
from parquet_transform.checkpoint import ArchiveCheckpoint, FailedList, RunCheckpoint
```
```python
# New
from parquet_transform.checkpoint import SubfolderCheckpoint, FailedList, RunCheckpoint
```

**4b.** `__init__` parameter comment (line 963–964):
```python
# Old
        start_part: int | None = None,   # when set, always use _part_NNN naming (archive mode)
        _leaf: bool = False,             # internal: prevents subfolder detection recursion
```
```python
# New
        start_part: int | None = None,   # when set, always use _part_NNN naming (subfolder mode)
        _is_subfolder_run: bool = False, # internal: prevents subfolder detection recursion
```

**4c.** `__init__` body assignment (line 981):
```python
# Old
        self._leaf = _leaf
```
```python
# New
        self._is_subfolder_run = _is_subfolder_run
```

**4d.** `__init__` body — inner worker attribute (line 987):
```python
# Old
        self._archive_inner: "DataCollectorWorker | None" = None
```
```python
# New
        self._subfolder_inner: "DataCollectorWorker | None" = None
```

**4e.** `cancel()` method (lines 993–995):
```python
# Old
        # Forward cancellation to a running inner worker (archive mode)
        if self._archive_inner is not None:
            self._archive_inner.cancel()
```
```python
# New
        # Forward cancellation to a running inner worker (subfolder mode)
        if self._subfolder_inner is not None:
            self._subfolder_inner.cancel()
```

**4f.** Method name + docstring (lines 1007–1014):
```python
# Old
    def _run_archive_mode(self, subfolders: list[str]) -> None:
        """Process *subfolders* one-by-one with a local ArchiveCheckpoint.

        Each subfolder is a synchronous mini-run using a fresh DataCollectorWorker
        with _leaf=True (no further subfolder detection).  The checkpoint is updated
        after each successful subfolder so interrupted runs resume from where they
        left off.
        """
```
```python
# New
    def _run_subfolder_mode(self, subfolders: list[str]) -> None:
        """Process *subfolders* one-by-one with a local SubfolderCheckpoint.

        Each subfolder is a synchronous mini-run using a fresh DataCollectorWorker
        with _is_subfolder_run=True (no further subfolder detection). The checkpoint
        is updated after each successful subfolder so interrupted runs resume from
        where they left off.
        """
```

**4g.** `SubfolderCheckpoint.load_or_create` call (line 1015):
```python
# Old
        checkpoint = ArchiveCheckpoint.load_or_create(
```
```python
# New
        checkpoint = SubfolderCheckpoint.load_or_create(
```

**4h.** Five log messages — strip the `[Archive]` prefix:
```python
# Old
            f"[Archive] {n} Subfolder gefunden — "
            f"{skipped} bereits erledigt, {n - skipped} ausstehend. "
            f"Checkpoint: {checkpoint.path.name}"
```
```python
# New
            f"{n} Subfolder gefunden — "
            f"{skipped} bereits erledigt, {n - skipped} ausstehend. "
            f"Checkpoint: {checkpoint.path.name}"
```

```python
# Old
                    f"[Archive] Subfolder {idx + 1}/{n}: {subfolder} — übersprungen"
```
```python
# New
                    f"Subfolder {idx + 1}/{n}: {subfolder} — übersprungen"
```

```python
# Old
                f"[Archive] Subfolder {idx + 1}/{n}: {subfolder} — wird verarbeitet …"
```
```python
# New
                f"Subfolder {idx + 1}/{n}: {subfolder} — wird verarbeitet …"
```

```python
# Old
                    f"[Archive] Subfolder {subfolder} fertig: "
                    f"{rows:,} Zeilen, {parts_produced} Teil(e)"
```
```python
# New
                    f"Subfolder {subfolder} fertig: "
                    f"{rows:,} Zeilen, {parts_produced} Teil(e)"
```

```python
# Old
                    f"[Archive] Subfolder {subfolder} fertig: keine Treffer"
```
```python
# New
                    f"Subfolder {subfolder} fertig: keine Treffer"
```

```python
# Old
            f"[Archive] Alle {n} Subfolder verarbeitet — "
            f"{checkpoint.total_rows:,} Zeilen gesamt"
```
```python
# New
            f"Alle {n} Subfolder verarbeitet — "
            f"{checkpoint.total_rows:,} Zeilen gesamt"
```

**4i.** Inner worker creation — `_leaf` parameter (line 1063):
```python
# Old
                _leaf=True,
```
```python
# New
                _is_subfolder_run=True,
```

**4j.** Inner worker tracking (lines 1095–1097):
```python
# Old
            self._archive_inner = inner
            inner.run()   # synchronous — runs in this QThread's thread
            self._archive_inner = None
```
```python
# New
            self._subfolder_inner = inner
            inner.run()   # synchronous — runs in this QThread's thread
            self._subfolder_inner = None
```

**4k.** `run()` — subfolder detection block comment (lines 1151–1154):
```python
# Old
        # ── Subfolder detection (archive mode) ───────────────────────────────
        # When the source prefix contains virtual subdirectories, process them
        # one-by-one with a local checkpoint so interrupted runs can resume.
        # _leaf=True suppresses this check for inner runs (prevents recursion).
        if not self._leaf:
```
```python
# New
        # ── Subfolder detection ───────────────────────────────────────────────
        # When the source prefix contains virtual subdirectories, process them
        # one-by-one with a local checkpoint so interrupted runs can resume.
        # _is_subfolder_run=True suppresses this check for inner runs (prevents recursion).
        if not self._is_subfolder_run:
```

**4l.** `run()` — call site (line 1166):
```python
# Old
                self._run_archive_mode(_subfolders)
```
```python
# New
                self._run_subfolder_mode(_subfolders)
```

**4m.** `run()` — part_counter comment (around line 1220):
```python
# Old
            # When start_part is provided (archive mode), start the counter one below
```
```python
# New
            # When start_part is provided (subfolder mode), start the counter one below
```

**4n.** `_flush_and_upload` comment (around line 1237):
```python
# Old
                #   (b) called from archive mode (start_part set → global part numbering required)
```
```python
# New
                #   (b) called from subfolder mode (start_part set → global part numbering required)
```

- [ ] **Step 5: Update `tests/test_collector_worker.py` — rename test identifiers and patch targets**

**5a.** Section comment:
```python
# Old
# --- start_part / _leaf ---
```
```python
# New
# --- start_part / _is_subfolder_run ---
```

**5b.** Test name + body:
```python
# Old
def test_leaf_flag_defaults_to_false():
    worker = _make_worker()
    assert worker._leaf is False
```
```python
# New
def test_is_subfolder_run_defaults_to_false():
    worker = _make_worker()
    assert worker._is_subfolder_run is False
```

**5c.** Section comment:
```python
# Old
# --- archive mode ---
```
```python
# New
# --- subfolder mode ---
```

**5d.** Test name (keep body identical, only rename function):
```python
# Old
def test_archive_mode_skips_completed_subfolder():
```
```python
# New
def test_subfolder_mode_skips_completed_subfolder():
```

**5e.** Patch target inside `test_subfolder_mode_skips_completed_subfolder`:
```python
# Old
         patch("parquet_transform.checkpoint.ArchiveCheckpoint.load_or_create") as mock_cp_ctor:
```
```python
# New
         patch("parquet_transform.checkpoint.SubfolderCheckpoint.load_or_create") as mock_cp_ctor:
```

**5f.** Test name (keep body identical):
```python
# Old
def test_archive_mode_emits_finished_with_total_rows():
```
```python
# New
def test_subfolder_mode_emits_finished_with_total_rows():
```

**5g.** Patch target inside `test_subfolder_mode_emits_finished_with_total_rows`:
```python
# Old
         patch("parquet_transform.checkpoint.ArchiveCheckpoint.load_or_create") as mock_cp_ctor:
```
```python
# New
         patch("parquet_transform.checkpoint.SubfolderCheckpoint.load_or_create") as mock_cp_ctor:
```

- [ ] **Step 6: Run full test suite**

```bash
pytest -q
```

Expected: all tests pass. The renamed test file is discovered automatically by pytest. If any test fails, re-check the rename steps above.

- [ ] **Step 7: Commit**

```bash
git add parquet_transform/checkpoint.py gui/workers.py tests/test_subfolder_checkpoint.py tests/test_collector_worker.py
git commit -m "refactor: rename ArchiveCheckpoint → SubfolderCheckpoint, remove Archive log lingo"
```

---

## Task 2: Add new SubfolderCheckpoint methods

Adds `checkpoint_path()`, `load_existing()`, `mark_in_progress()`, `in_progress_subfolder`, and updates `mark_done()` to clear the in-progress field.

**Files:**
- Modify: `parquet_transform/checkpoint.py`
- Modify: `tests/test_subfolder_checkpoint.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_subfolder_checkpoint.py`:

```python
# --- checkpoint_path / load_existing ---

def test_checkpoint_path_matches_load_or_create_path(tmp_path):
    cp = _cp(tmp_path)
    expected = SubfolderCheckpoint.checkpoint_path(
        "mycontainer", "archive/", "SenderUid", ["uid1", "uid2"],
        _checkpoint_dir=tmp_path,
    )
    assert cp.path == expected


def test_load_existing_returns_none_for_missing_file(tmp_path):
    result = SubfolderCheckpoint.load_existing(tmp_path / "nonexistent.json")
    assert result is None


def test_load_existing_loads_saved_checkpoint(tmp_path):
    cp = _cp(tmp_path)
    cp.mark_done("26-02-2026", parts_produced=1, rows=500)

    loaded = SubfolderCheckpoint.load_existing(cp.path)
    assert loaded is not None
    assert loaded.done_count == 1
    assert loaded.total_rows == 500


def test_load_existing_raises_on_corrupt_file(tmp_path):
    cp = _cp(tmp_path)
    cp.mark_done("26-02-2026", parts_produced=1, rows=100)
    cp.path.write_text("not json", encoding="utf-8")

    with pytest.raises(RuntimeError, match="corrupt"):
        SubfolderCheckpoint.load_existing(cp.path)


# --- mark_in_progress / in_progress_subfolder ---

def test_in_progress_subfolder_defaults_to_none(tmp_path):
    cp = _cp(tmp_path)
    assert cp.in_progress_subfolder is None


def test_mark_in_progress_sets_subfolder(tmp_path):
    cp = _cp(tmp_path)
    cp.mark_in_progress("26-03-2026")
    assert cp.in_progress_subfolder == "26-03-2026"


def test_mark_in_progress_persists_across_loads(tmp_path):
    cp = _cp(tmp_path)
    cp.mark_done("26-02-2026", parts_produced=1, rows=500)
    cp.mark_in_progress("26-03-2026")

    cp2 = _cp(tmp_path)
    assert cp2.in_progress_subfolder == "26-03-2026"


def test_mark_done_clears_in_progress_subfolder(tmp_path):
    cp = _cp(tmp_path)
    cp.mark_in_progress("26-03-2026")
    assert cp.in_progress_subfolder == "26-03-2026"

    cp.mark_done("26-03-2026", parts_produced=1, rows=300)
    assert cp.in_progress_subfolder is None


def test_mark_in_progress_overwrites_previous(tmp_path):
    cp = _cp(tmp_path)
    cp.mark_in_progress("26-02-2026")
    cp.mark_in_progress("26-03-2026")
    assert cp.in_progress_subfolder == "26-03-2026"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_subfolder_checkpoint.py -v -k "checkpoint_path or load_existing or in_progress"
```

Expected: FAIL — `AttributeError: type object 'SubfolderCheckpoint' has no attribute 'checkpoint_path'` (or similar).

- [ ] **Step 3: Implement the new methods in `parquet_transform/checkpoint.py`**

**3a.** Add `checkpoint_path()` and `load_existing()` as static methods inside `SubfolderCheckpoint`, directly after `_make_path()` and before `load_or_create()`:

```python
    @staticmethod
    def checkpoint_path(
        container: str,
        source_prefix: str,
        filter_col: str,
        filter_values: list[str],
        _checkpoint_dir: Path | None = None,
    ) -> Path:
        """Return the checkpoint file path for the given run key.

        Does not create the file. Safe to call before load_or_create.
        *_checkpoint_dir* overrides the default storage location (tests only).
        """
        directory = _checkpoint_dir or _SUBFOLDER_CHECKPOINT_DIR
        return SubfolderCheckpoint._make_path(
            container, source_prefix, filter_col, filter_values, directory
        )

    @staticmethod
    def load_existing(path: Path) -> "SubfolderCheckpoint | None":
        """Load a checkpoint from *path* for read-only inspection.

        Returns None if *path* does not exist.
        Raises RuntimeError if the file is corrupt (unparseable JSON).
        """
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"Subfolder checkpoint is corrupt and cannot be loaded: {path}\n"
                    f"Delete the file to start fresh. Detail: {exc}"
                ) from exc
        return SubfolderCheckpoint(path, data)
```

**3b.** Add `in_progress_subfolder` property in the Queries section, after `done_count`:

```python
    @property
    def in_progress_subfolder(self) -> str | None:
        """The subfolder currently being processed, or None if no run is active."""
        return self._data.get("in_progress_subfolder")
```

**3c.** Add `mark_in_progress()` mutation in the Mutations section, before `mark_done()`:

```python
    def mark_in_progress(self, subfolder: str) -> None:
        """Record *subfolder* as the one currently being processed.

        Called just before starting an inner run. Allows the panel to show
        which subfolder was interrupted if the run is cancelled.
        """
        with self._lock:
            self._data["in_progress_subfolder"] = subfolder
            self._data["updated_at"] = _now()
            self._write()
```

**3d.** Update `mark_done()` to clear `in_progress_subfolder`. Inside the `with self._lock:` block in `mark_done()`, add one line before `self._write()`:

```python
            self._data["in_progress_subfolder"] = None
```

The full updated `mark_done()` body should look like:

```python
    def mark_done(self, subfolder: str, parts_produced: int, rows: int) -> None:
        """Record *subfolder* as complete and persist atomically.

        Idempotent: calling twice for the same subfolder is a no-op after
        the first call.
        """
        if parts_produced < 0 or rows < 0:
            raise ValueError(
                f"parts_produced and rows must be >= 0, got {parts_produced!r}, {rows!r}"
            )
        with self._lock:
            done: list[str] = self._data.setdefault("subfolders_done", [])
            if subfolder in done:
                return  # already recorded — don't double-count
            done.append(subfolder)
            self._data["next_part"] = self._data.get("next_part", 1) + parts_produced
            self._data["total_rows"] = self._data.get("total_rows", 0) + rows
            self._data["in_progress_subfolder"] = None
            self._data["updated_at"] = _now()
            self._write()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_subfolder_checkpoint.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Run full suite**

```bash
pytest -q
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add parquet_transform/checkpoint.py tests/test_subfolder_checkpoint.py
git commit -m "feat: add SubfolderCheckpoint.checkpoint_path, load_existing, mark_in_progress"
```

---

## Task 3: Wire mark_in_progress into _run_subfolder_mode

Calls `checkpoint.mark_in_progress(subfolder)` just before each inner run so the checkpoint file records which subfolder was being processed when a cancellation happens.

**Files:**
- Modify: `gui/workers.py`
- Modify: `tests/test_collector_worker.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_collector_worker.py`, in the `# --- subfolder mode ---` section:

```python
def test_subfolder_mode_calls_mark_in_progress_before_inner_run():
    """mark_in_progress must be called for each non-skipped subfolder."""
    from unittest.mock import MagicMock, patch, call

    raw = _make_parquet_bytes("uid1", "dev1", 3)

    with patch("gui.workers.BlobStorageClient") as MockClient, \
         patch("parquet_transform.checkpoint.SubfolderCheckpoint.load_or_create") as mock_cp_ctor:

        mock_cp = MagicMock()
        mock_cp.next_part = 1
        mock_cp.total_rows = 0
        mock_cp.done_count = 0
        mock_cp.is_done.return_value = False
        mock_cp.path = MagicMock()
        mock_cp.path.name = "test_checkpoint.json"
        mock_cp_ctor.return_value = mock_cp

        mock_instance = MockClient.return_value
        mock_instance.list_blob_prefixes.return_value = ["26-02-2026", "26-03-2026"]
        mock_instance.list_blobs.return_value = []  # no blobs → inner run exits fast
        mock_instance.upload_stream = MagicMock()

        worker = _make_worker(source_prefix="data/")
        worker.run()

    assert mock_cp.mark_in_progress.call_count == 2
    mock_cp.mark_in_progress.assert_any_call("26-02-2026")
    mock_cp.mark_in_progress.assert_any_call("26-03-2026")
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_collector_worker.py::test_subfolder_mode_calls_mark_in_progress_before_inner_run -v
```

Expected: FAIL — `AssertionError: Expected call count 2, got 0`.

- [ ] **Step 3: Add `checkpoint.mark_in_progress(subfolder)` call in `gui/workers.py`**

In `_run_subfolder_mode()`, locate the lines immediately before `self._subfolder_inner = inner`:

```python
            self._subfolder_inner = inner
            inner.run()   # synchronous — runs in this QThread's thread
            self._subfolder_inner = None
```

Replace with:

```python
            checkpoint.mark_in_progress(subfolder)
            self._subfolder_inner = inner
            inner.run()   # synchronous — runs in this QThread's thread
            self._subfolder_inner = None
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_collector_worker.py -v -k "subfolder"
```

Expected: all subfolder-related tests PASS.

- [ ] **Step 5: Run full suite**

```bash
pytest -q
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add gui/workers.py tests/test_collector_worker.py
git commit -m "feat: call mark_in_progress before each subfolder inner run"
```

---

## Task 4: Resume dialog in collector_panel.py

Detects an existing `SubfolderCheckpoint` file before starting a run and shows a Resume / Start Fresh / Cancel dialog with the interrupted subfolder name and completed count.

**Files:**
- Modify: `gui/collector_panel.py`
- Modify: `tests/test_collector_panel.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_collector_panel.py`:

```python
import json
from unittest.mock import MagicMock, patch


def _fill_panel(panel: CollectorPanel) -> None:
    """Fill required fields with valid dummy values."""
    panel._conn_edit.setText("fake_conn")
    panel._container_edit.setText("mycontainer")
    panel._source_edit.setText("data/")
    panel._output_edit.setText("output/")
    panel._ids_edit.setPlainText("uid1")
    # SenderUid is the first item in the combo — already selected by default


def _fake_cp_json(done: list[str], in_progress: str | None) -> str:
    return json.dumps({
        "container": "mycontainer",
        "source_prefix": "data/",
        "filter_col": "SenderUid",
        "filter_values": ["uid1"],
        "output_prefix": "output/",
        "output_container": "mycontainer",
        "subfolders_done": done,
        "in_progress_subfolder": in_progress,
        "next_part": len(done) + 1,
        "total_rows": len(done) * 500,
        "created_at": "2026-01-01T00:00:00",
        "updated_at": "2026-01-01T00:00:00",
    })


def test_start_fresh_deletes_subfolder_checkpoint(tmp_path):
    """Clicking Start Fresh in the subfolder dialog removes the checkpoint file."""
    from parquet_transform.checkpoint import SubfolderCheckpoint

    panel = CollectorPanel()
    _fill_panel(panel)

    fake_cp_path = tmp_path / "fake__subfolder.json"
    fake_cp_path.write_text(
        _fake_cp_json(done=["26-01-2026", "26-02-2026"], in_progress="26-03-2026"),
        encoding="utf-8",
    )

    mock_rr = MagicMock()
    mock_rr.is_complete.return_value = False
    mock_rr.status = "none"

    with patch.object(SubfolderCheckpoint, "checkpoint_path", return_value=fake_cp_path), \
         patch("gui.collector_panel.CollectorRunRecord.load_or_create", return_value=mock_rr), \
         patch("gui.workers.DataCollectorWorker.start"), \
         patch("PyQt6.QtWidgets.QMessageBox") as MockMB:

        fresh_btn = MagicMock()

        def add_button(label, role):
            if label == "Start Fresh":
                return fresh_btn
            return MagicMock()

        mb_instance = MagicMock()
        MockMB.return_value = mb_instance
        mb_instance.addButton.side_effect = add_button
        mb_instance.clickedButton.return_value = fresh_btn

        panel._on_collect()

    assert not fake_cp_path.exists()


def test_resume_keeps_subfolder_checkpoint(tmp_path):
    """Clicking Resume in the subfolder dialog leaves the checkpoint file untouched."""
    from parquet_transform.checkpoint import SubfolderCheckpoint

    panel = CollectorPanel()
    _fill_panel(panel)

    fake_cp_path = tmp_path / "fake__subfolder.json"
    fake_cp_path.write_text(
        _fake_cp_json(done=["26-01-2026"], in_progress="26-02-2026"),
        encoding="utf-8",
    )

    mock_rr = MagicMock()
    mock_rr.is_complete.return_value = False
    mock_rr.status = "none"

    with patch.object(SubfolderCheckpoint, "checkpoint_path", return_value=fake_cp_path), \
         patch("gui.collector_panel.CollectorRunRecord.load_or_create", return_value=mock_rr), \
         patch("gui.workers.DataCollectorWorker.start"), \
         patch("PyQt6.QtWidgets.QMessageBox") as MockMB:

        resume_btn = MagicMock()

        def add_button(label, role):
            if label == "Resume":
                return resume_btn
            return MagicMock()

        mb_instance = MagicMock()
        MockMB.return_value = mb_instance
        mb_instance.addButton.side_effect = add_button
        mb_instance.clickedButton.return_value = resume_btn

        panel._on_collect()

    assert fake_cp_path.exists()


def test_cancel_subfolder_dialog_aborts_collection(tmp_path):
    """Clicking Cancel in the subfolder dialog does not start a worker."""
    from parquet_transform.checkpoint import SubfolderCheckpoint

    panel = CollectorPanel()
    _fill_panel(panel)

    fake_cp_path = tmp_path / "fake__subfolder.json"
    fake_cp_path.write_text(
        _fake_cp_json(done=["26-01-2026"], in_progress="26-02-2026"),
        encoding="utf-8",
    )

    with patch.object(SubfolderCheckpoint, "checkpoint_path", return_value=fake_cp_path), \
         patch("gui.collector_panel.CollectorRunRecord.load_or_create") as mock_rr_ctor, \
         patch("PyQt6.QtWidgets.QMessageBox") as MockMB:

        cancel_btn = MagicMock()

        mb_instance = MagicMock()
        MockMB.return_value = mb_instance
        mb_instance.addButton.return_value = MagicMock()
        mb_instance.clickedButton.return_value = cancel_btn  # not resume or fresh

        panel._on_collect()

    # CollectorRunRecord was never loaded (cancelled before reaching that block)
    mock_rr_ctor.assert_not_called()
    assert panel._worker is None


def test_no_subfolder_checkpoint_uses_run_record_flow(tmp_path):
    """When no SubfolderCheckpoint exists, the normal CollectorRunRecord flow runs."""
    from parquet_transform.checkpoint import SubfolderCheckpoint

    panel = CollectorPanel()
    _fill_panel(panel)

    non_existent = tmp_path / "does_not_exist.json"

    mock_rr = MagicMock()
    mock_rr.is_complete.return_value = False
    mock_rr.status = "none"

    with patch.object(SubfolderCheckpoint, "checkpoint_path", return_value=non_existent), \
         patch("gui.collector_panel.CollectorRunRecord.load_or_create", return_value=mock_rr) as mock_rr_ctor, \
         patch("gui.workers.DataCollectorWorker.start"):

        panel._on_collect()

    mock_rr_ctor.assert_called_once()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_collector_panel.py::test_start_fresh_deletes_subfolder_checkpoint tests/test_collector_panel.py::test_no_subfolder_checkpoint_uses_run_record_flow -v
```

Expected: FAIL — `ImportError` or `AttributeError` (SubfolderCheckpoint not imported in panel yet).

- [ ] **Step 3: Add `SubfolderCheckpoint` import to `gui/collector_panel.py`**

Change line 36:

```python
# Old
from parquet_transform.checkpoint import CollectorRunRecord
```
```python
# New
from parquet_transform.checkpoint import CollectorRunRecord, SubfolderCheckpoint
```

- [ ] **Step 4: Add checkpoint detection block to `_on_collect()` in `gui/collector_panel.py`**

Locate this block (starting at line 306):

```python
        filter_values = [
            line.strip()
            for line in self._ids_edit.toPlainText().splitlines()
            if line.strip()
        ]
        if not filter_values:
            self._log_error("Enter at least one ID.")
            return

        # Last Run Detection: check for a previous run with the same key
        try:
            run_record = CollectorRunRecord.load_or_create(
                container,
                self._source_edit.text().strip(),
                self._filter_combo.currentText(),
                filter_values,
            )
        except RuntimeError as exc:
            self._log_error(f"Corrupt run record (delete file to reset): {exc}")
            return

        if run_record.is_complete():
            from PyQt6.QtWidgets import QMessageBox
            msg = QMessageBox(self)
            msg.setWindowTitle("Previous Run Found")
            msg.setText(
                f"A previous collection run completed successfully.\n\n"
                f"Output blob:  {run_record.output_blob}\n"
                f"Row count:    {run_record.row_count}\n\n"
                "Re-run and overwrite the output?"
            )
            rerun_btn = msg.addButton("Re-run", QMessageBox.ButtonRole.AcceptRole)
            msg.addButton("Cancel", QMessageBox.ButtonRole.RejectRole)
            msg.exec()
            if msg.clickedButton() != rerun_btn:
                return
            run_record.reset()

        elif run_record.status == "in_progress":
            from PyQt6.QtWidgets import QMessageBox
            msg = QMessageBox(self)
            msg.setWindowTitle("Previous Run Interrupted")
            msg.setText(
                "A previous collection run was interrupted before completing.\n\n"
                "Start a fresh run?"
            )
            fresh_btn = msg.addButton("Start Fresh", QMessageBox.ButtonRole.AcceptRole)
            msg.addButton("Cancel", QMessageBox.ButtonRole.RejectRole)
            msg.exec()
            if msg.clickedButton() != fresh_btn:
                return
            run_record.reset()
```

Replace with:

```python
        filter_values = [
            line.strip()
            for line in self._ids_edit.toPlainText().splitlines()
            if line.strip()
        ]
        if not filter_values:
            self._log_error("Enter at least one ID.")
            return

        source_prefix = self._source_edit.text().strip()
        filter_col = self._filter_combo.currentText()

        # ── SubfolderCheckpoint detection ─────────────────────────────────────
        _cp_path = SubfolderCheckpoint.checkpoint_path(
            container, source_prefix, filter_col, filter_values,
        )
        _skip_run_record_dialogs = False
        _start_fresh = False
        if _cp_path.exists():
            try:
                _cp = SubfolderCheckpoint.load_existing(_cp_path)
            except RuntimeError as exc:
                self._log_error(f"Corrupt subfolder checkpoint (delete to reset): {exc}")
                return
            if _cp is not None and (_cp.done_count > 0 or _cp.in_progress_subfolder):
                from PyQt6.QtWidgets import QMessageBox
                msg = QMessageBox(self)
                msg.setWindowTitle("Previous Run Interrupted")
                lines = [f"{_cp.done_count} subfolder(s) completed."]
                if _cp.in_progress_subfolder:
                    lines.append(
                        f"Interrupted on: \"{_cp.in_progress_subfolder}\" — "
                        "resuming will rerun it from the beginning."
                    )
                msg.setText("\n".join(lines))
                resume_btn = msg.addButton("Resume", QMessageBox.ButtonRole.AcceptRole)
                fresh_btn = msg.addButton("Start Fresh", QMessageBox.ButtonRole.DestructiveRole)
                msg.addButton("Cancel", QMessageBox.ButtonRole.RejectRole)
                msg.exec()
                clicked = msg.clickedButton()
                if clicked == resume_btn:
                    _skip_run_record_dialogs = True
                elif clicked == fresh_btn:
                    _cp_path.unlink(missing_ok=True)
                    _skip_run_record_dialogs = True
                    _start_fresh = True
                else:
                    return  # Cancel
        # ── end SubfolderCheckpoint detection ─────────────────────────────────

        # Last Run Detection: check for a previous run with the same key
        try:
            run_record = CollectorRunRecord.load_or_create(
                container,
                source_prefix,
                filter_col,
                filter_values,
            )
        except RuntimeError as exc:
            self._log_error(f"Corrupt run record (delete file to reset): {exc}")
            return

        if _start_fresh:
            run_record.reset()
        elif not _skip_run_record_dialogs:
            if run_record.is_complete():
                from PyQt6.QtWidgets import QMessageBox
                msg = QMessageBox(self)
                msg.setWindowTitle("Previous Run Found")
                msg.setText(
                    f"A previous collection run completed successfully.\n\n"
                    f"Output blob:  {run_record.output_blob}\n"
                    f"Row count:    {run_record.row_count}\n\n"
                    "Re-run and overwrite the output?"
                )
                rerun_btn = msg.addButton("Re-run", QMessageBox.ButtonRole.AcceptRole)
                msg.addButton("Cancel", QMessageBox.ButtonRole.RejectRole)
                msg.exec()
                if msg.clickedButton() != rerun_btn:
                    return
                run_record.reset()

            elif run_record.status == "in_progress":
                from PyQt6.QtWidgets import QMessageBox
                msg = QMessageBox(self)
                msg.setWindowTitle("Previous Run Interrupted")
                msg.setText(
                    "A previous collection run was interrupted before completing.\n\n"
                    "Start a fresh run?"
                )
                fresh_btn = msg.addButton("Start Fresh", QMessageBox.ButtonRole.AcceptRole)
                msg.addButton("Cancel", QMessageBox.ButtonRole.RejectRole)
                msg.exec()
                if msg.clickedButton() != fresh_btn:
                    return
                run_record.reset()
```

- [ ] **Step 5: Update the DataCollectorWorker creation in `_on_collect()` to use the extracted variables**

Locate:

```python
        self._worker = DataCollectorWorker(
            connection_string=conn,
            container=container,
            source_prefix=self._source_edit.text().strip(),
            output_prefix=output_prefix,
            filter_col=self._filter_combo.currentText(),
            filter_values=filter_values,
```

Replace with:

```python
        self._worker = DataCollectorWorker(
            connection_string=conn,
            container=container,
            source_prefix=source_prefix,
            output_prefix=output_prefix,
            filter_col=filter_col,
            filter_values=filter_values,
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
pytest tests/test_collector_panel.py -v
```

Expected: all tests PASS.

- [ ] **Step 7: Run full suite**

```bash
pytest -q
```

Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add gui/collector_panel.py tests/test_collector_panel.py
git commit -m "feat: show Resume/Start Fresh dialog when subfolder run was interrupted"
```

---

## Self-Review

**Spec coverage:**
- ✅ Rename `ArchiveCheckpoint` → `SubfolderCheckpoint` (Task 1)
- ✅ Rename `_run_archive_mode` → `_run_subfolder_mode`, `_archive_inner` → `_subfolder_inner`, `_leaf` → `_is_subfolder_run` (Task 1)
- ✅ `__archive.json` suffix → `__subfolder.json` (Task 1)
- ✅ `[Archive]` log prefixes removed (Task 1)
- ✅ `checkpoint_path()` static method (Task 2)
- ✅ `load_existing()` static method (Task 2)
- ✅ `mark_in_progress()` mutation (Task 2)
- ✅ `in_progress_subfolder` property (Task 2)
- ✅ `mark_done()` clears `in_progress_subfolder` (Task 2)
- ✅ `checkpoint.mark_in_progress(subfolder)` called before inner run (Task 3)
- ✅ Detection in `_on_collect()` before `CollectorRunRecord` dialogs (Task 4)
- ✅ Resume dialog with done_count + interrupted subfolder name (Task 4)
- ✅ Resume keeps checkpoint file (Task 4)
- ✅ Start Fresh deletes checkpoint file + resets run_record (Task 4)
- ✅ Cancel aborts without starting worker (Task 4)
- ✅ No checkpoint → existing CollectorRunRecord flow unchanged (Task 4)

**Placeholder scan:** None found. All steps contain complete code.

**Type consistency:**
- `SubfolderCheckpoint.checkpoint_path(...)` → returns `Path` → passed to `_cp_path.exists()` and `_cp_path.unlink()` ✅
- `SubfolderCheckpoint.load_existing(path)` → returns `SubfolderCheckpoint | None` → guarded with `if _cp is not None` ✅
- `_cp.done_count` (int) and `_cp.in_progress_subfolder` (str | None) match Task 2 property definitions ✅
- `source_prefix` and `filter_col` extracted as local variables in Task 4, used in both `CollectorRunRecord.load_or_create` and `DataCollectorWorker()` constructor ✅
