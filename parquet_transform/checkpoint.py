"""
Local checkpoint and failed-file tracking for transform runs.

Two independent, thread-safe classes:
- RunCheckpoint  — cursor-based resume (one file per container+prefix)
- FailedList     — persistent failed-file log (separate file, same key)

Both write atomically via temp-file + os.replace to survive crashes.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import threading
from datetime import datetime
from pathlib import Path

_CHECKPOINT_DIR: Path = Path(__file__).parent.parent / "CollectorCheckpoint"
"""Single directory for all checkpoint files: repo_root/CollectorCheckpoint/ (gitignored)."""


def _sanitize_key(container: str, prefix: str) -> str:
    """Build a filesystem-safe key with a hash suffix to prevent collisions."""
    raw = f"{container}/{prefix}"
    digest = hashlib.sha1(raw.encode()).hexdigest()[:8]
    c = re.sub(r"[^a-zA-Z0-9]", "_", container)
    p = re.sub(r"[^a-zA-Z0-9]", "_", prefix)
    return f"{c}__{p}__{digest}"


def _atomic_write(path: Path, data: dict) -> None:
    """Write *data* as JSON to *path* atomically via a sibling .tmp file."""
    path.parent.mkdir(parents=True, exist_ok=True)
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
        return _CHECKPOINT_DIR / f"{_sanitize_key(container, prefix)}__checkpoint.json"

    @staticmethod
    def load_or_create(
        container: str,
        prefix: str,
        output_prefix: str | None,
    ) -> "RunCheckpoint":
        """Load an existing checkpoint or create a fresh in_progress one.

        Note: when creating a new checkpoint (no file on disk), the file is not
        written until the first call to advance_cursor() or mark_complete(). Use
        checkpoint_path(...).exists() only after at least one blob has been processed.
        """
        path = RunCheckpoint.checkpoint_path(container, prefix)
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f"Checkpoint file is corrupt and cannot be loaded: {path}\n"
                        f"Delete the file to start fresh. Detail: {exc}"
                    ) from exc
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

    def is_complete(self) -> bool:
        """True when the run finished without cancellation."""
        return self._data.get("status") == "complete"

    def should_skip(self, blob_name: str) -> bool:
        """True if *blob_name* is at or before the cursor in the sorted listing."""
        cursor = self._data.get("cursor")
        if cursor is None:
            return False
        return blob_name <= cursor

    @property
    def cursor(self) -> str | None:
        return self._data.get("cursor")

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
        return _CHECKPOINT_DIR / f"{_sanitize_key(container, prefix)}__failed.json"

    @staticmethod
    def load_or_create(container: str, prefix: str) -> "FailedList":
        """Load an existing failed list or create an empty one.

        Note: when creating a new failed list (no file on disk), the file is not
        written until the first call to add_or_update(). Use
        failed_list_path(...).exists() only after at least one failure has been recorded.
        """
        path = FailedList.failed_list_path(container, prefix)
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f"Checkpoint file is corrupt and cannot be loaded: {path}\n"
                        f"Delete the file to start fresh. Detail: {exc}"
                    ) from exc
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

    @property
    def entries(self) -> list[dict]:
        """Snapshot of all failed entries (deep copy — safe to read while writers run)."""
        return copy.deepcopy(self._data["entries"])

    @property
    def corrupt_count(self) -> int:
        return sum(1 for e in self._data["entries"] if e.get("type") == "corrupt")

    @property
    def network_count(self) -> int:
        return sum(1 for e in self._data["entries"] if e.get("type") == "network")

    def blob_names(self) -> list[str]:
        """All failed blob names — use to re-add to a blob list for retry."""
        return [e["name"] for e in self._data["entries"]]

    def add_or_update(self, blob_name: str, failure_type: str, reason: str) -> None:
        """Record a failure. Updates an existing entry if the blob already failed."""
        with self._lock:
            entries = self._data["entries"]
            for entry in entries:
                if entry["name"] == blob_name:
                    entry["type"] = failure_type
                    entry["reason"] = reason
                    entry["failed_at"] = _now()
                    break
            else:
                entries.append({
                    "name": blob_name,
                    "type": failure_type,
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


# ---------------------------------------------------------------------------
# CollectorRunRecord
# ---------------------------------------------------------------------------

class CollectorRunRecord:
    """
    Persists metadata about the last DataCollector run for a given
    (container, source_prefix, filter_col, filter_values) combination.

    Status values:
    - "none"        — no run has been started yet (or was reset)
    - "in_progress" — a run started but did not complete (cancelled / crashed)
    - "complete"    — a run finished and produced output_blob with row_count rows

    File is stored under _CHECKPOINT_DIR using a hash of the key so that
    different filters or prefixes never collide.
    """

    def __init__(self, path: Path, data: dict) -> None:
        self._path = path
        self._data = data
        self._lock = threading.Lock()

    @staticmethod
    def record_path(
        container: str,
        prefix: str,
        filter_col: str,
        filter_values: list[str],
    ) -> Path:
        """Return the .json path for this (container, prefix, filter_col, filter_values) key."""
        sorted_values = sorted(filter_values)
        raw = f"{container}/{prefix}/{filter_col}/{','.join(sorted_values)}"
        digest = hashlib.sha1(raw.encode()).hexdigest()[:8]
        c = re.sub(r"[^a-zA-Z0-9]", "_", container)
        p = re.sub(r"[^a-zA-Z0-9]", "_", prefix)
        return _CHECKPOINT_DIR / f"{c}__{p}__{digest}__collector_run.json"

    @staticmethod
    def load_or_create(
        container: str,
        prefix: str,
        filter_col: str,
        filter_values: list[str],
    ) -> "CollectorRunRecord":
        """Load an existing record or create a fresh one with status 'none'.

        Raises RuntimeError if the file exists but cannot be parsed.
        """
        path = CollectorRunRecord.record_path(container, prefix, filter_col, filter_values)
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f"Collector run record is corrupt and cannot be loaded: {path}\n"
                        f"Delete the file to start fresh. Detail: {exc}"
                    ) from exc
        else:
            now = _now()
            data = {
                "container": container,
                "prefix": prefix,
                "filter_col": filter_col,
                "filter_values": sorted(filter_values),
                "status": "none",
                "output_blob": None,
                "row_count": 0,
                "created_at": now,
                "updated_at": now,
            }
        return CollectorRunRecord(path, data)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def is_complete(self) -> bool:
        """True when the last run finished successfully."""
        return self._data.get("status") == "complete"

    @property
    def status(self) -> str:
        return self._data.get("status", "none")

    @property
    def output_blob(self) -> str | None:
        return self._data.get("output_blob")

    @property
    def row_count(self) -> int:
        return self._data.get("row_count", 0)

    # ------------------------------------------------------------------
    # Mutations (all atomic)
    # ------------------------------------------------------------------

    def mark_in_progress(self) -> None:
        """Record that a run has started — clears any previous result."""
        with self._lock:
            self._data["status"] = "in_progress"
            self._data["output_blob"] = None
            self._data["row_count"] = 0
            self._data["updated_at"] = _now()
            _atomic_write(self._path, self._data)

    def mark_complete(self, output_blob: str, row_count: int) -> None:
        """Record a successful run result."""
        with self._lock:
            self._data["status"] = "complete"
            self._data["output_blob"] = output_blob
            self._data["row_count"] = row_count
            self._data["updated_at"] = _now()
            _atomic_write(self._path, self._data)

    def reset(self) -> None:
        """Clear all run data — use before a fresh run when previous was complete."""
        with self._lock:
            self._data["status"] = "none"
            self._data["output_blob"] = None
            self._data["row_count"] = 0
            self._data["updated_at"] = _now()
            _atomic_write(self._path, self._data)


# ---------------------------------------------------------------------------
# SubfolderCheckpoint
# ---------------------------------------------------------------------------

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

    def __init__(self, path: Path, data: dict) -> None:
        self._path = path
        self._data = data
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @staticmethod
    def _make_path(
        container: str,
        source_prefix: str,
        filter_col: str,
        filter_values: list[str],
        checkpoint_dir: Path,
    ) -> Path:
        sorted_vals = sorted(filter_values)
        raw = f"{container}/{source_prefix}/{filter_col}/{','.join(sorted_vals)}"
        digest = hashlib.sha1(raw.encode()).hexdigest()[:8]
        c  = re.sub(r"[^a-zA-Z0-9]", "_", container)
        p  = re.sub(r"[^a-zA-Z0-9]", "_", source_prefix)
        fc = re.sub(r"[^a-zA-Z0-9]", "_", filter_col)
        return checkpoint_dir / f"{c}__{p}__{fc}__{digest}__subfolder.json"

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
        directory = _checkpoint_dir or _CHECKPOINT_DIR
        return SubfolderCheckpoint._make_path(
            container, source_prefix, filter_col, filter_values, directory
        )

    @staticmethod
    def load_existing(path: Path) -> "SubfolderCheckpoint | None":
        """Load a checkpoint from *path* without creating it if absent.

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

    @staticmethod
    def load_or_create(
        container: str,
        source_prefix: str,
        filter_col: str,
        filter_values: list[str],
        output_prefix: str,
        output_container: str,
        _checkpoint_dir: Path | None = None,
    ) -> "SubfolderCheckpoint":
        """Load an existing checkpoint or create a fresh one.

        *_checkpoint_dir* overrides the default storage location and is
        intended for tests only.

        Raises RuntimeError if the checkpoint file exists but is corrupt.
        """
        directory = _checkpoint_dir or _CHECKPOINT_DIR
        path = SubfolderCheckpoint._make_path(
            container, source_prefix, filter_col, filter_values, directory
        )
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f"Subfolder checkpoint is corrupt and cannot be loaded: {path}\n"
                        f"Delete the file to start fresh. Detail: {exc}"
                    ) from exc
        else:
            data = {
                "container": container,
                "source_prefix": source_prefix,
                "filter_col": filter_col,
                "filter_values": sorted(filter_values),
                "output_prefix": output_prefix,
                "output_container": output_container,
                "subfolders_done": [],
                "next_part": 1,
                "total_rows": 0,
                "created_at": _now(),
                "updated_at": _now(),
            }
        return SubfolderCheckpoint(path, data)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def is_done(self, subfolder: str) -> bool:
        """True if *subfolder* was previously marked complete."""
        return subfolder in self._data.get("subfolders_done", [])

    @property
    def next_part(self) -> int:
        """Part number to assign to the first output file of the next subfolder."""
        return self._data.get("next_part", 1)

    @property
    def total_rows(self) -> int:
        """Cumulative row count across all completed subfolders."""
        return self._data.get("total_rows", 0)

    @property
    def done_count(self) -> int:
        """Number of subfolders marked complete."""
        return len(self._data.get("subfolders_done", []))

    @property
    def in_progress_subfolder(self) -> str | None:
        """The subfolder currently being processed, or None if no run is active."""
        return self._data.get("in_progress_subfolder")

    @property
    def path(self) -> Path:
        return self._path

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def mark_in_progress(self, subfolder: str) -> None:
        """Record *subfolder* as the one currently being processed.

        Called just before starting an inner run. Allows the panel to show
        which subfolder was interrupted if the run is cancelled.
        """
        if not subfolder:
            raise ValueError("subfolder must be a non-empty string")
        with self._lock:
            self._data["in_progress_subfolder"] = subfolder
            self._data["updated_at"] = _now()
            self._write()

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

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _write(self) -> None:
        """Atomic write: temp file + os.replace."""
        # Cannot reuse module-level _atomic_write(); that function targets _CHECKPOINT_DIR.
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)
        os.replace(tmp, self._path)
