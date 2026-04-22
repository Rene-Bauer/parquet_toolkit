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

# Store checkpoints in the user's home directory so the app works without
# elevated permissions when installed in a system-protected location (e.g. C:\).
_CHECKPOINTS_DIR: Path = Path.home() / ".parquet_toolkit" / "checkpoints"


def _sanitize_key(container: str, prefix: str) -> str:
    """Build a filesystem-safe key with a hash suffix to prevent collisions."""
    raw = f"{container}/{prefix}"
    digest = hashlib.sha1(raw.encode()).hexdigest()[:8]
    c = re.sub(r"[^a-zA-Z0-9]", "_", container)
    p = re.sub(r"[^a-zA-Z0-9]", "_", prefix)
    return f"{c}__{p}__{digest}"


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
        return _CHECKPOINTS_DIR / f"{_sanitize_key(container, prefix)}__failed.json"

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
