# Process Failed Files Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a "Process Failed Files" button that independently retries all entries in the FailedList for the current container+prefix, removing them on success.

**Architecture:** Single-file change in `gui/main_window.py`. The new button calls a new `_on_process_failed()` handler that loads the `FailedList`, shows a confirmation dialog, then calls the existing `_start_transform()` with the failed blob names and the `FailedList` object. The existing worker integration already handles removing entries on success and updating them on renewed failure.

**Tech Stack:** PyQt6, `parquet_transform.checkpoint.FailedList` (already imported).

---

## File Map

| File | Action |
|---|---|
| `gui/main_window.py` | Modify — add button, update state methods, add handler |

---

## Task 1: Add button, wire state methods, add handler

**Files:**
- Modify: `gui/main_window.py`

This is a GUI task — no unit tests exist for Qt widget interaction. The existing 74 tests must remain green after the change.

- [ ] **Step 1: Add the button in `_build_action_row()`**

Find the line `layout.addWidget(self._dryrun_btn)` (around line 269). Insert the button creation before the widget additions and add it to the layout between `_dryrun_btn` and `addStretch()`:

```python
        self._failed_btn = QPushButton("Process Failed Files")
        self._failed_btn.setFixedWidth(160)
        self._failed_btn.setToolTip(
            "Retry all files that failed in previous runs for this prefix.\n"
            "Successfully processed files are removed from the failed list."
        )
        self._failed_btn.setEnabled(False)
        self._failed_btn.clicked.connect(self._on_process_failed)
```

Then in the layout section, add it after `_dryrun_btn`:
```python
        layout.addWidget(self._dryrun_btn)
        layout.addWidget(self._failed_btn)   # ← add this line
        layout.addStretch()
```

- [ ] **Step 2: Disable button in `_set_ready_state()`**

Find `_set_ready_state()`. Add after the existing `setEnabled` calls:
```python
        self._failed_btn.setEnabled(False)
```

- [ ] **Step 3: Enable button in `_set_schema_loaded_state()`**

Find `_set_schema_loaded_state()`. Add after the existing `setEnabled` calls:
```python
        self._failed_btn.setEnabled(True)
```

- [ ] **Step 4: Disable button in `_set_processing_state()`**

Find `_set_processing_state()`. Add after the existing `setEnabled` calls:
```python
        self._failed_btn.setEnabled(False)
```

- [ ] **Step 5: Disable button on load error**

Find `_on_load_error()`. After the existing disable calls (`_dryrun_btn`, `_apply_btn`), add:
```python
        self._failed_btn.setEnabled(False)
```

- [ ] **Step 6: Add `_on_process_failed()` handler**

Add this method to `MainWindow`, just before `_on_apply()`:

```python
    def _on_process_failed(self) -> None:
        """Retry all files in the FailedList for the current container+prefix."""
        container = self._container_edit.text().strip()
        prefix = self._prefix_edit.text().strip()

        col_configs = self._schema_table.get_column_configs()
        if not col_configs:
            self._log_info("[Failed List] No transforms selected — nothing to do.")
            return

        try:
            fl = FailedList.load_or_create(container, prefix)
        except RuntimeError as exc:
            QMessageBox.critical(self, "Corrupt Failed List", str(exc))
            return

        if not fl.entries:
            self._log_info("[Failed List] No failed files recorded for this prefix.")
            return

        entries = fl.entries
        lines = [f"  \u2022 {e['name']}  ({e['type']})" for e in entries[:10]]
        if len(entries) > 10:
            lines.append(f"  ... and {len(entries) - 10} more")

        msg = QMessageBox(self)
        msg.setWindowTitle("Process Failed Files")
        msg.setText(
            f"{len(entries)} file(s) failed in previous run(s):\n\n"
            + "\n".join(lines)
            + "\n\nProcess them now using the current transforms?"
        )
        process_btn = msg.addButton("Process", QMessageBox.ButtonRole.AcceptRole)
        msg.addButton("Cancel", QMessageBox.ButtonRole.RejectRole)
        msg.exec()

        if msg.clickedButton() != process_btn:
            return

        self._log_info(f"[Failed List] Processing {len(entries)} failed file(s)...")

        # Reset run-level tracking (same as _on_apply)
        self._dry_run = False
        self._retry_depth = 0
        self._corrupted_blobs = []
        self._pending_prefixes = []
        self._run_start_time = perf_counter()
        self._bytes_processed = 0
        self._files_completed_in_run = 0
        self._total_files_in_run = len(entries)

        self._start_transform(
            blob_names=fl.blob_names(),
            failed_list=fl,
        )
```

- [ ] **Step 7: Run all tests**

```bash
cd D:/Coding/parquet_schema_modificator
pytest tests/ -v
```

Expected: 74 passed.

- [ ] **Step 8: Commit**

```bash
git add gui/main_window.py
git commit -m "feat: add Process Failed Files button to retry failed runs"
```

---

## Self-Review

**Spec coverage:**

| Requirement | Step |
|---|---|
| Button in action row between Dry Run and stretch | Step 1 |
| Disabled in ready state | Step 2 |
| Enabled in schema-loaded state | Step 3 |
| Disabled in processing state | Step 4 |
| Disabled on load error | Step 5 |
| Load FailedList, empty → log message | Step 6 |
| Show dialog with file list (max 10) | Step 6 |
| Call _start_transform with blob_names + failed_list | Step 6 |
| No checkpoint passed | Step 6 (checkpoint omitted, defaults to None) |
| Log "[Failed List] Processing N failed file(s)..." | Step 6 |
| Existing worker removes entries on success | Pre-existing (no change needed) |

All spec requirements covered. No placeholders.
