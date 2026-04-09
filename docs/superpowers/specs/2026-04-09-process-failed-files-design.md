# Process Failed Files Feature Design

**Date:** 2026-04-09  
**Status:** Approved

---

## Overview

Add a "Process Failed Files" button to the action row that independently processes all entries in the `FailedList` for the current container+prefix. Successful files are removed from the list by the existing worker integration. No new modules required.

---

## Changes

**Only file modified:** `gui/main_window.py`

| Component | Change |
|---|---|
| `_build_action_row()` | Add `self._failed_btn = QPushButton("Process Failed Files")` |
| `_set_ready_state()` | Disable `_failed_btn` |
| `_set_schema_loaded_state()` | Enable `_failed_btn` |
| `_set_processing_state()` | Disable `_failed_btn` |
| `_on_process_failed()` | New handler — load FailedList, show dialog, start worker |

---

## Button Placement

In `_build_action_row()`, placed between the `_dryrun_btn` and `addStretch()`:

```
[Workers] [AutoScale] [Attempts] [Dry Run] [Process Failed Files]  →stretch→  [Apply to All Files] [Pause] [Cancel]
```

---

## Handler: _on_process_failed()

```
1. Read container + prefix from UI fields
2. Load FailedList.load_or_create(container, prefix)
3. If entries empty:
       _log_info("[Failed List] No failed files recorded for this prefix.")
       return
4. Show QMessageBox with list of failed files (name + type per entry, max 10 shown)
   Buttons: "Process" (AcceptRole) | "Cancel" (RejectRole)
5. If user cancels: return
6. _log_info(f"[Failed List] Processing {N} failed file(s)...")
7. _start_transform(
       blob_names=failed_list.blob_names(),
       failed_list=failed_list,
       retry_failed=False,   # blob_names already set explicitly
   )
```

No checkpoint object passed — these are explicit blob names, not a prefix scan.

---

## State Management

| State | `_failed_btn` |
|---|---|
| Ready (no schema) | Disabled |
| Schema loaded | **Enabled** |
| Processing | Disabled |

---

## Log Messages (English)

- `[Failed List] No failed files recorded for this prefix.` — when list is empty
- `[Failed List] Processing N failed file(s)...` — before starting worker
- Per-file success/failure messages come from existing worker integration:
  - `[Failed List] Cleared previously failed file: <name>` (on success)
  - `[Failed List] Recorded network/corrupt failure: <name>` (on renewed failure)

---

## What Is NOT Changing

- `parquet_transform/checkpoint.py` — unchanged
- `gui/workers.py` — unchanged (existing `failed_list.remove()` on success already handles cleanup)
- No new signals, no new modules
