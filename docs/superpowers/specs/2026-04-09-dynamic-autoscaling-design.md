# Dynamic Autoscaling Design

**Date:** 2026-04-09  
**Goal:** Maximize files/hour by continuously adapting worker count to actual measured bandwidth, replacing the one-shot startup calibration with a continuous scaling loop.

---

## Problem with Current Approach

- Calibration runs 4 files with 2 workers at startup, computes worker count once, never revisits
- No adaptation during long runs (bandwidth can fluctuate)
- Only scale-down path is error-throttle (halve on ≥10% connection errors per pass) — no scale-up path after initial calibration
- 4-file sample is statistically weak

---

## Architecture

### New file: `parquet_transform/scaler.py`

A standalone `AdaptiveScaler` class with a clean interface. All scaling logic lives here — `TransformWorker` stays focused on I/O.

```python
class AdaptiveScaler:
    def record_upload(self, bytes: int, seconds: float, success: bool) -> None
    def should_scale(self, current_workers: int, p95_bytes: int) -> int  # returns new worker count
```

**Internal state:**
- Rolling window of last `SCALER_WINDOW_SIZE=100` upload measurements (bytes, seconds, success/fail)
- `_first_scale_done: bool` — distinguishes first check (proportional) from subsequent checks (incremental)
- `_consecutive_headroom_checks: int` — tracks stability for scale-up confirmation
- Thread-safe via internal `Lock`

**Minimum samples:** No decisions before `SCALER_MIN_SAMPLES=50` measurements are collected.

---

## Scaling Logic

### Scale-DOWN (checked first, takes priority)

Trigger when **both** conditions hold over the rolling window:
- Error rate > `SCALER_DOWN_ERROR_RATE=0.15` (15%)
- Measured throughput < `(1 - SCALER_DOWN_THROUGHPUT_DROP) × median_throughput` (i.e. a 30%+ drop vs. rolling median)

Action: `new_count = max(1, current_workers - 1)`

The combined condition avoids false positives from brief network spikes (errors alone) or natural file-size variance (throughput alone).

### Scale-UP (only when no down-signal)

**Headroom metric** — derived from rolling window measurements:

```
per_conn_bw  = total_upload_bytes / total_upload_seconds  # bytes/s per slot
total_bw     = per_conn_bw × current_workers              # estimated link bandwidth
headroom     = floor(total_bw × UPLOAD_TIMEOUT_S / p95_bytes × 0.7) - current_workers
```

`headroom` is the number of additional workers the measured bandwidth could sustain. This is the "unused potential" metric — not a guess, derived from actual throughput.

**First check** (`_first_scale_done=False`):
- Proportional: `new_count = min(configured_max_workers, current_workers + headroom)`
  where `configured_max_workers` = the worker count the user set in the UI (passed into `TransformWorker` as `worker_count`)
- Sets `_first_scale_done = True`

**Subsequent checks:**
- Require `headroom ≥ SCALER_UP_MIN_HEADROOM=2` for `SCALER_UP_CONFIRM_CHECKS=2` consecutive checks
- Action: `new_count = current_workers + min(headroom, SCALER_UP_MAX_STEP=4)`
- Resets `_consecutive_headroom_checks` to 0 after each scale event

**Self-stabilizing:** Adding workers reduces per-connection throughput, which reduces headroom, which prevents runaway scaling.

---

## Check Interval

`TransformWorker` tracks `_completed_since_last_check`. After every `SCALER_CHECK_INTERVAL=150` successfully completed files, it calls `scaler.should_scale()`. If the returned count differs from the current count, it updates `self._worker_count` — effective on the next task queue fill (running threads are not interrupted).

---

## Changes to `TransformWorker`

**Removed:**
- `needs_calibration` logic and all associated variables
- `upload_measurements` / `upload_meas_lock`
- Constants: `_CALIB_BATCH_SIZE`, `_CALIB_LARGE_FILE_THRESHOLD_MB`, `UPLOAD_BANDWIDTH_FALLBACK_KBYTES_S`
- Signal: `workers_calibrated`

**Added:**
- `AdaptiveScaler` instantiated in `run()` when `autoscale=True`
- `record_upload()` called from `worker_loop()` after each upload (via scaler's internal lock)
- `_completed_since_last_check` counter, reset after each `should_scale()` call
- Signal: `workers_scaled(new_count: int, old_count: int, direction: str, reason: str)`

**Unchanged:**
- `workers_reduced` signal and error-throttle logic (hard emergency brake, separate from scaler)
- All retry / checkpoint / failed-list logic

---

## Constants

Defined in `workers.py`:

| Constant | Value | Purpose |
|---|---|---|
| `SCALER_WINDOW_SIZE` | 100 | Rolling window size (uploads) |
| `SCALER_MIN_SAMPLES` | 50 | Minimum measurements before first decision |
| `SCALER_CHECK_INTERVAL` | 150 | Completed files between checks |
| `SCALER_DOWN_ERROR_RATE` | 0.15 | Scale-down: error rate threshold |
| `SCALER_DOWN_THROUGHPUT_DROP` | 0.30 | Scale-down: throughput drop vs. median |
| `SCALER_UP_MIN_HEADROOM` | 2 | Scale-up: minimum headroom slots required |
| `SCALER_UP_CONFIRM_CHECKS` | 2 | Scale-up: consecutive stable checks required |
| `SCALER_UP_MAX_STEP` | 4 | Scale-up: max workers added per check (non-first) |

---

## UI / Logging

`main_window.py` connects `workers_scaled` signal to a log entry:

```
[Autoscale ↑] 4→7 workers — 2.4 MB/s gemessen, 3 Slots Headroom
[Autoscale ↓] 6→5 workers — Fehlerrate 18%, Throughput −35%
```

No new UI widgets. No user-configurable scaler parameters.

---

## Files Changed

| File | Change |
|---|---|
| `parquet_transform/scaler.py` | New — `AdaptiveScaler` class |
| `parquet_transform/workers.py` | Remove calibration, add scaler integration + new constants |
| `gui/main_window.py` | Replace `workers_calibrated` handler with `workers_scaled` |
| `tests/test_scaler.py` | New — unit tests for `AdaptiveScaler` |
