# Dynamic Autoscaling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the one-shot startup calibration with a continuous `AdaptiveScaler` that maximises files/hour by dynamically adjusting worker count based on rolling upload throughput measurements.

**Architecture:** A new `AdaptiveScaler` class lives in `parquet_transform/scaler.py` and owns all scaling logic. `TransformWorker` feeds it measurements after each upload and queries it every 150 completed files. Scale-down triggers on combined error-rate + throughput-drop; scale-up triggers on measured bandwidth headroom.

**Tech Stack:** Python 3.11+, PyQt6 signals, `statistics` stdlib, `collections.deque`, `threading.Lock`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `parquet_transform/scaler.py` | **Create** | `AdaptiveScaler` — all scaling decisions |
| `tests/test_scaler.py` | **Create** | Unit tests for `AdaptiveScaler` |
| `parquet_transform/workers.py` | **Modify** | Remove calibration, add scaler integration + new constants |
| `gui/main_window.py` | **Modify** | Replace `workers_calibrated` signal handler with `workers_scaled` |

---

## Task 1: `AdaptiveScaler` — structure and `record_upload`

**Files:**
- Create: `parquet_transform/scaler.py`
- Create: `tests/test_scaler.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_scaler.py
import pytest
from parquet_transform.scaler import AdaptiveScaler


def _make_scaler(**overrides) -> AdaptiveScaler:
    defaults = dict(
        window_size=100,
        min_samples=50,
        upload_timeout_s=300,
        down_error_rate=0.15,
        down_throughput_drop=0.30,
        up_min_headroom=2,
        up_confirm_checks=2,
        up_max_step=4,
        configured_max_workers=16,
    )
    defaults.update(overrides)
    return AdaptiveScaler(**defaults)


def test_record_upload_below_min_samples_returns_current():
    scaler = _make_scaler(min_samples=50)
    for _ in range(49):
        scaler.record_upload(1_000_000, 1.0, True)
    assert scaler.should_scale(4, 10_000_000) == (4, "")


def test_record_upload_zero_samples_returns_current():
    scaler = _make_scaler()
    assert scaler.should_scale(4, 10_000_000) == (4, "")


def test_record_upload_zero_p95_returns_current():
    scaler = _make_scaler(min_samples=1)
    scaler.record_upload(1_000_000, 1.0, True)
    assert scaler.should_scale(4, 0) == (4, "")
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd D:/Coding/parquet_schema_modificator && python -m pytest tests/test_scaler.py -v
```

Expected: `ModuleNotFoundError` or `ImportError` — `scaler` does not exist yet.

- [ ] **Step 3: Create `parquet_transform/scaler.py` with minimal structure**

```python
"""
Adaptive worker-count scaler based on rolling upload throughput.

Scale-DOWN: error_rate > threshold AND recent throughput dropped > threshold
Scale-UP (first check): proportional to measured bandwidth headroom
Scale-UP (subsequent):  +1..max_step when headroom stable for N checks
"""
from __future__ import annotations

import statistics
import threading
from collections import deque
from typing import NamedTuple


class _UploadRecord(NamedTuple):
    bytes_: int
    seconds: float
    success: bool


class AdaptiveScaler:
    def __init__(
        self,
        *,
        window_size: int,
        min_samples: int,
        upload_timeout_s: int,
        down_error_rate: float,
        down_throughput_drop: float,
        up_min_headroom: int,
        up_confirm_checks: int,
        up_max_step: int,
        configured_max_workers: int,
    ) -> None:
        self._window: deque[_UploadRecord] = deque(maxlen=window_size)
        self._lock = threading.Lock()
        self._min_samples = min_samples
        self._upload_timeout_s = upload_timeout_s
        self._down_error_rate = down_error_rate
        self._down_throughput_drop = down_throughput_drop
        self._up_min_headroom = up_min_headroom
        self._up_confirm_checks = up_confirm_checks
        self._up_max_step = up_max_step
        self._configured_max_workers = configured_max_workers
        self._first_scale_done = False
        self._consecutive_headroom_checks = 0

    def record_upload(self, bytes_: int, seconds: float, success: bool) -> None:
        """Thread-safe: record one upload measurement."""
        with self._lock:
            self._window.append(_UploadRecord(bytes_, seconds, success))

    def should_scale(self, current_workers: int, p95_bytes: int) -> tuple[int, str]:
        """
        Return (new_worker_count, reason_string).
        Returns (current_workers, "") when no change is warranted.
        """
        with self._lock:
            window = list(self._window)

        if len(window) < self._min_samples or p95_bytes <= 0:
            return current_workers, ""

        successes = [(r.bytes_, r.seconds) for r in window if r.success]
        total_bytes = sum(b for b, _ in successes)
        total_seconds = sum(s for _, s in successes)

        if total_seconds <= 0 or not successes:
            return current_workers, ""

        return current_workers, ""  # scale logic added in Tasks 2 & 3
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_scaler.py -v
```

Expected: all 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add parquet_transform/scaler.py tests/test_scaler.py
git commit -m "feat: add AdaptiveScaler scaffold with record_upload"
```

---

## Task 2: `AdaptiveScaler` — scale-down logic

**Files:**
- Modify: `parquet_transform/scaler.py`
- Modify: `tests/test_scaler.py`

- [ ] **Step 1: Write failing scale-down tests**

Add these to `tests/test_scaler.py`:

```python
def _fill_scaler_with_good_uploads(scaler: AdaptiveScaler, count: int, bytes_: int = 5_000_000, seconds: float = 5.0) -> None:
    for _ in range(count):
        scaler.record_upload(bytes_, seconds, True)


def test_scale_down_triggers_on_combined_error_and_throughput_drop():
    scaler = _make_scaler(
        min_samples=10,
        window_size=100,
        down_error_rate=0.15,
        down_throughput_drop=0.30,
    )
    # Fill with good uploads first (sets the median baseline)
    _fill_scaler_with_good_uploads(scaler, 40, bytes_=5_000_000, seconds=5.0)
    # Add failing records (20% error rate)
    for _ in range(10):
        scaler.record_upload(0, 0.0, False)
    # Add slow uploads (throughput < 70% of baseline)
    for _ in range(40):
        scaler.record_upload(1_000_000, 5.0, True)  # 200 KB/s vs baseline 1 MB/s
    new_count, reason = scaler.should_scale(4, 10_000_000)
    assert new_count == 3
    assert "error" in reason.lower() or "throughput" in reason.lower()


def test_scale_down_does_not_trigger_on_errors_alone():
    scaler = _make_scaler(min_samples=10, window_size=100, down_error_rate=0.15, down_throughput_drop=0.30)
    _fill_scaler_with_good_uploads(scaler, 70, bytes_=5_000_000, seconds=5.0)
    for _ in range(20):
        scaler.record_upload(0, 0.0, False)  # 22% errors but throughput stable
    new_count, _ = scaler.should_scale(4, 10_000_000)
    assert new_count == 4  # no change — throughput drop condition not met


def test_scale_down_does_not_trigger_on_throughput_drop_alone():
    scaler = _make_scaler(min_samples=10, window_size=100, down_error_rate=0.15, down_throughput_drop=0.30)
    _fill_scaler_with_good_uploads(scaler, 40, bytes_=5_000_000, seconds=5.0)
    # Add slow uploads — but no errors
    for _ in range(60):
        scaler.record_upload(500_000, 5.0, True)  # slow but all succeed
    new_count, _ = scaler.should_scale(4, 10_000_000)
    assert new_count == 4  # no change — error rate condition not met


def test_scale_down_never_goes_below_1():
    scaler = _make_scaler(min_samples=10, window_size=100, down_error_rate=0.15, down_throughput_drop=0.30)
    _fill_scaler_with_good_uploads(scaler, 40, bytes_=5_000_000, seconds=5.0)
    for _ in range(10):
        scaler.record_upload(0, 0.0, False)
    for _ in range(40):
        scaler.record_upload(100_000, 5.0, True)
    new_count, _ = scaler.should_scale(1, 10_000_000)
    assert new_count == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_scaler.py::test_scale_down_triggers_on_combined_error_and_throughput_drop tests/test_scaler.py::test_scale_down_does_not_trigger_on_errors_alone tests/test_scaler.py::test_scale_down_does_not_trigger_on_throughput_drop_alone tests/test_scaler.py::test_scale_down_never_goes_below_1 -v
```

Expected: FAIL — `should_scale` always returns `(current_workers, "")`.

- [ ] **Step 3: Implement scale-down in `should_scale`**

Replace the final `return current_workers, ""` line in `should_scale` with:

```python
        error_rate = 1.0 - (len(successes) / len(window))
        per_file_tp = [b / s for b, s in successes if s > 0]

        # --- Scale-DOWN (checked first, takes priority) ---
        if len(per_file_tp) >= 4 and error_rate > self._down_error_rate:
            full_median = statistics.median(per_file_tp)
            recent_count = max(1, len(per_file_tp) // 4)
            recent_median = statistics.median(per_file_tp[-recent_count:])
            if full_median > 0 and recent_median < full_median * (1 - self._down_throughput_drop):
                self._consecutive_headroom_checks = 0
                self._first_scale_done = True
                return max(1, current_workers - 1), (
                    f"Fehlerrate {error_rate:.0%}, Throughput -{self._down_throughput_drop:.0%}"
                )

        return current_workers, ""  # scale-up added in Task 3
```

Also add `error_rate` and `per_file_tp` computation before the scale-down block. The full `should_scale` body after the early-returns should now read:

```python
        successes = [(r.bytes_, r.seconds) for r in window if r.success]
        total_bytes = sum(b for b, _ in successes)
        total_seconds = sum(s for _, s in successes)

        if total_seconds <= 0 or not successes:
            return current_workers, ""

        error_rate = 1.0 - (len(successes) / len(window))
        per_file_tp = [b / s for b, s in successes if s > 0]

        # --- Scale-DOWN ---
        if len(per_file_tp) >= 4 and error_rate > self._down_error_rate:
            full_median = statistics.median(per_file_tp)
            recent_count = max(1, len(per_file_tp) // 4)
            recent_median = statistics.median(per_file_tp[-recent_count:])
            if full_median > 0 and recent_median < full_median * (1 - self._down_throughput_drop):
                self._consecutive_headroom_checks = 0
                self._first_scale_done = True
                return max(1, current_workers - 1), (
                    f"Fehlerrate {error_rate:.0%}, Throughput -{self._down_throughput_drop:.0%}"
                )

        return current_workers, ""  # scale-up added in Task 3
```

- [ ] **Step 4: Run all scaler tests**

```bash
python -m pytest tests/test_scaler.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add parquet_transform/scaler.py tests/test_scaler.py
git commit -m "feat: implement scale-down logic in AdaptiveScaler"
```

---

## Task 3: `AdaptiveScaler` — scale-up logic

**Files:**
- Modify: `parquet_transform/scaler.py`
- Modify: `tests/test_scaler.py`

- [ ] **Step 1: Write failing scale-up tests**

Add to `tests/test_scaler.py`:

```python
def test_first_scale_up_is_proportional():
    # 4 workers, each doing 5 MB in 5s = 1 MB/s per connection = 4 MB/s total.
    # headroom = floor(4_000_000 * 300 / 10_000_000 * 0.7) - 4 = floor(84) - 4 = 80
    # But capped at configured_max_workers=16, so new_count = 16.
    scaler = _make_scaler(min_samples=10, configured_max_workers=16)
    _fill_scaler_with_good_uploads(scaler, 50, bytes_=5_000_000, seconds=5.0)
    new_count, reason = scaler.should_scale(4, 10_000_000)
    assert new_count == 16  # capped at configured_max_workers
    assert "headroom" in reason.lower() or "kb/s" in reason.lower()


def test_first_scale_up_sets_first_scale_done():
    scaler = _make_scaler(min_samples=10, configured_max_workers=16)
    _fill_scaler_with_good_uploads(scaler, 50, bytes_=5_000_000, seconds=5.0)
    scaler.should_scale(4, 10_000_000)  # first call
    # Second call: same conditions but should only do incremental now
    _fill_scaler_with_good_uploads(scaler, 10)
    new_count_2, _ = scaler.should_scale(16, 10_000_000)
    # With 16 workers and same bandwidth, headroom is near 0 — no scale up
    assert new_count_2 == 16


def test_subsequent_scale_up_requires_two_stable_checks():
    # 2 workers, 2 MB/s total → headroom = floor(2_000_000*300/5_000_000*0.7)-2 = floor(84)-2 = 82 → up_max_step=4
    scaler = _make_scaler(min_samples=10, up_confirm_checks=2, up_max_step=4, configured_max_workers=32)
    _fill_scaler_with_good_uploads(scaler, 50, bytes_=5_000_000, seconds=5.0)
    # First check is proportional — consume it
    scaler.should_scale(2, 5_000_000)  # first call, proportional
    _fill_scaler_with_good_uploads(scaler, 10)
    # Now in incremental mode. Check 1 of 2:
    new_count_1, _ = scaler.should_scale(2, 5_000_000)
    assert new_count_1 == 2  # not yet — only 1 of 2 stable checks
    _fill_scaler_with_good_uploads(scaler, 10)
    # Check 2 of 2:
    new_count_2, reason = scaler.should_scale(2, 5_000_000)
    assert new_count_2 == 6  # 2 + min(headroom, 4) = 2 + 4 = 6
    assert "headroom" in reason.lower() or "kb/s" in reason.lower()


def test_scale_up_capped_at_configured_max():
    scaler = _make_scaler(min_samples=10, up_confirm_checks=1, up_max_step=4, configured_max_workers=5)
    _fill_scaler_with_good_uploads(scaler, 50, bytes_=5_000_000, seconds=5.0)
    # First check is proportional:
    new_count, _ = scaler.should_scale(4, 10_000_000)
    assert new_count == 5  # capped at configured_max_workers


def test_no_scale_up_when_no_headroom():
    # Workers already match bandwidth: 1 worker, 1 MB/s, p95=300MB → headroom≈0
    scaler = _make_scaler(min_samples=10, up_min_headroom=2, configured_max_workers=16)
    _fill_scaler_with_good_uploads(scaler, 50, bytes_=1_000_000, seconds=1.0)
    # total_bw = 1_000_000/1.0 * 1 = 1_000_000 B/s
    # headroom = floor(1_000_000 * 300 / 300_000_000 * 0.7) - 1 = floor(0.7) - 1 = 0 - 1 = -1
    new_count, _ = scaler.should_scale(1, 300_000_000)
    assert new_count == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_scaler.py::test_first_scale_up_is_proportional tests/test_scaler.py::test_first_scale_up_sets_first_scale_done tests/test_scaler.py::test_subsequent_scale_up_requires_two_stable_checks tests/test_scaler.py::test_scale_up_capped_at_configured_max tests/test_scaler.py::test_no_scale_up_when_no_headroom -v
```

Expected: FAIL.

- [ ] **Step 3: Implement scale-up in `should_scale`**

Replace the final `return current_workers, ""` line in `should_scale` (after the scale-down block) with:

```python
        # --- Scale-UP ---
        per_conn_bw = total_bytes / total_seconds  # bytes/s per connection slot
        total_bw = per_conn_bw * current_workers
        headroom = int(total_bw * self._upload_timeout_s / p95_bytes * 0.7) - current_workers
        measured_bw_kbs = int(total_bw / 1024)

        # Update consecutive headroom counter (state mutation — done without lock
        # since should_scale is not designed to be called concurrently; only
        # one thread (the finalize_success check) ever calls it).
        if headroom >= self._up_min_headroom:
            self._consecutive_headroom_checks += 1
        else:
            self._consecutive_headroom_checks = 0

        if not self._first_scale_done:
            self._first_scale_done = True
            self._consecutive_headroom_checks = 0
            if headroom > 0:
                new_count = min(self._configured_max_workers, current_workers + headroom)
                return new_count, f"{measured_bw_kbs} KB/s, {headroom} Slots headroom"
            return current_workers, ""

        if self._consecutive_headroom_checks >= self._up_confirm_checks:
            self._consecutive_headroom_checks = 0
            step = min(headroom, self._up_max_step)
            new_count = min(self._configured_max_workers, current_workers + step)
            return new_count, f"{measured_bw_kbs} KB/s, {headroom} Slots headroom"

        return current_workers, ""
```

- [ ] **Step 4: Run all scaler tests**

```bash
python -m pytest tests/test_scaler.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Run full test suite to check for regressions**

```bash
python -m pytest -v
```

Expected: all existing tests PASS.

- [ ] **Step 6: Commit**

```bash
git add parquet_transform/scaler.py tests/test_scaler.py
git commit -m "feat: implement scale-up logic in AdaptiveScaler"
```

---

## Task 4: Update `workers.py` — remove calibration, integrate scaler

**Files:**
- Modify: `parquet_transform/workers.py`

- [ ] **Step 1: Add import and new constants, remove old ones**

At the top of `workers.py`, add the import after the existing local imports:

```python
from parquet_transform.scaler import AdaptiveScaler
```

Remove these three constants entirely:
```python
UPLOAD_BANDWIDTH_FALLBACK_KBYTES_S: int = 350
_CALIB_BATCH_SIZE: int = 4
_CALIB_LARGE_FILE_THRESHOLD_MB: int = 8
```

Add these new constants in their place:
```python
SCALER_WINDOW_SIZE: int = 100
"""Rolling window size — number of upload measurements kept."""

SCALER_MIN_SAMPLES: int = 50
"""Minimum measurements collected before any scaling decision is made."""

SCALER_CHECK_INTERVAL: int = 150
"""Number of successfully completed files between scaling checks."""

SCALER_DOWN_ERROR_RATE: float = 0.15
"""Scale-down trigger: error rate threshold (fraction, e.g. 0.15 = 15%)."""

SCALER_DOWN_THROUGHPUT_DROP: float = 0.30
"""Scale-down trigger: recent throughput must be this far below window median."""

SCALER_UP_MIN_HEADROOM: int = 2
"""Scale-up trigger: minimum headroom slots required to consider scaling up."""

SCALER_UP_CONFIRM_CHECKS: int = 2
"""Scale-up trigger: headroom must be stable for this many consecutive checks."""

SCALER_UP_MAX_STEP: int = 4
"""Scale-up: maximum workers added per incremental check."""
```

- [ ] **Step 2: Replace `workers_calibrated` signal with `workers_scaled`**

In `TransformWorker`, find:
```python
    workers_calibrated = pyqtSignal(int, int)
```

Replace with:
```python
    workers_scaled = pyqtSignal(int, int, str, str)
    # (new_count, old_count, direction, reason)
    # direction: "up" | "down"
```

- [ ] **Step 3: Remove calibration variables from `run()`**

In `run()`, find and remove these two lines:
```python
        upload_measurements: list[tuple[int, float]] = []
        upload_meas_lock = threading.Lock()
```

- [ ] **Step 4: Remove the `needs_calibration` block**

Find and remove the entire block:
```python
        needs_calibration = (
            self._autoscale
            and p95_bytes >= _CALIB_LARGE_FILE_THRESHOLD_MB * 1024 * 1024
            and not self._dry_run
            and total >= _CALIB_BATCH_SIZE
            and self._worker_count > 2
        )
```

- [ ] **Step 5: Remove upload measurement collection from `worker_loop`**

In `worker_loop`, find and remove:
```python
                    # Collect upload timing for calibration
                    if result.upload_bytes > 0 and result.upload_seconds > 0:
                        with upload_meas_lock:
                            upload_measurements.append(
                                (result.upload_bytes, result.upload_seconds)
                            )
```

- [ ] **Step 6: Remove the entire calibration pass block**

Find and remove the block that starts with:
```python
        if needs_calibration:
```
...through and including:
```python
            self.workers_calibrated.emit(new_worker_count, measured_bw_kbs)
```

This is approximately 40 lines including the comment header `# Calibration pass`.

- [ ] **Step 7: Add scaler instantiation in `run()`**

Directly after the `start_time = perf_counter()` line, add:

```python
        # Adaptive scaler (only active when autoscale=True and not a dry run)
        scaler: AdaptiveScaler | None = None
        completed_since_check: int = 0
        if self._autoscale and not self._dry_run:
            scaler = AdaptiveScaler(
                window_size=SCALER_WINDOW_SIZE,
                min_samples=SCALER_MIN_SAMPLES,
                upload_timeout_s=UPLOAD_TIMEOUT_S,
                down_error_rate=SCALER_DOWN_ERROR_RATE,
                down_throughput_drop=SCALER_DOWN_THROUGHPUT_DROP,
                up_min_headroom=SCALER_UP_MIN_HEADROOM,
                up_confirm_checks=SCALER_UP_CONFIRM_CHECKS,
                up_max_step=SCALER_UP_MAX_STEP,
                configured_max_workers=self._worker_count,
            )
```

- [ ] **Step 8: Add scaler feed in `worker_loop`**

In `worker_loop`, after the `if result.status in {"success", "skipped"}:` block's `finalize_success(...)` call (but still inside the else-branch and the outer while loop), add upload recording. The best place is right after computing `result`, before the status check. Find this section in `worker_loop`:

```python
                    result = self._process_blob_once(worker_client, blob_name)
                    duration_tracker[blob_name] = (
                        duration_tracker.get(blob_name, 0.0) + result.duration_ms
                    )
                    aggregated_duration = duration_tracker[blob_name]
```

Add immediately after `aggregated_duration = ...`:

```python
                    # Feed upload measurement to scaler
                    if scaler is not None:
                        if result.upload_bytes > 0 and result.upload_seconds > 0:
                            scaler.record_upload(result.upload_bytes, result.upload_seconds, True)
                        elif result.status == "error":
                            scaler.record_upload(0, 0.0, False)
```

- [ ] **Step 9: Add periodic scale check in `finalize_success`**

In `run()`, modify `finalize_success` to trigger a scale check every `SCALER_CHECK_INTERVAL` completed files. Replace the existing `finalize_success` closure with:

```python
        def finalize_success(*, blob_name, worker_id, attempts, duration_ms,
                             skipped, note, size_bytes):
            nonlocal processed, completed, total_duration_ms, completed_since_check
            do_scale_check = False
            with stats_lock:
                processed += 1
                completed += 1
                completed_so_far = completed
                total_duration_ms += duration_ms
                if self._checkpoint:
                    self._checkpoint.advance_cursor(blob_name)
                if self._failed_list:
                    self._failed_list.remove(blob_name)
                if scaler is not None and not skipped:
                    completed_since_check += 1
                    if completed_since_check >= SCALER_CHECK_INTERVAL:
                        completed_since_check = 0
                        do_scale_check = True

            if do_scale_check:
                old_count = self._worker_count
                new_count, reason = scaler.should_scale(old_count, p95_bytes)
                if new_count != old_count:
                    self._worker_count = new_count
                    direction = "up" if new_count > old_count else "down"
                    self.workers_scaled.emit(new_count, old_count, direction, reason)

            self.progress.emit(completed_so_far, total, blob_name,
                               duration_ms, worker_id, skipped, note, size_bytes)
```

- [ ] **Step 10: Run the full test suite**

```bash
python -m pytest -v
```

Expected: all tests PASS. (Some tests that reference `workers_calibrated` or removed constants may need updating — fix any that fail.)

- [ ] **Step 11: Commit**

```bash
git add parquet_transform/workers.py parquet_transform/scaler.py
git commit -m "feat: replace startup calibration with continuous AdaptiveScaler in TransformWorker"
```

---

## Task 5: Update `main_window.py` — replace `workers_calibrated` handler

**Files:**
- Modify: `gui/main_window.py`

- [ ] **Step 1: Replace signal connection**

Find (around line 778):
```python
        self._transform_worker.workers_calibrated.connect(self._on_workers_calibrated)
```

Replace with:
```python
        self._transform_worker.workers_scaled.connect(self._on_workers_scaled)
```

- [ ] **Step 2: Replace the handler method**

Find and remove the entire `_on_workers_calibrated` method:
```python
    def _on_workers_calibrated(self, calibrated_count: int, measured_bw_kbs: int) -> None:
        """Called after the calibration pass measured real upload bandwidth."""
        self._worker_spin.setValue(calibrated_count)
        if measured_bw_kbs > 0:
            self._log_info(
                f"Auto-Scale: measured {measured_bw_kbs} KB/s upload bandwidth "
                f"→ {calibrated_count} worker(s) for remaining files."
            )
        else:
            self._log_info(
                f"Auto-Scale: calibration had no measurable uploads "
                f"→ keeping {calibrated_count} worker(s)."
            )
```

Add in its place:
```python
    def _on_workers_scaled(self, new_count: int, old_count: int, direction: str, reason: str) -> None:
        """Called when the AdaptiveScaler adjusts the worker count."""
        self._worker_spin.setValue(new_count)
        arrow = "↑" if direction == "up" else "↓"
        self._log_info(
            f"[Autoscale {arrow}] {old_count}→{new_count} workers — {reason}"
        )
```

- [ ] **Step 3: Run the application manually**

```bash
python main.py
```

Start a transform run with autoscale enabled. After ~150 files check the log for `[Autoscale ↑]` or `[Autoscale ↓]` entries.

- [ ] **Step 4: Run the full test suite**

```bash
python -m pytest -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add gui/main_window.py
git commit -m "feat: update main_window to handle workers_scaled signal"
```
