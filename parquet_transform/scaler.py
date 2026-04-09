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
    """Adaptive worker-count scaler based on rolling upload throughput.

    NOTE: `should_scale` is not thread-safe and must be called from a single thread only.
    Only `record_upload` is safe for concurrent callers.
    """
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
                actual_drop = (full_median - recent_median) / full_median
                return max(1, current_workers - 1), (
                    f"error rate {error_rate:.0%}, throughput drop {actual_drop:.0%}"
                )

        return current_workers, ""  # scale-up added in Task 3
