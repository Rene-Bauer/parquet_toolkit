"""
Adaptive worker-count scaler based on rolling upload throughput.

Two phases run in parallel and compare notes:

Phase B — AIMD + plateau guard (active from the first check)
    Scale up by at most ``up_max_step`` workers per check, but only when total
    throughput has grown by at least ``plateau_threshold`` since the last
    scale-up.  This prevents the "big formula jump → saturation cascade" pattern
    and is directly analogous to TCP's additive-increase / multiplicative-
    decrease asymmetry.

Phase C — Universal Scalability Law (USL) curve fit
    Activates once ``usl_min_levels`` distinct worker-count levels each have
    ``usl_min_samples_per_level`` throughput observations.  Fits the USL model

        C(N) = N / (1 + α(N-1) + βN(N-1))

    using ordinary least squares to find α (contention) and β (coherency).
    The analytical peak:

        N_opt = sqrt((1 − α) / β)     [when β > 0]

    is compared against Phase B's formula target.  When they agree (within
    ``usl_agree_tolerance``) USL's N_opt is enforced as a ceiling; when they
    disagree the scaler takes the conservative of the two and adds only +1
    worker per check until both estimates converge.
"""
from __future__ import annotations

import math
import statistics
import threading
from collections import deque
from typing import NamedTuple


class _UploadRecord(NamedTuple):
    bytes_: int
    seconds: float
    success: bool


class AdaptiveScaler:
    """Adaptive worker-count scaler.

    NOTE: ``should_scale`` is not thread-safe and must be called from a single
    thread only.  ``record_upload``, ``record_throughput_observation``, and
    ``get_usl_result`` are safe for concurrent callers.
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
        hot_error_rate: float = 0.5,
        hot_error_min_samples: int | None = None,
        recovery_error_ceiling: float = 0.05,
        overrun_headroom_threshold: int = 1,
        # Phase B
        plateau_threshold: float = 0.05,
        # Phase C
        usl_min_levels: int = 5,
        usl_min_samples_per_level: int = 3,
        usl_agree_tolerance: float = 0.20,
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
        self._hot_error_rate = hot_error_rate
        self._hot_error_min_samples = hot_error_min_samples or min_samples
        self._recovery_error_ceiling = recovery_error_ceiling
        self._hot_error_flag = False
        self._hot_error_cooldown: int = 0
        self._overrun_headroom_threshold = overrun_headroom_threshold

        # Phase B — plateau guard
        self._plateau_threshold = plateau_threshold
        self._last_scale_up_bps: float = 0.0

        # Phase C — USL
        self._usl_min_levels = usl_min_levels
        self._usl_min_samples_per_level = usl_min_samples_per_level
        self._usl_agree_tolerance = usl_agree_tolerance
        self._throughput_by_workers: dict[int, list[float]] = {}
        self._usl_alpha: float | None = None
        self._usl_beta: float | None = None
        self._usl_n_opt: int | None = None
        self._usl_activation_fired: bool = False  # for one-shot "just activated" notification

    # ------------------------------------------------------------------
    # Thread-safe data ingestion
    # ------------------------------------------------------------------

    def record_upload(self, bytes_: int, seconds: float, success: bool) -> None:
        """Thread-safe: record one upload measurement into the rolling window."""
        with self._lock:
            self._window.append(_UploadRecord(bytes_, seconds, success))
            if self._hot_error_cooldown > 0:
                self._hot_error_cooldown -= 1
                return
            window = list(self._window)
            if len(window) >= self._hot_error_min_samples:
                success_count = sum(1 for r in window if r.success)
                error_rate = 1.0 - (success_count / len(window))
                if error_rate >= self._hot_error_rate and not self._hot_error_flag:
                    self._hot_error_flag = True

    def record_throughput_observation(self, n_workers: int, total_bps: float) -> None:
        """Thread-safe: log a (N, total_bps) observation for USL curve fitting.

        Should be called once per scaler check, *before* ``should_scale``, at
        the current (pre-decision) worker count and total measured throughput.
        USL fitting is triggered automatically once enough levels are populated.
        """
        if n_workers < 1 or total_bps <= 0:
            return
        with self._lock:
            bucket = self._throughput_by_workers.setdefault(n_workers, [])
            bucket.append(total_bps)
            # Re-fit whenever we gain a new qualifying level
            levels_ready = sum(
                1 for obs in self._throughput_by_workers.values()
                if len(obs) >= self._usl_min_samples_per_level
            )
            if levels_ready >= self._usl_min_levels:
                self._fit_usl_locked()

    def consume_hot_error_flag(self) -> bool:
        """Return True once per detected hot-error spike."""
        with self._lock:
            flagged = self._hot_error_flag
            self._hot_error_flag = False
            if flagged:
                self._hot_error_cooldown = self._hot_error_min_samples
            return flagged

    def usl_just_activated(self) -> bool:
        """Return True exactly once — the first time the USL model becomes ready.

        Thread-safe.  Designed to be polled after each ``should_scale`` call so
        the caller can emit a log message when Phase C comes online.
        """
        with self._lock:
            if self._usl_n_opt is not None and not self._usl_activation_fired:
                self._usl_activation_fired = True
                return True
            return False

    def get_usl_result(self) -> tuple[float | None, float | None, int | None]:
        """Return ``(alpha, beta, n_opt)`` from the latest USL fit.

        Returns ``(None, None, None)`` when Phase C has not yet activated.
        Thread-safe.
        """
        with self._lock:
            return self._usl_alpha, self._usl_beta, self._usl_n_opt

    # ------------------------------------------------------------------
    # Readiness
    # ------------------------------------------------------------------

    def window_ready(self) -> bool:
        """Return True once enough rolling-window samples are present."""
        with self._lock:
            return len(self._window) >= self._min_samples

    # ------------------------------------------------------------------
    # Throughput statistics (for UI)
    # ------------------------------------------------------------------

    def compute_throughput_stats(
        self, current_workers: int, p95_bytes: int
    ) -> tuple[float, float]:
        """Return ``(measured_total_bps, estimated_capacity_bps)``.

        Thread-safe.
        """
        with self._lock:
            window = list(self._window)

        successes = [
            (r.bytes_, r.seconds)
            for r in window
            if r.success and r.seconds > 0
        ]
        if not successes or p95_bytes <= 0:
            return 0.0, 0.0

        total_bytes   = sum(b for b, _ in successes)
        total_seconds = sum(s for _, s in successes)
        if total_seconds <= 0:
            return 0.0, 0.0

        per_conn_bps  = total_bytes / total_seconds
        measured_bps  = per_conn_bps * current_workers

        max_workers_float = per_conn_bps * self._upload_timeout_s / p95_bytes * 0.7
        max_workers_float = min(max_workers_float, self._configured_max_workers)
        capacity_bps = per_conn_bps * max(max_workers_float, 1.0)

        return measured_bps, capacity_bps

    # ------------------------------------------------------------------
    # Main scaling decision
    # ------------------------------------------------------------------

    def should_scale(self, current_workers: int, p95_bytes: int) -> tuple[int, str]:
        """Return ``(new_worker_count, reason_string)``.

        Returns ``(current_workers, "")`` when no change is warranted.
        Not thread-safe — call from a single thread only.
        """
        with self._lock:
            window = list(self._window)

        if len(window) < self._min_samples or p95_bytes <= 0:
            return current_workers, ""

        successes = [(r.bytes_, r.seconds) for r in window if r.success]
        window_error_rate = 1.0 - (len(successes) / len(window))

        # ── Scale-DOWN (checked first, highest priority) ───────────────────

        # Hot path: critical error rate → halve workers
        if window_error_rate >= self._hot_error_rate and current_workers > 2:
            new_count = max(2, current_workers // 2)
            self._consecutive_headroom_checks = 0
            self._first_scale_done = True
            self._last_scale_up_bps = 0.0  # release plateau guard after recovery
            return new_count, (
                f"critical error rate {window_error_rate:.0%}, halving workers"
            )

        if not successes:
            return current_workers, ""

        total_bytes = sum(b for b, _ in successes)
        total_seconds = sum(s for _, s in successes)

        if total_seconds <= 0:
            return current_workers, ""

        per_file_tp = [b / s for b, s in successes if s > 0]

        # Normal: error rate + throughput drop → −1 worker
        if len(per_file_tp) >= 4 and window_error_rate > self._down_error_rate:
            full_median = statistics.median(per_file_tp)
            recent_count = max(1, len(per_file_tp) // 4)
            recent_median = statistics.median(per_file_tp[-recent_count:])
            if full_median > 0 and recent_median < full_median * (1 - self._down_throughput_drop):
                self._consecutive_headroom_checks = 0
                self._first_scale_done = True
                self._last_scale_up_bps = 0.0
                actual_drop = (full_median - recent_median) / full_median
                return max(2, current_workers - 1), (
                    f"error rate {window_error_rate:.0%}, throughput drop {actual_drop:.0%}"
                )

        # Bandwidth overrun: workers exceed sustainable capacity (low error rate)
        if window_error_rate <= self._recovery_error_ceiling and current_workers > 2:
            per_conn_bw_ov = total_bytes / total_seconds
            headroom_ov = (
                int(per_conn_bw_ov * current_workers * self._upload_timeout_s / p95_bytes * 0.7)
                - current_workers
            )
            if headroom_ov < -self._overrun_headroom_threshold:
                self._consecutive_headroom_checks = 0
                self._first_scale_done = True
                self._last_scale_up_bps = 0.0
                return max(2, current_workers - 1), (
                    f"bandwidth overrun (headroom {headroom_ov:+d} slots)"
                )

        # ── Scale-UP ───────────────────────────────────────────────────────

        if window_error_rate > self._recovery_error_ceiling:
            self._consecutive_headroom_checks = 0
            return current_workers, ""

        per_conn_bw = total_bytes / total_seconds
        total_bw = per_conn_bw * current_workers
        headroom = int(total_bw * self._upload_timeout_s / p95_bytes * 0.7) - current_workers
        measured_bw_kbs = int(total_bw / 1024)

        # Phase B — plateau guard: block scale-up if per-connection throughput has
        # dropped by more than plateau_threshold since the last scale-up event.
        # Comparing *per-connection* bps (not total) means adding more workers and
        # naturally seeing higher total throughput doesn't trivially bypass the guard —
        # only genuine per-connection efficiency surviving the scale-up does.
        if (
            self._last_scale_up_bps > 0
            and per_conn_bw < self._last_scale_up_bps * (1.0 - self._plateau_threshold)
        ):
            self._consecutive_headroom_checks = 0
            return current_workers, ""

        if headroom >= self._up_min_headroom:
            self._consecutive_headroom_checks += 1
        else:
            self._consecutive_headroom_checks = 0

        # ── First scale: slow-start (cap to min(current_workers, up_max_step)) ──
        if not self._first_scale_done:
            self._first_scale_done = True
            self._consecutive_headroom_checks = 0
            if headroom > 0:
                # Slow-start: never jump by more than the current worker count or
                # up_max_step in one shot (replaces the old "jump to full headroom").
                step = min(headroom, max(1, min(current_workers, self._up_max_step)))
                formula_target = min(self._configured_max_workers, current_workers + step)
                reason_base = f"slow-start +{step} ({measured_bw_kbs} KB/s, {headroom} slots headroom)"
                new_count, reason = self._apply_usl_ceiling(
                    current_workers, formula_target, reason_base, measured_bw_kbs
                )
                if new_count > current_workers:
                    self._last_scale_up_bps = per_conn_bw
                    return new_count, reason
            return current_workers, ""

        # ── Subsequent scales: confirm headroom ──────────────────────────────
        if self._consecutive_headroom_checks >= self._up_confirm_checks:
            self._consecutive_headroom_checks = 0
            step = min(headroom, self._up_max_step)
            formula_target = min(self._configured_max_workers, current_workers + step)
            reason_base = f"{measured_bw_kbs} KB/s, {headroom} slots headroom"
            new_count, reason = self._apply_usl_ceiling(
                current_workers, formula_target, reason_base, measured_bw_kbs
            )
            if new_count > current_workers:
                self._last_scale_up_bps = per_conn_bw
                return new_count, reason

        return current_workers, ""

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _apply_usl_ceiling(
        self,
        current_workers: int,
        formula_target: int,
        formula_reason: str,
        measured_bw_kbs: int,
    ) -> tuple[int, str]:
        """Compare the Phase-B formula target against the USL N_opt (Phase C).

        Returns ``(final_target, final_reason)``.  If ``final_target ==
        current_workers`` the caller should treat it as no change.
        """
        with self._lock:
            usl_n_opt = self._usl_n_opt
            usl_alpha = self._usl_alpha
            usl_beta  = self._usl_beta

        if usl_n_opt is None:
            # Phase C not ready yet — Phase B runs alone
            return formula_target, formula_reason

        # Already at or above USL optimum — block scale-up entirely
        if current_workers >= usl_n_opt:
            return current_workers, ""

        diff_ratio = abs(formula_target - usl_n_opt) / max(formula_target, usl_n_opt, 1)

        if diff_ratio <= self._usl_agree_tolerance:
            # Agreement: use USL N_opt as ceiling (take the conservative of the two)
            target = min(formula_target, usl_n_opt)
            reason = (
                f"{measured_bw_kbs} KB/s — formula={formula_target}, "
                f"USL N_opt={usl_n_opt} (α={usl_alpha:.3f} β={usl_beta:.4f}) — agreed"
            )
        else:
            # Disagreement: be maximally conservative, add only +1 and wait for
            # more data to resolve which estimate is right.
            target = min(formula_target, usl_n_opt, current_workers + 1)
            reason = (
                f"{measured_bw_kbs} KB/s — formula={formula_target} vs "
                f"USL N_opt={usl_n_opt} (α={usl_alpha:.3f} β={usl_beta:.4f}) "
                f"— disagreement, conservative +1"
            )

        return max(current_workers, target), reason

    def _fit_usl_locked(self) -> None:
        """Fit the USL curve from accumulated (N, bps) observations.

        Must be called with ``self._lock`` held.

        Uses ordinary least squares on the linearised USL form:

            y_i = α · x1_i + β · x2_i
            where  y_i  = N_i / C_i − 1
                   x1_i = N_i − 1
                   x2_i = N_i · (N_i − 1)
                   C_i  = T(N_i) / T_1_est   (speedup vs single worker)
                   T_1_est = T(N_ref) / N_ref  (single-worker throughput, extrapolated)

        Solved analytically via the 2×2 normal equations (no numpy needed).
        """
        # Build (N, median_bps) table for qualifying levels only
        data: list[tuple[int, float]] = sorted(
            (n, statistics.median(obs))
            for n, obs in self._throughput_by_workers.items()
            if len(obs) >= self._usl_min_samples_per_level
        )
        if len(data) < self._usl_min_levels:
            return

        # Extrapolate single-worker throughput from lowest observed N
        n_ref, t_ref = data[0]
        t1_est = t_ref / n_ref  # bytes/s per worker at N_ref (≈ unconstrained single-worker)
        if t1_est <= 0:
            return

        # Accumulate normal-equation sums
        s_x1sq = s_x1x2 = s_x2sq = s_x1y = s_x2y = 0.0
        for n, t in data:
            c = t / t1_est   # speedup
            if c <= 0:
                continue
            y  = n / c - 1.0
            x1 = float(n - 1)
            x2 = float(n * (n - 1))
            s_x1sq += x1 * x1
            s_x1x2 += x1 * x2
            s_x2sq += x2 * x2
            s_x1y  += x1 * y
            s_x2y  += x2 * y

        det = s_x1sq * s_x2sq - s_x1x2 * s_x1x2
        if abs(det) < 1e-12:
            return

        alpha = (s_x2sq * s_x1y - s_x1x2 * s_x2y) / det
        beta  = (s_x1sq * s_x2y - s_x1x2 * s_x1y) / det

        # Physical constraint: both must be non-negative
        alpha = max(0.0, min(1.0, alpha))
        beta  = max(0.0, min(1.0, beta))

        self._usl_alpha = alpha
        self._usl_beta  = beta

        # Analytical peak: N_opt = sqrt((1 − α) / β)
        if beta > 1e-6 and alpha < 1.0:
            n_opt_f = math.sqrt((1.0 - alpha) / beta)
            self._usl_n_opt = int(max(2, min(round(n_opt_f), self._configured_max_workers)))
        else:
            # β ≈ 0: pure contention model, throughput grows monotonically until the
            # hardware ceiling — set N_opt to max so Phase C doesn't block scale-up.
            self._usl_n_opt = self._configured_max_workers
