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
        hot_error_rate=0.5,
        hot_error_min_samples=None,
        recovery_error_ceiling=0.05,
    )
    defaults.update(overrides)
    return AdaptiveScaler(**defaults)


def _fill_scaler_with_good_uploads(scaler: AdaptiveScaler, count: int, bytes_: int = 5_000_000, seconds: float = 5.0) -> None:
    for _ in range(count):
        scaler.record_upload(bytes_, seconds, True)


def test_hot_error_flag_sets_once():
    scaler = _make_scaler(min_samples=5, hot_error_rate=0.5, hot_error_min_samples=5)
    for _ in range(5):
        scaler.record_upload(0, 0.0, False)
    assert scaler.consume_hot_error_flag() is True
    assert scaler.consume_hot_error_flag() is False


def test_window_ready_after_min_samples():
    scaler = _make_scaler(min_samples=3)
    scaler.record_upload(1_000_000, 1.0, True)
    assert not scaler.window_ready()
    scaler.record_upload(1_000_000, 1.0, True)
    assert not scaler.window_ready()
    scaler.record_upload(1_000_000, 1.0, True)
    assert scaler.window_ready()


def test_recovery_ceiling_blocks_scale_up_until_errors_drop():
    scaler = _make_scaler(min_samples=5, configured_max_workers=10, recovery_error_ceiling=0.05)
    for _ in range(4):
        scaler.record_upload(5_000_000, 5.0, True)
    scaler.record_upload(0, 0.0, False)  # 20% errors
    new_count, _ = scaler.should_scale(4, 1_000_000)
    assert new_count == 4  # blocked due to error ceiling
    # Push error rate below 5% (1 failure over 25 records)
    for _ in range(21):
        scaler.record_upload(5_000_000, 5.0, True)
    new_count, _ = scaler.should_scale(4, 1_000_000)
    assert new_count > 4


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


def test_scale_down_triggers_on_combined_error_and_throughput_drop():
    scaler = _make_scaler(
        min_samples=10,
        window_size=100,
        down_error_rate=0.15,
        down_throughput_drop=0.30,
    )
    # Fill with good uploads first (sets the median baseline)
    _fill_scaler_with_good_uploads(scaler, 35, bytes_=5_000_000, seconds=5.0)
    # Add failing records (16% error rate: 13 out of 81 total)
    for _ in range(13):
        scaler.record_upload(0, 0.0, False)
    # Add slow uploads (throughput < 70% of baseline)
    for _ in range(33):
        scaler.record_upload(1_000_000, 5.0, True)  # 200 KB/s vs baseline 1 MB/s
    new_count, reason = scaler.should_scale(4, 10_000_000)
    assert new_count == 3
    assert "error" in reason.lower() or "throughput" in reason.lower()


def test_scale_down_does_not_trigger_on_moderate_errors_alone():
    """Moderate error rate (22%) without throughput drop does NOT trigger scale-down."""
    scaler = _make_scaler(min_samples=10, window_size=100, down_error_rate=0.15, down_throughput_drop=0.30)
    _fill_scaler_with_good_uploads(scaler, 70, bytes_=5_000_000, seconds=5.0)
    scaler.should_scale(4, 10_000_000)  # consume first proportional check
    for _ in range(20):
        scaler.record_upload(0, 0.0, False)  # 22% errors but throughput stable
    new_count, _ = scaler.should_scale(4, 10_000_000)
    assert new_count == 4  # no change — error rate below 50% and throughput stable


def test_critical_error_rate_triggers_immediate_halving():
    """>=50% error rate halves workers immediately without throughput check."""
    scaler = _make_scaler(min_samples=5, window_size=20)
    _fill_scaler_with_good_uploads(scaler, 5, bytes_=5_000_000, seconds=5.0)
    for _ in range(5):
        scaler.record_upload(0, 0.0, False)  # 50% error rate
    new_count, reason = scaler.should_scale(16, 10_000_000)
    assert new_count == 8  # halved
    assert "critical" in reason.lower() or "halving" in reason.lower()


def test_critical_error_rate_handles_all_failures():
    """Halving still triggers even if the window only contains failures."""
    scaler = _make_scaler(min_samples=5, window_size=20)
    for _ in range(5):
        scaler.record_upload(0, 0.0, False)  # 100% errors, zero successes
    new_count, reason = scaler.should_scale(10, 10_000_000)
    assert new_count == 5
    assert "error" in reason.lower()


def test_critical_error_rate_does_not_go_below_2():
    scaler = _make_scaler(min_samples=5, window_size=20)
    _fill_scaler_with_good_uploads(scaler, 3, bytes_=5_000_000, seconds=5.0)
    for _ in range(7):
        scaler.record_upload(0, 0.0, False)  # 70% error rate
    new_count, _ = scaler.should_scale(3, 10_000_000)
    assert new_count == 2  # floor is 2, not 1


def test_scale_down_does_not_trigger_on_throughput_drop_alone():
    scaler = _make_scaler(min_samples=10, window_size=100, down_error_rate=0.15, down_throughput_drop=0.30)
    _fill_scaler_with_good_uploads(scaler, 40, bytes_=5_000_000, seconds=5.0)
    scaler.should_scale(4, 10_000_000)  # consume first proportional check
    # Add slow uploads — but no errors
    for _ in range(60):
        scaler.record_upload(500_000, 5.0, True)  # slow but all succeed
    new_count, _ = scaler.should_scale(4, 10_000_000)
    assert new_count == 4  # no change — error rate condition not met


def test_scale_down_never_goes_below_2():
    scaler = _make_scaler(min_samples=10, window_size=100,
                          down_error_rate=0.15, down_throughput_drop=0.30)
    _fill_scaler_with_good_uploads(scaler, 35, bytes_=5_000_000, seconds=5.0)
    for _ in range(13):                             # 13/81 = 16%, above threshold
        scaler.record_upload(0, 0.0, False)
    for _ in range(33):
        scaler.record_upload(100_000, 5.0, True)    # 20 KB/s — well below 70% of baseline
    new_count, _ = scaler.should_scale(2, 10_000_000)
    assert new_count == 2


def test_first_scale_up_is_slow_start():
    # 4 workers, 1 MB/s per connection = 4 MB/s total.
    # headroom = 80, but slow-start caps step to min(headroom, min(current=4, up_max_step=4)) = 4
    # → new_count = 8, not a jump to the full headroom estimate.
    scaler = _make_scaler(min_samples=10, configured_max_workers=16)
    _fill_scaler_with_good_uploads(scaler, 50, bytes_=5_000_000, seconds=5.0)
    new_count, reason = scaler.should_scale(4, 10_000_000)
    assert new_count == 8  # slow-start: +min(headroom, min(current, up_max_step)) = +4
    assert "slow-start" in reason.lower()


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
    # 2 workers → slow-start step = min(82, min(2, 4)) = 2 → new_count_0 = 4
    # At 4 workers total_bw doubles → plateau guard passes.
    # Then up_confirm_checks=2 still requires two consecutive stable checks.
    scaler = _make_scaler(min_samples=10, up_confirm_checks=2, up_max_step=4, configured_max_workers=32)
    _fill_scaler_with_good_uploads(scaler, 50, bytes_=5_000_000, seconds=5.0)
    # First check: slow-start
    new_count_0, _ = scaler.should_scale(2, 5_000_000)
    assert new_count_0 == 4  # slow-start: +min(82, min(2, 4)) = +2

    _fill_scaler_with_good_uploads(scaler, 10)
    # Incremental check 1 of 2 (at the new worker count so total_bw grows past plateau):
    new_count_1, _ = scaler.should_scale(new_count_0, 5_000_000)
    assert new_count_1 == new_count_0  # not yet — only 1 of 2 stable checks

    _fill_scaler_with_good_uploads(scaler, 10)
    # Incremental check 2 of 2:
    new_count_2, reason = scaler.should_scale(new_count_0, 5_000_000)
    assert new_count_2 == new_count_0 + 4  # 4 + min(headroom, 4) = 4 + 4 = 8
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


def test_plateau_guard_blocks_second_jump_after_saturation():
    """After a scale-up from N=2→4, a saturated run where total throughput drops
    must be blocked by the plateau guard.

    The guard compares *total* bps (per_conn × N).  True saturation is when the
    link is fully loaded and adding workers causes total throughput to fall — not
    merely per-connection throughput halving while total grows (which is normal
    shared-bandwidth behaviour).

    Threshold arithmetic (plateau_threshold=0.05):
      At N=2, slow-start records per_conn=1 MB/s → _last_total_bw = 2 MB/s.
      Guard fires when: total_bw < 2 MB/s × 0.95 = 1.9 MB/s
                    i.e. per_conn at N=4 < 475 KB/s.
      10 baseline (1 MB/s) + 15 saturated (100 KB/s):
        per_conn = (50+7.5) MB / (50+75) s ≈ 460 KB/s → total = 1.84 MB/s < 1.9 ✓
    """
    scaler = _make_scaler(
        min_samples=5,
        up_confirm_checks=1,
        up_max_step=8,
        configured_max_workers=32,
        plateau_threshold=0.05,
    )
    # Baseline: 10 good uploads, 1 MB/s per connection
    _fill_scaler_with_good_uploads(scaler, 10, bytes_=5_000_000, seconds=5.0)

    # First check at N=2 → slow-start to N=4 (step = min(82, min(2,8)) = 2)
    # _last_scale_up_bps = total_bw at N=2 = 1 MB/s × 2 = 2 MB/s
    new_count_0, _ = scaler.should_scale(2, 5_000_000)
    assert new_count_0 == 4

    # Simulate true saturation: total throughput drops (link fully loaded, new workers
    # introduce contention overhead that reduces throughput below the N=2 baseline)
    for _ in range(15):
        scaler.record_upload(500_000, 5.0, True)  # 100 KB/s each → per_conn ≈ 460 KB/s

    # Plateau guard must fire: total_bw (≈1.84 MB/s) < _last (2 MB/s) × 0.95 = 1.9 MB/s
    new_count_1, reason_1 = scaler.should_scale(new_count_0, 5_000_000)
    assert new_count_1 == new_count_0, (
        f"Expected plateau guard to block scale-up, got {new_count_0}→{new_count_1} ({reason_1!r})"
    )


def test_plateau_guard_allows_scale_up_when_total_bw_grows():
    """When total throughput grows after a scale-up (bandwidth scales with workers),
    the plateau guard must NOT block the next scale-up.

    On a shared link per-connection throughput halves when doubling workers, but
    total_bw stays roughly constant — well above the (1 − plateau_threshold) threshold
    because _last_total_bw was recorded at the lower worker count.
    Here uploads are constant (1 MB/s per conn) so total_bw doubles: 2 MB/s → 4 MB/s,
    which is clearly above 0.95 × 2 MB/s = 1.9 MB/s.
    """
    scaler = _make_scaler(
        min_samples=5,
        up_confirm_checks=1,
        up_max_step=8,
        configured_max_workers=32,
        plateau_threshold=0.05,
    )
    _fill_scaler_with_good_uploads(scaler, 10, bytes_=5_000_000, seconds=5.0)

    # First check at N=2 → slow-start
    new_count_0, _ = scaler.should_scale(2, 5_000_000)
    assert new_count_0 == 4

    # Per-connection throughput unchanged — bandwidth scaled linearly with workers
    _fill_scaler_with_good_uploads(scaler, 10, bytes_=5_000_000, seconds=5.0)

    # Plateau guard should NOT block; up_confirm_checks=1 means one check suffices
    new_count_1, _ = scaler.should_scale(new_count_0, 5_000_000)
    assert new_count_1 > new_count_0, (
        f"Expected scale-up from {new_count_0}, got {new_count_1}"
    )


def test_bandwidth_cap_not_set_by_plateau_guard():
    """Plateau guard blocking a scale-up must NOT permanently lower _bandwidth_cap.

    Scenario: total throughput dips transiently after a scale-up (e.g. a brief
    TCP-connection burst when new threads start together).  The plateau guard
    correctly pauses the next scale-up, but the bandwidth ceiling must remain at
    configured_max so the scaler can continue once the transient clears.

    Threshold arithmetic (plateau_threshold=0.05):
      At N=2 slow-start: _last_total_bw = 1 MB/s × 2 = 2 MB/s.
      Guard fires when total_bw < 2 MB/s × 0.95 = 1.9 MB/s.
      10 baseline (1 MB/s) + 15 jitter (100 KB/s):
        per_conn ≈ 460 KB/s → total at N=4 = 1.84 MB/s < 1.9 ✓ guard fires.
      But _bandwidth_cap is updated from raw measurements before the guard runs,
      so the ceiling must still reflect the full measured capacity (configured_max).
    """
    scaler = _make_scaler(
        min_samples=5,
        up_confirm_checks=1,
        up_max_step=8,
        configured_max_workers=32,
        plateau_threshold=0.05,
    )
    # Baseline at high throughput
    _fill_scaler_with_good_uploads(scaler, 10, bytes_=5_000_000, seconds=5.0)

    # Slow-start: 2 → 4
    new_count_0, _ = scaler.should_scale(2, 5_000_000)
    assert new_count_0 == 4

    # Inject jitter records that drop total_bw below the guard threshold.
    # 10 baseline (1 MB/s) + 15 jitter (100 KB/s):
    #   per_conn = (50 + 7.5) MB / (50 + 75) s ≈ 460 KB/s → total = 1.84 MB/s < 1.9 ✓
    for _ in range(15):
        scaler.record_upload(500_000, 5.0, True)  # 100 KB/s

    # Plateau guard must fire — but cap must remain at configured max (32)
    new_count_1, _ = scaler.should_scale(new_count_0, 5_000_000)
    assert new_count_1 == new_count_0   # blocked — correct
    assert scaler.get_bandwidth_cap() == 32  # cap unchanged — no permanent lock-in


def test_bandwidth_cap_set_by_critical_error_halving():
    """A critical error rate (>=50%) must lock _bandwidth_cap at the pre-halving
    worker count so the scaler won't re-escalate above it after recovery.
    """
    scaler = _make_scaler(
        min_samples=5,
        up_confirm_checks=1,
        configured_max_workers=32,
    )
    _fill_scaler_with_good_uploads(scaler, 5, bytes_=5_000_000, seconds=5.0)

    # Critical error rate at 20 workers → halve to 10, cap locked at 20
    for _ in range(5):
        scaler.record_upload(0, 0.0, False)  # 50% errors → critical
    new_count, reason = scaler.should_scale(20, 5_000_000)
    assert new_count == 10               # halved
    assert "critical" in reason.lower() or "halving" in reason.lower()
    assert scaler.get_bandwidth_cap() == 20  # ceiling locked at pre-halving count

    # After recovery: scaler must not scale above 20 (the learned ceiling)
    _fill_scaler_with_good_uploads(scaler, 50, bytes_=5_000_000, seconds=5.0)
    # With configured_max=32 and cap=20, formula_target is bounded by cap
    new_count_recovery, _ = scaler.should_scale(10, 5_000_000)
    assert new_count_recovery <= 20, (
        f"Scaler exceeded learned ceiling: went to {new_count_recovery}"
    )


def test_crash_cascade_ceiling_locked_at_first_halving():
    """When a cascade of halvings occurs (240→120→60→30→…), the crash ceiling must
    be set at the FIRST halving only (240), not lowered on every subsequent halving.
    If it cascaded, the ceiling would end up at 2-3 and block all scale-up after
    recovery — exactly the bug seen in production.

    Uses a small window (window_size=20, min_samples=5) so that 20 clean recovery
    records are enough to push the error rate below 5% and allow scale-up.
    """
    scaler = _make_scaler(
        min_samples=5,
        window_size=20,
        up_confirm_checks=1,
        configured_max_workers=256,
    )
    # Seed window: 5 good + 5 bad = 50% errors → critical
    _fill_scaler_with_good_uploads(scaler, 5, bytes_=5_000_000, seconds=5.0)
    for _ in range(5):
        scaler.record_upload(0, 0.0, False)

    # First halving: 240 → 120 — crash ceiling must be set to 240
    workers = 240
    workers, _ = scaler.should_scale(workers, 5_000_000)
    assert workers == 120
    assert scaler.get_bandwidth_cap() == 240, (
        f"Ceiling after first halving should be 240, got {scaler.get_bandwidth_cap()}"
    )

    # Second halving: still ≥50% errors, 120 → 60 — ceiling must STAY at 240
    for _ in range(5):
        scaler.record_upload(0, 0.0, False)
    workers, _ = scaler.should_scale(workers, 5_000_000)
    assert workers == 60
    assert scaler.get_bandwidth_cap() == 240, (
        f"Ceiling after second halving cascaded down to {scaler.get_bandwidth_cap()} (bug)"
    )

    # Third halving: 60 → 30 — ceiling must STAY at 240
    for _ in range(5):
        scaler.record_upload(0, 0.0, False)
    workers, _ = scaler.should_scale(workers, 5_000_000)
    assert workers == 30
    assert scaler.get_bandwidth_cap() == 240, (
        f"Ceiling after third halving cascaded down to {scaler.get_bandwidth_cap()} (bug)"
    )

    # Flush the error window with 20 clean records (window_size=20, so all errors
    # are evicted).  Error rate drops to 0% → below recovery_error_ceiling (5%).
    _fill_scaler_with_good_uploads(scaler, 20, bytes_=5_000_000, seconds=5.0)

    # Scaler must now be able to scale back up.  With crash_ceiling=240 and a
    # healthy window, formula_target will be >2.  The previous ceiling-cascade bug
    # would have left _crash_ceiling=30 (or lower), then dynamic_cap = min(30, 84) = 30,
    # making formula_target = min(30, 2+step) = 4 which > 2 — but at deeper cascade
    # levels (ceiling=3) formula_target = min(3, 2+step) = 3 which > 2 still...
    # The real regression is ceiling=3 when current_workers=3 → can never go to 4+.
    # So let's test from 3 workers, where the bug manifests fully:
    new_count_from_3, _ = scaler.should_scale(3, 5_000_000)
    assert new_count_from_3 > 3, (
        f"Scaler stuck at 3 workers after recovery — crash ceiling cascaded too low "
        f"(_crash_ceiling should be 240 but effective cap={scaler.get_bandwidth_cap()})"
    )
    # The effective cap is min(dynamic_measurement, _crash_ceiling).
    # dynamic_measurement at 3 workers × 1 MB/s = 3 MB/s → raw_cap = int(3M*300/5M*0.7)=126.
    # With _crash_ceiling=240 the cap should be min(240, 126)=126.
    # If _crash_ceiling had cascaded to 30, the cap would be min(30, 126)=30.
    # Either way scale-up from 3 works as long as crash_ceiling > 3.
    # Verify the crash ceiling is bounded away from 3:
    assert scaler.get_bandwidth_cap() > 3, (
        f"Effective bandwidth cap is {scaler.get_bandwidth_cap()} — "
        "crash ceiling cascaded down to 3 (the scale-up-blocking level)"
    )
