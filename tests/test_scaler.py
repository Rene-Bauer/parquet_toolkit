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
    """After a scale-up from 2→10, a saturated 10-worker run (low per-conn bps)
    must not immediately be allowed to jump to 18 workers.

    Root-cause of the 2→10→18 cascade bug: the old guard compared *total* bps
    at N=10 against *total* bps at N=2.  With 5× more workers total throughput
    naturally grows 5×, trivially passing the 5% threshold.  The new guard
    compares *per-connection* bps, which drops under saturation.
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
    new_count_0, _ = scaler.should_scale(2, 5_000_000)
    assert new_count_0 == 4

    # Simulate saturation: per-connection throughput has halved (500 KB/s instead of 1 MB/s)
    for _ in range(10):
        scaler.record_upload(2_500_000, 5.0, True)  # 500 KB/s each

    # Plateau guard must fire: per_conn (500 KB/s) < last_per_conn (1 MB/s) × 0.95
    new_count_1, reason_1 = scaler.should_scale(new_count_0, 5_000_000)
    assert new_count_1 == new_count_0, (
        f"Expected plateau guard to block scale-up, got {new_count_0}→{new_count_1} ({reason_1!r})"
    )


def test_plateau_guard_allows_scale_up_when_per_conn_stable():
    """When per-connection throughput stays flat after a scale-up (no saturation),
    the plateau guard must NOT block the next scale-up.
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
