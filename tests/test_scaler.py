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


def _fill_scaler_with_good_uploads(scaler: AdaptiveScaler, count: int, bytes_: int = 5_000_000, seconds: float = 5.0) -> None:
    for _ in range(count):
        scaler.record_upload(bytes_, seconds, True)


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
    scaler = _make_scaler(min_samples=10, window_size=100,
                          down_error_rate=0.15, down_throughput_drop=0.30)
    _fill_scaler_with_good_uploads(scaler, 35, bytes_=5_000_000, seconds=5.0)
    for _ in range(13):                             # 13/81 = 16%, above threshold
        scaler.record_upload(0, 0.0, False)
    for _ in range(33):
        scaler.record_upload(100_000, 5.0, True)    # 20 KB/s — well below 70% of baseline
    new_count, _ = scaler.should_scale(1, 10_000_000)
    assert new_count == 1
