from gui.workers import SCALER_CHECK_INTERVAL, TransformWorker


class DummyScaler:
    def __init__(self, ready: bool = True, new_count: int | None = None):
        self.ready = ready
        self.new_count = new_count
        self.window_ready_calls = 0
        self.should_scale_calls = 0

    def window_ready(self) -> bool:
        self.window_ready_calls += 1
        return self.ready

    def should_scale(self, current_workers: int, p95_bytes: int):
        self.should_scale_calls += 1
        if self.new_count is None:
            return current_workers, ""
        return self.new_count, "mock"

    def compute_throughput_stats(self, current_workers: int, p95_bytes: int):
        return 0.0, 0.0

    def record_throughput_observation(self, n_workers: int, total_bps: float) -> None:
        pass

    def usl_just_activated(self) -> bool:
        return False

    def get_usl_result(self):
        return None, None, None

    def consume_hot_error_flag(self) -> bool:  # pragma: no cover - only for interface completeness
        return False


def _make_worker() -> TransformWorker:
    return TransformWorker(
        connection_string="",
        container="container",
        prefix="prefix",
        col_configs=[],
        output_prefix=None,
        dry_run=True,
        worker_count=4,
        max_attempts=1,
        autoscale=True,
    )


def test_maybe_run_scale_check_ignores_when_no_trigger():
    worker = _make_worker()
    scaler = DummyScaler()
    worker._completed_since_scale_check = 0
    worker._pending_force_scale = False
    worker._maybe_run_scale_check(scaler, 1_000_000)
    assert scaler.window_ready_calls == 0
    assert scaler.should_scale_calls == 0


def test_maybe_run_scale_check_force_path_updates_workers():
    worker = _make_worker()
    scaler = DummyScaler(new_count=worker._worker_count - 1)
    worker._pending_force_scale = True
    worker._maybe_run_scale_check(scaler, 1_000_000)
    assert scaler.window_ready_calls == 1
    assert scaler.should_scale_calls == 1
    assert worker._worker_count == 3
    assert worker._pending_force_scale is False


def test_maybe_run_scale_check_interval_resets_counter():
    worker = _make_worker()
    scaler = DummyScaler(new_count=worker._worker_count)
    worker._completed_since_scale_check = SCALER_CHECK_INTERVAL
    worker._pending_force_scale = False
    worker._maybe_run_scale_check(scaler, 1_000_000)
    assert scaler.should_scale_calls == 1
    assert worker._completed_since_scale_check == 0
