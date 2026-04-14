"""
System resource monitoring — CPU, RAM, and network upload/download rates.

Designed to be polled from a background QThread at ~1 s intervals.
The first call to ``snapshot()`` always returns 0.0 for the byte-rate fields
because the delta needs a reference point; subsequent calls are accurate.
"""
from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter

import psutil


@dataclass(frozen=True)
class SystemSnapshot:
    """Immutable snapshot of current system resource usage."""
    cpu_pct: float           # 0–100, averaged across all cores
    ram_pct: float           # 0–100
    ram_used_bytes: int
    ram_total_bytes: int
    net_upload_bps: float    # system-wide bytes/s sent (all interfaces, all processes)
    net_download_bps: float  # system-wide bytes/s received


class SystemMonitor:
    """Computes resource deltas between successive ``snapshot()`` calls.

    Not thread-safe — create one instance per polling thread.
    """

    def __init__(self) -> None:
        # Prime CPU measurement (first call to cpu_percent with interval=None
        # always returns 0.0; calling it once here initialises the reference).
        psutil.cpu_percent(interval=None)

        counters = psutil.net_io_counters()
        self._last_sent: int = counters.bytes_sent
        self._last_recv: int = counters.bytes_recv
        self._last_time: float = perf_counter()

    def snapshot(self) -> SystemSnapshot:
        """Return a fresh ``SystemSnapshot``.  Call at most once per second."""
        cpu = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory()

        now = perf_counter()
        counters = psutil.net_io_counters()

        elapsed = max(now - self._last_time, 1e-6)
        upload_bps = (counters.bytes_sent - self._last_sent) / elapsed
        download_bps = (counters.bytes_recv - self._last_recv) / elapsed

        # Clamp negative deltas (counter wrap or interface reset)
        upload_bps = max(upload_bps, 0.0)
        download_bps = max(download_bps, 0.0)

        self._last_sent = counters.bytes_sent
        self._last_recv = counters.bytes_recv
        self._last_time = now

        return SystemSnapshot(
            cpu_pct=cpu,
            ram_pct=mem.percent,
            ram_used_bytes=mem.used,
            ram_total_bytes=mem.total,
            net_upload_bps=upload_bps,
            net_download_bps=download_bps,
        )
