"""
Compact "System Resources" group box shown between the action row and the log.

Updates
-------
* ``update_snapshot(SystemSnapshot)``  — called every second from
  ``SystemMonitorWorker``; refreshes CPU, RAM and system-wide net bars.
* ``update_worker_throughput(measured_bps, capacity_bps)``  — called
  whenever the ``AdaptiveScaler`` runs a check; refreshes the Workers bar
  and the Bottleneck diagnosis label.
* ``clear_worker_throughput()``  — called when a run finishes/is cancelled;
  resets the worker-specific widgets to "—".
"""
from __future__ import annotations

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QGridLayout,
    QGroupBox,
    QLabel,
    QProgressBar,
)

from parquet_transform.system_monitor import SystemSnapshot

# ── colour thresholds for the bar chunks ────────────────────────────────────
_GREEN  = "#4caf50"
_ORANGE = "#ff9800"
_RED    = "#f44336"

_CHUNK_TPL = "QProgressBar::chunk {{ background-color: {color}; border-radius: 2px; }}"
_BAR_STYLE  = (
    "QProgressBar {{ border: 1px solid #aaa; border-radius: 3px; background: #f0f0f0; }}"
    " " + _CHUNK_TPL
)


def _bar_color(ratio: float) -> str:
    if ratio < 0.60:
        return _GREEN
    if ratio < 0.80:
        return _ORANGE
    return _RED


def _fmt_bps(bps: float) -> str:
    """Human-readable bytes/s string."""
    mbs = bps / (1024 * 1024)
    if mbs >= 1.0:
        return f"{mbs:.2f} MB/s"
    kbs = bps / 1024
    return f"{kbs:.1f} KB/s"


class ResourcesPanel(QGroupBox):
    """Live-updating resource monitor widget."""

    def __init__(self, parent=None) -> None:
        super().__init__("System Resources", parent)

        # Rolling scale for the two throughput bars — starts at 10 MB/s and
        # expands automatically as higher values are observed.  Never shrinks
        # (avoids bar bouncing when throughput fluctuates).
        self._net_scale_bps: float = 10 * 1024 * 1024   # 10 MB/s
        self._worker_scale_bps: float = 10 * 1024 * 1024

        # Last known capacity from the scaler (for bottleneck diagnosis)
        self._capacity_bps: float = 0.0
        self._worker_bps: float = 0.0

        self._setup_ui()

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def _setup_ui(self) -> None:
        small = QFont()
        small.setPointSize(8)

        grid = QGridLayout(self)
        grid.setSpacing(5)
        grid.setContentsMargins(8, 4, 8, 4)

        # ── Row 0: CPU ─ RAM ───────────────────────────────────────────
        grid.addWidget(self._label("CPU", small), 0, 0)
        self._cpu_bar = self._make_bar()
        grid.addWidget(self._cpu_bar, 0, 1)
        self._cpu_lbl = self._label("—", small, fixed_w=42)
        grid.addWidget(self._cpu_lbl, 0, 2)

        grid.addWidget(self._label("RAM", small), 0, 3)
        self._ram_bar = self._make_bar()
        grid.addWidget(self._ram_bar, 0, 4)
        self._ram_lbl = self._label("—", small, fixed_w=115)
        grid.addWidget(self._ram_lbl, 0, 5)

        # ── Row 1: Net ↑ ─ Workers ────────────────────────────────────
        grid.addWidget(self._label("Net ↑", small), 1, 0)
        self._net_bar = self._make_bar()
        grid.addWidget(self._net_bar, 1, 1)
        self._net_lbl = self._label("—", small, fixed_w=90)
        grid.addWidget(self._net_lbl, 1, 2)

        grid.addWidget(self._label("Workers", small), 1, 3)
        self._worker_bar = self._make_bar()
        grid.addWidget(self._worker_bar, 1, 4)
        self._worker_lbl = self._label("—", small, fixed_w=165)
        grid.addWidget(self._worker_lbl, 1, 5)

        # ── Row 2: Bottleneck diagnosis ────────────────────────────────
        self._bottleneck_lbl = QLabel("")
        self._bottleneck_lbl.setFont(small)
        self._bottleneck_lbl.setAlignment(
            Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        )
        grid.addWidget(self._bottleneck_lbl, 2, 0, 1, 6)

        # The two throughput columns stretch; the label columns are fixed.
        grid.setColumnStretch(1, 1)
        grid.setColumnStretch(4, 1)

    @staticmethod
    def _label(text: str, font: QFont, fixed_w: int | None = None) -> QLabel:
        lbl = QLabel(text)
        lbl.setFont(font)
        if fixed_w is not None:
            lbl.setFixedWidth(fixed_w)
        return lbl

    @staticmethod
    def _make_bar() -> QProgressBar:
        bar = QProgressBar()
        bar.setRange(0, 1000)   # 1/10 % resolution
        bar.setValue(0)
        bar.setTextVisible(False)
        bar.setFixedHeight(13)
        bar.setStyleSheet(_BAR_STYLE.format(color=_GREEN))
        return bar

    # ------------------------------------------------------------------
    # Public update API
    # ------------------------------------------------------------------

    def update_snapshot(self, snap: SystemSnapshot) -> None:
        """Refresh CPU, RAM and Net bars.  Safe to call from any thread
        (Qt queues cross-thread signal emissions)."""
        # CPU
        cpu = snap.cpu_pct
        self._cpu_bar.setValue(int(cpu * 10))
        self._cpu_lbl.setText(f"{cpu:.0f}%")
        self._set_bar_color(self._cpu_bar, cpu / 100.0)

        # RAM
        ram = snap.ram_pct
        self._ram_bar.setValue(int(ram * 10))
        used_gb  = snap.ram_used_bytes  / (1024 ** 3)
        total_gb = snap.ram_total_bytes / (1024 ** 3)
        self._ram_lbl.setText(f"{ram:.0f}% · {used_gb:.1f}/{total_gb:.1f} GB")
        self._set_bar_color(self._ram_bar, ram / 100.0)

        # System-wide net upload — scale bar to rolling max
        net_bps = snap.net_upload_bps
        self._net_scale_bps = max(self._net_scale_bps, net_bps * 1.1)
        ratio_net = net_bps / self._net_scale_bps if self._net_scale_bps > 0 else 0.0
        self._net_bar.setValue(int(ratio_net * 1000))
        self._net_lbl.setText(f"{_fmt_bps(net_bps)} sys")
        self._set_bar_color(self._net_bar, ratio_net)

        # Refresh bottleneck (needs CPU + current worker throughput)
        self._refresh_bottleneck(cpu, net_bps)

    def update_worker_throughput(self, measured_bps: float, capacity_bps: float) -> None:
        """Refresh the Workers bar and capacity annotation.

        ``measured_bps`` — total bytes/s across all active workers.
        ``capacity_bps`` — scaler's estimate of the maximum sustainable
                           throughput given current connection quality.
        """
        self._worker_bps = measured_bps
        self._capacity_bps = capacity_bps

        # Scale bar to whichever is larger: measured or capacity
        cap_for_scale = max(capacity_bps, measured_bps, 1.0)
        self._worker_scale_bps = max(self._worker_scale_bps, cap_for_scale * 1.1)

        ratio = min(measured_bps / self._worker_scale_bps, 1.0)
        self._worker_bar.setValue(int(ratio * 1000))
        self._set_bar_color(self._worker_bar, ratio)

        if capacity_bps > 0:
            cap_str = f"  cap ~{_fmt_bps(capacity_bps)}"
        else:
            cap_str = ""
        self._worker_lbl.setText(f"{_fmt_bps(measured_bps)}{cap_str}")

    def clear_worker_throughput(self) -> None:
        """Reset worker-specific widgets (call when a run ends or is cancelled)."""
        self._worker_bps = 0.0
        self._capacity_bps = 0.0
        self._worker_bar.setValue(0)
        self._worker_lbl.setText("—")
        self._bottleneck_lbl.setText("")
        self._bottleneck_lbl.setStyleSheet("")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _refresh_bottleneck(self, cpu_pct: float, net_bps: float) -> None:
        """Update the one-line bottleneck diagnosis at the bottom of the panel."""
        if self._capacity_bps <= 0:
            # No worker data yet — only show CPU overload warning
            if cpu_pct > 85:
                self._bottleneck_lbl.setText(
                    "⚡ CPU-bound — processing power is the bottleneck"
                )
                self._bottleneck_lbl.setStyleSheet("color: #cc6600;")
            else:
                self._bottleneck_lbl.setText("")
                self._bottleneck_lbl.setStyleSheet("")
            return

        net_util = self._worker_bps / self._capacity_bps if self._capacity_bps > 0 else 0.0

        if cpu_pct > 75:
            self._bottleneck_lbl.setText(
                "⚡ CPU-bound — adding more workers won't help here"
            )
            self._bottleneck_lbl.setStyleSheet("color: #cc6600;")
        elif net_util > 0.88:
            self._bottleneck_lbl.setText(
                "⚡ Network-bound — Azure upload is the bottleneck"
            )
            self._bottleneck_lbl.setStyleSheet("color: #aa8800;")
        elif cpu_pct < 45 and net_util < 0.55:
            self._bottleneck_lbl.setText("✓ Capacity available")
            self._bottleneck_lbl.setStyleSheet("color: #007700;")
        else:
            self._bottleneck_lbl.setText("")
            self._bottleneck_lbl.setStyleSheet("")

    @staticmethod
    def _set_bar_color(bar: QProgressBar, ratio: float) -> None:
        color = _bar_color(ratio)
        bar.setStyleSheet(_BAR_STYLE.format(color=color))
