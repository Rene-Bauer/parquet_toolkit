import sys
from PyQt6.QtWidgets import QApplication, QTabWidget

_app = QApplication.instance() or QApplication(sys.argv)


def test_collector_panel_has_resources_panel():
    """MainWindow creates CollectorPanel with a ResourcesPanel attached."""
    from gui.main_window import MainWindow
    from gui.resources_panel import ResourcesPanel

    w = MainWindow()
    assert isinstance(w._collector_panel.resources_panel, ResourcesPanel)
    # Cleanup
    w._sys_monitor.stop()
    w._sys_monitor.wait(2000)
    w.close()


def test_main_window_has_zip_tab():
    """MainWindow must expose a ZipPanel as the third tab (index 2, label 'ZIP → Parquet')."""
    from gui.main_window import MainWindow
    from gui.zip_panel import ZipPanel

    w = MainWindow()
    tabs = w.findChild(QTabWidget)
    zip_panel = w._zip_panel
    w._sys_monitor.stop()
    w._sys_monitor.wait(2000)
    w.close()

    assert isinstance(zip_panel, ZipPanel), "w._zip_panel is not a ZipPanel"
    assert tabs is not None, "No QTabWidget found in MainWindow"
    assert tabs.widget(2) is zip_panel, "ZipPanel is not the third tab (index 2)"
    assert tabs.tabText(2) == "ZIP → Parquet", f"Third tab label unexpected: {tabs.tabText(2)!r}"


def test_pending_entry_scan_driven_skips_checkpoint():
    """When _PendingEntry carries blob_names, _start_next_prefix calls _start_transform
    directly without going through _resolve_checkpoint."""
    from gui.main_window import MainWindow

    win = MainWindow()

    started_with = {}

    def fake_start_transform(**kwargs):
        started_with.update(kwargs)

    win._start_transform = fake_start_transform
    win._run_col_configs = []
    win._file_count = 0
    win._dry_run = False

    from gui.main_window import _PendingEntry
    win._pending_prefixes = [
        _PendingEntry(
            prefix="DataCollector/LiveData/14-02-2026",
            blob_names=["a.parquet", "b.parquet"],
            blob_sizes={"a.parquet": 100, "b.parquet": 200},
        )
    ]
    win._current_prefix_index = 0

    resolve_called = []
    win._resolve_checkpoint = lambda *a, **kw: resolve_called.append(True) or None

    win._start_next_prefix()

    assert not resolve_called, "_resolve_checkpoint must not be called for scan-driven entries"
    assert started_with.get("blob_names") == ["a.parquet", "b.parquet"]
    assert started_with.get("blob_sizes") == {"a.parquet": 100, "b.parquet": 200}
    assert started_with.get("prefix_override") == "DataCollector/LiveData/14-02-2026"

    win._sys_monitor.stop()
    win._sys_monitor.wait(2000)
    win.close()
