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
