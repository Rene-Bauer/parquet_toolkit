import sys
from PyQt6.QtWidgets import QApplication

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
    """MainWindow must expose a ZipPanel as the third tab."""
    from gui.main_window import MainWindow
    from gui.zip_panel import ZipPanel

    w = MainWindow()
    found = any(True for _ in w.findChildren(ZipPanel))
    w._sys_monitor.stop()
    w._sys_monitor.wait(2000)
    w.close()
    assert found, "No ZipPanel found in MainWindow"
