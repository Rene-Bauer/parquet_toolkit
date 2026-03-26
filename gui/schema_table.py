"""
Schema table widget: displays Parquet column schema with per-column
transform dropdowns.
"""
from __future__ import annotations

import pyarrow as pa
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QComboBox,
    QHeaderView,
    QTableWidget,
    QTableWidgetItem,
)

from parquet_transform import transforms as tr
from parquet_transform.processor import ColumnConfig

_NO_CHANGE = "— no change —"


class SchemaTable(QTableWidget):
    """
    A three-column table showing:
      Column Name | Current Type | Transform (editable ComboBox)

    After the user selects transforms, call get_column_configs() to retrieve
    the list of ColumnConfig objects for non-identity (changed) columns.
    """

    HEADERS = ["Column", "Current Type", "Transform"]
    COL_NAME = 0
    COL_TYPE = 1
    COL_TRANSFORM = 2

    def __init__(self, parent=None):
        super().__init__(0, 3, parent)
        self.setHorizontalHeaderLabels(self.HEADERS)
        self.horizontalHeader().setSectionResizeMode(
            self.COL_NAME, QHeaderView.ResizeMode.ResizeToContents
        )
        self.horizontalHeader().setSectionResizeMode(
            self.COL_TYPE, QHeaderView.ResizeMode.ResizeToContents
        )
        self.horizontalHeader().setSectionResizeMode(
            self.COL_TRANSFORM, QHeaderView.ResizeMode.Stretch
        )
        self.setSelectionBehavior(
            QTableWidget.SelectionBehavior.SelectRows
        )
        self.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.verticalHeader().setVisible(False)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_schema(self, schema: pa.Schema) -> None:
        """Populate the table from a PyArrow schema."""
        self.setRowCount(0)
        for field in schema:
            self._add_row(field.name, field.type)

    def get_column_configs(self) -> list[ColumnConfig]:
        """
        Return ColumnConfig objects for every row where the user has chosen
        a transform (i.e. not "— no change —").
        """
        configs: list[ColumnConfig] = []
        for row in range(self.rowCount()):
            combo: QComboBox = self.cellWidget(row, self.COL_TRANSFORM)
            selected = combo.currentData()  # registry key or None
            if selected is None:
                continue
            col_name = self.item(row, self.COL_NAME).text()
            configs.append(ColumnConfig(name=col_name, transform=selected))
        return configs

    def clear_schema(self) -> None:
        self.setRowCount(0)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _add_row(self, col_name: str, arrow_type: pa.DataType) -> None:
        row = self.rowCount()
        self.insertRow(row)

        # Column name (read-only)
        name_item = QTableWidgetItem(col_name)
        name_item.setFlags(Qt.ItemFlag.ItemIsEnabled)
        self.setItem(row, self.COL_NAME, name_item)

        # Current type (read-only)
        type_item = QTableWidgetItem(str(arrow_type))
        type_item.setFlags(Qt.ItemFlag.ItemIsEnabled)
        self.setItem(row, self.COL_TYPE, type_item)

        # Transform ComboBox
        combo = self._build_combo(arrow_type)
        self.setCellWidget(row, self.COL_TRANSFORM, combo)

    def _build_combo(self, arrow_type: pa.DataType) -> QComboBox:
        combo = QComboBox()
        combo.addItem(_NO_CHANGE, userData=None)

        for key, display_name in tr.list_transforms():
            combo.addItem(display_name, userData=key)

        # Auto-suggest
        suggested_key = tr.get_suggested(arrow_type)
        if suggested_key is not None:
            for i in range(combo.count()):
                if combo.itemData(i) == suggested_key:
                    combo.setCurrentIndex(i)
                    break

        return combo
