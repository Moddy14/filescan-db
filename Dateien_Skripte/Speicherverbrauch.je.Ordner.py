#!/usr/bin/env python3
import sys
import os
import csv
import sqlite3
from PyQt5 import QtWidgets, QtCore, QtGui

def convert_size(size_bytes, unit):
    """
    Konvertiert eine Größe in Bytes in die gewünschte Einheit.
    Unterstützte Einheiten: KB, MB, GB, TB.
    """
    try:
        size_bytes = float(size_bytes)
    except (ValueError, TypeError):
        return 0
    if unit == 'KB':
        return size_bytes / 1024
    elif unit == 'MB':
        return size_bytes / (1024 ** 2)
    elif unit == 'GB':
        return size_bytes / (1024 ** 3)
    elif unit == 'TB':
        return size_bytes / (1024 ** 4)
    else:
        return size_bytes

class NumericTableWidgetItem(QtWidgets.QTableWidgetItem):
    """
    QTableWidgetItem, das einen numerischen Wert (in Qt.UserRole) speichert,
    um eine korrekte numerische Sortierung zu ermöglichen.
    """
    def __lt__(self, other):
        try:
            self_data = float(self.data(QtCore.Qt.UserRole))
            other_data = float(other.data(QtCore.Qt.UserRole))
            return self_data < other_data
        except (ValueError, TypeError):
            return super().__lt__(other)

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, db_path):
        super().__init__()
        self.db_path = db_path
        self.conn = None
        self.current_drive_id = None
        self.cached_data = None  # Zwischenspeicher für geladene Daten
        self.init_db()
        self.init_ui()
        
    def init_db(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
        except sqlite3.Error as e:
            QtWidgets.QMessageBox.critical(self, "Datenbank Fehler",
                                           f"Fehler beim Verbinden mit der Datenbank: {e}")
            sys.exit(1)
            
    def init_ui(self):
        self.setWindowTitle("Dateien je Ordner - Übersicht")
        self.resize(900, 650)
        _icon = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'icons', 'disk_usage.ico')
        if os.path.exists(_icon):
            self.setWindowIcon(QtGui.QIcon(_icon))
        
        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QtWidgets.QVBoxLayout(central_widget)
        
        controls_layout = QtWidgets.QHBoxLayout()
        
        self.drive_combo = QtWidgets.QComboBox()
        self.drive_combo.setToolTip("Wählen Sie ein Laufwerk aus")
        controls_layout.addWidget(QtWidgets.QLabel("Laufwerk:"))
        controls_layout.addWidget(self.drive_combo)
        self.load_drives()
        self.drive_combo.currentIndexChanged.connect(self.drive_changed)
        
        self.unit_combo = QtWidgets.QComboBox()
        self.unit_combo.addItems(['Bytes', 'KB', 'MB', 'GB', 'TB'])
        self.unit_combo.setToolTip("Wählen Sie die anzuzeigende Einheit")
        controls_layout.addWidget(QtWidgets.QLabel("Einheit:"))
        controls_layout.addWidget(self.unit_combo)
        # Bei Änderung der Einheit erfolgt nur ein Update der Darstellung
        self.unit_combo.currentIndexChanged.connect(self.update_table)
        
        refresh_btn = QtWidgets.QPushButton("Aktualisieren")
        refresh_btn.setToolTip("Daten neu laden")
        refresh_btn.clicked.connect(self.refresh_data)
        controls_layout.addWidget(refresh_btn)
        
        export_btn = QtWidgets.QPushButton("Exportieren")
        export_btn.setToolTip("Daten als CSV exportieren")
        export_btn.clicked.connect(self.export_csv)
        controls_layout.addWidget(export_btn)
        
        controls_layout.addStretch()
        main_layout.addLayout(controls_layout)
        
        self.table = QtWidgets.QTableWidget()
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(["Verzeichnis", "Dateien", "Größe"])
        self.table.setSortingEnabled(True)
        self.table.setAlternatingRowColors(True)
        main_layout.addWidget(self.table)
        
        self.status_bar = self.statusBar()
        self.status_bar.showMessage("Bereit")
        
        self.init_menu()
        
        if self.drive_combo.count() > 0:
            self.drive_combo.setCurrentIndex(0)
            self.drive_changed(self.drive_combo.currentIndex())
    
    def init_menu(self):
        menubar = self.menuBar()
        file_menu = menubar.addMenu("Datei")
        
        export_action = QtWidgets.QAction("Als CSV exportieren", self)
        export_action.triggered.connect(self.export_csv)
        file_menu.addAction(export_action)
        
        exit_action = QtWidgets.QAction("Beenden", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        help_menu = menubar.addMenu("Hilfe")
        about_action = QtWidgets.QAction("Über", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
        
    def show_about(self):
        QtWidgets.QMessageBox.information(self, "Über", 
            "Dateien je Ordner - Übersicht\nVerbessert und erweitert von Ihrem Script-Team.\nVersion 2.0")
        
    def load_drives(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id, name FROM drives ORDER BY name")
            drives = cursor.fetchall()
            self.drive_combo.clear()
            if drives:
                for drive in drives:
                    self.drive_combo.addItem(drive[1], drive[0])
            else:
                self.drive_combo.addItem("Keine Laufwerke gefunden", None)
        except sqlite3.Error as e:
            QtWidgets.QMessageBox.critical(self, "DB Fehler",
                                           f"Fehler beim Laden der Laufwerke: {e}")
            
    def load_data(self):
        """
        Liest die Verzeichniseinträge des aktuell gewählten Laufwerks ein.
        Dabei werden nur die Werte des jeweiligen Verzeichnisses geholt.
        """
        if self.current_drive_id is None:
            return []
        try:
            cursor = self.conn.cursor()
            # Angepasste SQL für optimierte Datenbankstruktur
            query = """
            SELECT directories.full_path, COUNT(files.id) AS file_count, IFNULL(SUM(files.size), 0) AS total_size
            FROM directories
            LEFT JOIN files ON directories.id = files.directory_id
            WHERE directories.drive_id = ?
            GROUP BY directories.id
            ORDER BY directories.full_path
            """
            cursor.execute(query, (self.current_drive_id,))
            results = cursor.fetchall()
            return results
        except sqlite3.Error as e:
            QtWidgets.QMessageBox.critical(self, "DB Fehler",
                                           f"Fehler beim Laden der Daten: {e}")
            return []
        
    def aggregate_data(self, raw_data):
        """
        Aggregiert die Daten so, dass die Werte höher geordneter Verzeichnisse die
        ihrer untergeordneten Verzeichnisse beinhalten.
        
        Es wird angenommen, dass in raw_data jeder Eintrag als Tupel (Pfad, file_count, total_size)
        vorliegt. Mittels os.path.dirname wird der Elternpfad ermittelt.
        """
        # Erstellen eines Dictionaries: {Verzeichnis: {"count": ..., "size": ...}}
        agg = {path: {"count": count, "size": size} for path, count, size in raw_data}
        # Sortierung nach Pfadlänge absteigend, damit zunächst tiefere Verzeichnisse verarbeitet werden.
        sorted_paths = sorted(agg.keys(), key=lambda p: len(p), reverse=True)
        
        for path in sorted_paths:
            parent = os.path.dirname(path)
            # Überspringen, wenn sich der Elternpfad nicht ändert oder nicht im Dictionary vorhanden ist
            if parent and parent != path and parent in agg:
                agg[parent]["count"] += agg[path]["count"]
                agg[parent]["size"] += agg[path]["size"]
        
        # Rückgabe als Liste sortiert nach dem Verzeichnisnamen
        aggregated = [(path, agg[path]["count"], agg[path]["size"]) for path in agg]
        aggregated.sort(key=lambda x: x[0])
        return aggregated
            
    def drive_changed(self, index):
        drive_id = self.drive_combo.itemData(index)
        if drive_id is not None:
            self.current_drive_id = drive_id
            # Daten laden und im Cache ablegen
            self.cached_data = self.load_data()
            self.update_table()
        else:
            self.cached_data = None
            self.table.setRowCount(0)
            
    def update_table(self):
        """
        Aktualisiert die Darstellung der Tabelle auf Basis des Caches.
        Dabei werden die aggregierten Werte (inklusive aller untergeordneten Verzeichnisse)
        verwendet und die Werte anhand der aktuell ausgewählten Einheit umgerechnet.
        """
        if self.cached_data is None:
            self.cached_data = self.load_data()
        unit = self.unit_combo.currentText()
        
        # Aggregation inklusive Subdirectories
        aggregated = self.aggregate_data(self.cached_data)
        
        self.table.setRowCount(len(aggregated))
        size_header = "Größe" if unit == "Bytes" else f"Größe ({unit})"
        self.table.setHorizontalHeaderLabels(["Verzeichnis", "Dateien", size_header])
        
        for row_idx, (directory, file_count, total_size) in enumerate(aggregated):
            if unit != "Bytes":
                conv_size = convert_size(total_size, unit)
                size_str = f"{conv_size:.2f}"
                numeric_value = conv_size
            else:
                size_str = str(total_size)
                numeric_value = total_size
                
            self.table.setItem(row_idx, 0, QtWidgets.QTableWidgetItem(directory))
            self.table.setItem(row_idx, 1, QtWidgets.QTableWidgetItem(str(file_count)))
            
            size_item = NumericTableWidgetItem(size_str)
            size_item.setData(QtCore.Qt.UserRole, numeric_value)
            self.table.setItem(row_idx, 2, size_item)
            
        self.table.resizeColumnsToContents()
        self.status_bar.showMessage(f"{len(aggregated)} Verzeichnisse geladen.", 3000)
            
    def refresh_data(self):
        """
        Lädt die Laufwerksliste und zugehörige Daten neu und aktualisiert den Cache.
        """
        self.load_drives()
        if self.drive_combo.count() > 0:
            self.drive_combo.setCurrentIndex(0)
        self.cached_data = self.load_data()
        self.update_table()
        self.status_bar.showMessage("Daten aktualisiert", 3000)
            
    def export_csv(self):
        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, "CSV exportieren", "",
                                                        "CSV Dateien (*.csv)")
        if not path:
            return
        try:
            with open(path, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                headers = [self.table.horizontalHeaderItem(col).text()
                           for col in range(self.table.columnCount())]
                writer.writerow(headers)
                for row_idx in range(self.table.rowCount()):
                    row_data = []
                    for col in range(self.table.columnCount()):
                        item = self.table.item(row_idx, col)
                        row_data.append(item.text() if item is not None else "")
                    writer.writerow(row_data)
            self.status_bar.showMessage("CSV erfolgreich exportiert", 5000)
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Export Fehler",
                                           f"Fehler beim Exportieren: {e}")
            
    def closeEvent(self, event):
        if self.conn:
            self.conn.close()
        event.accept()

def main():
    if len(sys.argv) < 2:
        QtWidgets.QMessageBox.critical(None, "Usage Error",
                                       "Usage: {} <db_path>\n".format(sys.argv[0]))
        sys.exit(1)
        
    db_path = sys.argv[1]
    if not os.path.exists(db_path):
        QtWidgets.QMessageBox.critical(None, "Dateifehler",
                                       f"Die Datenbankdatei '{db_path}' wurde nicht gefunden.")
        sys.exit(1)
    
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow(db_path)
    window.show()
    print("GUI gestartet.")
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()
