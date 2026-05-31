#!/usr/bin/env python3
import sys
import sqlite3
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QComboBox, QPushButton, QTableWidget, QTableWidgetItem, QFormLayout, QMessageBox
)
from PyQt5.QtCore import Qt

class MainWindow(QMainWindow):
    def __init__(self, db_path):
        super().__init__()
        self.setWindowTitle("Dateivergleich aus DB")
        self.db_path = db_path
        # Öffne DB-Verbindung (nur Lesezugriff)
        try:
            self.conn = sqlite3.connect(self.db_path)
        except Exception as e:
            QMessageBox.critical(self, "DB Fehler", f"Fehler beim Öffnen der DB: {e}")
            sys.exit(1)
        self.initUI()
        self.loadDrives()
        self.loadExtensions()
    
    def initUI(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout()
        central.setLayout(layout)
        
        # Formular-Layout für die Auswahlfelder
        form_layout = QFormLayout()
        layout.addLayout(form_layout)
        
        # Auswahl Laufwerk 1 und Directory 1
        self.drive1_cb = QComboBox()
        self.dir1_cb = QComboBox()
        form_layout.addRow("Laufwerk 1:", self.drive1_cb)
        form_layout.addRow("Directory 1:", self.dir1_cb)
        
        # Auswahl Laufwerk 2 und Directory 2
        self.drive2_cb = QComboBox()
        self.dir2_cb = QComboBox()
        form_layout.addRow("Laufwerk 2:", self.drive2_cb)
        form_layout.addRow("Directory 2:", self.dir2_cb)
        
        # Auswahl Dateiendung
        self.ext_cb = QComboBox()
        form_layout.addRow("Dateiendung:", self.ext_cb)
        
        # Button zum Vergleichen
        self.compare_btn = QPushButton("Vergleichen")
        layout.addWidget(self.compare_btn)
        self.compare_btn.clicked.connect(self.compareFiles)
        
        # Tabelle für das Ergebnis
        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["Dateiname", "Größe", "Pfad", "Fundorte"])
        layout.addWidget(self.table)
        
        # Verbinde Signale zur Aktualisierung der Directory-Listen
        self.drive1_cb.currentIndexChanged.connect(self.loadDirectories1)
        self.drive2_cb.currentIndexChanged.connect(self.loadDirectories2)
    
    def loadDrives(self):
        """Lädt alle Laufwerke aus der DB und füllt beide Laufwerks-ComboBoxes."""
        query = "SELECT id, name FROM drives ORDER BY name"
        cur = self.conn.cursor()
        try:
            cur.execute(query)
            drives = cur.fetchall()
        except sqlite3.Error as e:
            QMessageBox.critical(self, "DB Fehler", f"Fehler beim Laden der Laufwerke: {e}")
            return
        
        # Speichere Laufwerksdaten als Liste von Tuple (id, name)
        self.drives = drives
        self.drive1_cb.clear()
        self.drive2_cb.clear()
        for drive in drives:
            display = drive[1]
            self.drive1_cb.addItem(display, drive[0])
            self.drive2_cb.addItem(display, drive[0])
        
        # Lade Directories für die jeweils erste Auswahl
        if drives:
            self.loadDirectories1()
            self.loadDirectories2()
    
    def loadDirectories1(self):
        """Lädt Directory-Liste für Laufwerk 1."""
        drive_id = self.drive1_cb.currentData()
        if drive_id is None:
            return
        query = "SELECT id, path FROM directories WHERE drive_id = ? ORDER BY path"
        cur = self.conn.cursor()
        try:
            cur.execute(query, (drive_id,))
            dirs = cur.fetchall()
        except sqlite3.Error as e:
            QMessageBox.critical(self, "DB Fehler", f"Fehler beim Laden der Directories: {e}")
            return
        self.dir1_cb.clear()
        for d in dirs:
            self.dir1_cb.addItem(d[1], d[0])
    
    def loadDirectories2(self):
        """Lädt Directory-Liste für Laufwerk 2."""
        drive_id = self.drive2_cb.currentData()
        if drive_id is None:
            return
        query = "SELECT id, path FROM directories WHERE drive_id = ? ORDER BY path"
        cur = self.conn.cursor()
        try:
            cur.execute(query, (drive_id,))
            dirs = cur.fetchall()
        except sqlite3.Error as e:
            QMessageBox.critical(self, "DB Fehler", f"Fehler beim Laden der Directories: {e}")
            return
        self.dir2_cb.clear()
        for d in dirs:
            self.dir2_cb.addItem(d[1], d[0])
    
    def loadExtensions(self):
        """Lädt distinct Dateiendungen aus der DB und füllt die ComboBox.
           Es wird aus den file_path-Werten die Endung ermittelt (alles ab dem ersten '.')."""
        query = """
          SELECT DISTINCT lower(substr(file_path, instr(file_path, '.'))) as ext
          FROM files
          WHERE file_path LIKE '%.%'
          ORDER BY ext
        """
        cur = self.conn.cursor()
        try:
            cur.execute(query)
            exts = cur.fetchall()
        except sqlite3.Error as e:
            QMessageBox.critical(self, "DB Fehler", f"Fehler beim Laden der Dateiendungen: {e}")
            return
        self.ext_cb.clear()
        # Eine Leerstelle, um alle Endungen zuzulassen
        self.ext_cb.addItem("Alle", "%")
        for ext in exts:
            if ext[0]:  # ext[0] enthält die Endung (z.B. '.mp4')
                self.ext_cb.addItem(ext[0], f"%{ext[0]}")
    
    def compareFiles(self):
        """
        Vergleicht Dateien aus zwei ausgewählten (Laufwerk, Directory)-Kombinationen 
        und filtert zusätzlich nach Dateiendung.
        
        Die Suche erfolgt über folgende Logik:
          - Es werden Dateien aus der Tabelle files herangezogen, denen über einen Join
            aus directories der zugehörige drive_id und path zugeordnet werden.
          - Es werden nur Dateien berücksichtigt, die in einem der beiden gewählten Verzeichnissen liegen.
            Hierzu wird das Directory als Präfix genutzt (directory + '%').
          - Es wird mittels SQL ein CTE (FilesWithBase) verwendet, der den reinen Dateinamen („basename“)
            extrahiert – ohne Pfad –, mithilfe von LENGTH, RTRIM und REPLACE.
          - Anschließend werden Gruppen (basierend auf basename und Größe) ermittelt, die in _beiden_ Sets
            (also von jeweils einem Directory) vorhanden sind (mittels HAVING COUNT(DISTINCT drive_id) = 2).
        """
        drive1 = self.drive1_cb.currentData()
        dir1 = self.dir1_cb.currentText()  # Directory-Pfad als String
        drive2 = self.drive2_cb.currentData()
        dir2 = self.dir2_cb.currentText()  # Directory-Pfad als String
        ext_filter = self.ext_cb.currentData()  # z.B. "%.mp4" oder "%" für alle
        
        # Wir nutzen die ausgewählten Verzeichnispfade als Präfix für die Suche (alle Unterverzeichnisse eingeschlossen)
        pattern1 = dir1.rstrip("\\/") + "%"
        pattern2 = dir2.rstrip("\\/") + "%"
        
        query = r"""
        WITH FilesWithBase AS (
          SELECT 
            TRIM(
              CASE 
                WHEN file_path LIKE '%\\%' THEN substr(file_path, LENGTH(rtrim(file_path, replace(file_path, '\\', ''))) + 1)
                ELSE substr(file_path, LENGTH(rtrim(file_path, replace(file_path, '/', ''))) + 1)
              END
            ) as basename,
            f.size,
            f.file_path,
            d.drive_id,
            d.path as dir_path
          FROM files f
          JOIN directories d ON f.directory_id = d.id
          WHERE ((d.drive_id = ? AND d.path LIKE ?) OR (d.drive_id = ? AND d.path LIKE ?))
            AND lower(f.file_path) LIKE ?
        )
        SELECT basename, size, group_concat(file_path, '; ') as paths, count(DISTINCT drive_id) as drive_count
        FROM FilesWithBase
        GROUP BY basename, size
        HAVING drive_count = 2
        ORDER BY size DESC, basename ASC;
        """
        params = (drive1, pattern1, drive2, pattern2, ext_filter)
        cur = self.conn.cursor()
        try:
            cur.execute(query, params)
            rows = cur.fetchall()
        except sqlite3.Error as e:
            QMessageBox.critical(self, "DB Fehler", f"Fehler beim Vergleichen: {e}")
            return
        
        # Fülle die Tabelle mit Ergebnissen
        self.table.setRowCount(0)
        for row in rows:
            row_position = self.table.rowCount()
            self.table.insertRow(row_position)
            # Die Spalten sind: Basename, Größe, (alle file_path-Strings), (Anzahl gefundener drive_ids)
            self.table.setItem(row_position, 0, QTableWidgetItem(str(row[0])))
            self.table.setItem(row_position, 1, QTableWidgetItem(str(row[1])))
            self.table.setItem(row_position, 2, QTableWidgetItem(str(row[3]) if row[3] else ""))
            self.table.setItem(row_position, 3, QTableWidgetItem(str(row[2])))
        
        if not rows:
            QMessageBox.information(self, "Ergebnis", "Keine übereinstimmenden Dateien gefunden.")
    
    def closeEvent(self, event):
        if self.conn:
            self.conn.close()
        event.accept()

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: {} <db_path>\n".format(sys.argv[0]))
        sys.exit(1)
    db_path = sys.argv[1]
    app = QApplication(sys.argv)
    win = MainWindow(db_path)
    win.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
