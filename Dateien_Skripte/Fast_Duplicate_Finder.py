#!/usr/bin/env python3
"""
Optimierter Duplikat-Finder mit Laufwerk/Verzeichnis-Auswahl
Deutlich schneller als die alte Version durch:
- Indizes
- Einschränkung auf bestimmte Laufwerke/Verzeichnisse
- Optimierte SQL-Abfragen mit CTEs
- Größen-Filter
"""

import sys
import os
import sqlite3
from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import QThread, pyqtSignal
import time

# Import parent directory for models
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class DuplicateScanThread(QThread):
    """Thread für die optimierte Duplikat-Suche"""
    progress = pyqtSignal(str)
    result = pyqtSignal(list)
    
    def __init__(self, db_path, options):
        super().__init__()
        self.db_path = db_path
        self.options = options
        
    def run(self):
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA foreign_keys = ON")
            cursor = conn.cursor()
            
            # Extrahiere Optionen
            selected_drives = self.options.get('drives', [])
            selected_paths = self.options.get('paths', [])
            min_size = self.options.get('min_size', 0)
            max_size = self.options.get('max_size', None)
            search_method = self.options.get('method', 'name_size')  # 'name_size' oder 'hash'
            limit = self.options.get('limit', 1000)
            
            # Baue WHERE-Klauseln
            where_clauses = []
            params = []
            
            # Laufwerk-Filter
            if selected_drives:
                drive_placeholders = ','.join('?' * len(selected_drives))
                where_clauses.append(f"d.drive_id IN ({drive_placeholders})")
                params.extend(selected_drives)
            
            # Pfad-Filter
            if selected_paths:
                path_conditions = []
                for path in selected_paths:
                    path_conditions.append("d.full_path LIKE ?")
                    params.append(f"{path}%")
                if path_conditions:
                    where_clauses.append(f"({' OR '.join(path_conditions)})")
            
            # Größen-Filter
            if min_size > 0:
                where_clauses.append("f.size >= ?")
                params.append(min_size)
            
            if max_size is not None:
                where_clauses.append("f.size <= ?")
                params.append(max_size)
            
            where_sql = ""
            if where_clauses:
                where_sql = "WHERE " + " AND ".join(where_clauses)
            
            self.progress.emit("Suche Duplikate...")
            start_time = time.time()
            
            if search_method == 'hash':
                # Hash-basierte Suche (schnell wenn Hashes vorhanden)
                query = f"""
                WITH duplicate_hashes AS (
                    SELECT f.hash, COUNT(*) as dup_count, SUM(f.size) as total_size
                    FROM files f
                    JOIN directories d ON f.directory_id = d.id
                    {where_sql}
                    {"AND" if where_sql else "WHERE"} f.hash IS NOT NULL AND f.hash != ''
                    GROUP BY f.hash
                    HAVING COUNT(*) > 1
                    ORDER BY total_size DESC
                    LIMIT ?
                )
                SELECT 
                    f.id,
                    d.full_path,
                    f.filename,
                    e.name as extension,
                    f.size,
                    f.hash,
                    f.modified_date,
                    dh.dup_count
                FROM duplicate_hashes dh
                JOIN files f ON f.hash = dh.hash
                JOIN directories d ON f.directory_id = d.id
                LEFT JOIN extensions e ON f.extension_id = e.id
                ORDER BY f.hash, d.full_path
                """
                params.append(limit)
                
            else:  # name_size method (Standard)
                # Name+Größe basierte Suche (funktioniert immer)
                query = f"""
                WITH duplicate_groups AS (
                    SELECT 
                        f.filename,
                        f.extension_id,
                        f.size,
                        COUNT(*) as dup_count,
                        SUM(f.size) as total_size
                    FROM files f
                    JOIN directories d ON f.directory_id = d.id
                    {where_sql}
                    GROUP BY f.filename, f.extension_id, f.size
                    HAVING COUNT(*) > 1
                    ORDER BY total_size DESC
                    LIMIT ?
                )
                SELECT 
                    f.id,
                    d.full_path,
                    f.filename,
                    e.name as extension,
                    f.size,
                    f.hash,
                    f.modified_date,
                    dg.dup_count,
                    f.filename || COALESCE(e.name, '') as group_key
                FROM duplicate_groups dg
                JOIN files f ON f.filename = dg.filename 
                    AND f.extension_id IS NOT DISTINCT FROM dg.extension_id
                    AND f.size = dg.size
                JOIN directories d ON f.directory_id = d.id
                LEFT JOIN extensions e ON f.extension_id = e.id
                {where_sql}
                ORDER BY group_key, f.size DESC, d.full_path
                """
                params_final = params.copy()
                params_final.append(limit)
                params_final.extend(params)  # WHERE-Klausel wird zweimal verwendet
                params = params_final
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            elapsed = time.time() - start_time
            self.progress.emit(f"Gefunden: {len(results)} Duplikate in {elapsed:.2f} Sekunden")
            
            # Gruppiere Ergebnisse
            grouped = {}
            for row in results:
                if search_method == 'hash':
                    key = row[5]  # hash
                else:
                    key = row[8]  # group_key (filename + extension)
                
                if key not in grouped:
                    grouped[key] = []
                
                grouped[key].append({
                    'id': row[0],
                    'path': row[1],
                    'filename': row[2],
                    'extension': row[3],
                    'size': row[4],
                    'hash': row[5],
                    'modified': row[6],
                    'count': row[7]
                })
            
            self.result.emit(list(grouped.values()))
            conn.close()
            
        except Exception as e:
            self.progress.emit(f"FEHLER: {str(e)}")
            self.result.emit([])

class FastDuplicateFinder(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Dateien.db')
        self.scan_thread = None
        self.init_ui()
        self.load_drives()
        
    def init_ui(self):
        self.setWindowTitle("Schneller Duplikat-Finder")
        self.setGeometry(100, 100, 1200, 800)
        
        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
        layout = QtWidgets.QVBoxLayout(central_widget)
        
        # Optionen-Bereich
        options_group = QtWidgets.QGroupBox("Such-Optionen")
        options_layout = QtWidgets.QGridLayout(options_group)
        
        # Laufwerk-Auswahl
        options_layout.addWidget(QtWidgets.QLabel("Laufwerke:"), 0, 0)
        self.drive_list = QtWidgets.QListWidget()
        self.drive_list.setSelectionMode(QtWidgets.QAbstractItemView.MultiSelection)
        self.drive_list.setMaximumHeight(100)
        options_layout.addWidget(self.drive_list, 0, 1)
        
        # Pfad-Filter
        options_layout.addWidget(QtWidgets.QLabel("Pfad-Filter:"), 1, 0)
        self.path_edit = QtWidgets.QLineEdit()
        self.path_edit.setPlaceholderText("z.B. C:/Users oder T:/Projekte (optional)")
        options_layout.addWidget(self.path_edit, 1, 1)
        
        # Größen-Filter
        size_layout = QtWidgets.QHBoxLayout()
        size_layout.addWidget(QtWidgets.QLabel("Min. Größe:"))
        self.min_size_spin = QtWidgets.QSpinBox()
        self.min_size_spin.setRange(0, 999999)
        self.min_size_spin.setValue(1)
        self.min_size_spin.setSuffix(" MB")
        size_layout.addWidget(self.min_size_spin)
        
        size_layout.addWidget(QtWidgets.QLabel("Max. Größe:"))
        self.max_size_spin = QtWidgets.QSpinBox()
        self.max_size_spin.setRange(0, 999999)
        self.max_size_spin.setValue(0)
        self.max_size_spin.setSuffix(" MB (0=unbegrenzt)")
        self.max_size_spin.setSpecialValueText("Unbegrenzt")
        size_layout.addWidget(self.max_size_spin)
        
        options_layout.addWidget(QtWidgets.QLabel("Größen-Filter:"), 2, 0)
        options_layout.addLayout(size_layout, 2, 1)
        
        # Such-Methode
        method_layout = QtWidgets.QHBoxLayout()
        self.method_name_radio = QtWidgets.QRadioButton("Name + Größe (schnell)")
        self.method_name_radio.setChecked(True)
        self.method_hash_radio = QtWidgets.QRadioButton("Hash (genau, wenn vorhanden)")
        method_layout.addWidget(self.method_name_radio)
        method_layout.addWidget(self.method_hash_radio)
        
        options_layout.addWidget(QtWidgets.QLabel("Methode:"), 3, 0)
        options_layout.addLayout(method_layout, 3, 1)
        
        # Limit
        options_layout.addWidget(QtWidgets.QLabel("Max. Gruppen:"), 4, 0)
        self.limit_spin = QtWidgets.QSpinBox()
        self.limit_spin.setRange(10, 10000)
        self.limit_spin.setValue(500)
        self.limit_spin.setToolTip("Begrenzt die Anzahl der Duplikat-Gruppen für bessere Performance")
        options_layout.addWidget(self.limit_spin, 4, 1)
        
        layout.addWidget(options_group)
        
        # Such-Button
        self.search_btn = QtWidgets.QPushButton("Duplikate suchen")
        self.search_btn.clicked.connect(self.start_search)
        self.search_btn.setStyleSheet("QPushButton { font-size: 14px; padding: 8px; background-color: #4CAF50; color: white; }")
        layout.addWidget(self.search_btn)
        
        # Status-Label
        self.status_label = QtWidgets.QLabel("Bereit")
        layout.addWidget(self.status_label)
        
        # Ergebnis-Tabelle
        self.result_tree = QtWidgets.QTreeWidget()
        self.result_tree.setHeaderLabels(["Dateiname", "Pfad", "Größe", "Änderungsdatum", "Anzahl"])
        self.result_tree.setAlternatingRowColors(True)
        self.result_tree.setSortingEnabled(True)
        layout.addWidget(self.result_tree)
        
        # Kontext-Menü
        self.result_tree.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.result_tree.customContextMenuRequested.connect(self.show_context_menu)
        
    def load_drives(self):
        """Lädt verfügbare Laufwerke aus der Datenbank"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT d.id, d.name, COUNT(f.id) as file_count
                FROM drives d
                LEFT JOIN directories dir ON dir.drive_id = d.id
                LEFT JOIN files f ON f.directory_id = dir.id
                GROUP BY d.id
                ORDER BY d.name
            """)
            
            for drive_id, drive_name, file_count in cursor.fetchall():
                item = QtWidgets.QListWidgetItem(f"{drive_name} ({file_count:,} Dateien)")
                item.setData(QtCore.Qt.UserRole, drive_id)
                self.drive_list.addItem(item)
                
                # Standardmäßig alle auswählen
                item.setSelected(True)
            
            conn.close()
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Fehler", f"Konnte Laufwerke nicht laden: {e}")
    
    def start_search(self):
        """Startet die Duplikat-Suche"""
        if self.scan_thread and self.scan_thread.isRunning():
            QtWidgets.QMessageBox.information(self, "Info", "Suche läuft bereits...")
            return
        
        # Sammle Optionen
        selected_drives = []
        for i in range(self.drive_list.count()):
            item = self.drive_list.item(i)
            if item.isSelected():
                selected_drives.append(item.data(QtCore.Qt.UserRole))
        
        if not selected_drives:
            QtWidgets.QMessageBox.warning(self, "Warnung", "Bitte mindestens ein Laufwerk auswählen!")
            return
        
        selected_paths = []
        if self.path_edit.text().strip():
            selected_paths = [p.strip() for p in self.path_edit.text().split(',')]
        
        options = {
            'drives': selected_drives,
            'paths': selected_paths,
            'min_size': self.min_size_spin.value() * 1024 * 1024,  # MB zu Bytes
            'max_size': self.max_size_spin.value() * 1024 * 1024 if self.max_size_spin.value() > 0 else None,
            'method': 'hash' if self.method_hash_radio.isChecked() else 'name_size',
            'limit': self.limit_spin.value()
        }
        
        # UI vorbereiten
        self.result_tree.clear()
        self.search_btn.setEnabled(False)
        self.status_label.setText("Suche läuft...")
        
        # Thread starten
        self.scan_thread = DuplicateScanThread(self.db_path, options)
        self.scan_thread.progress.connect(self.update_status)
        self.scan_thread.result.connect(self.show_results)
        self.scan_thread.start()
    
    def update_status(self, message):
        """Aktualisiert die Status-Anzeige"""
        self.status_label.setText(message)
    
    def show_results(self, groups):
        """Zeigt die gefundenen Duplikate an"""
        self.search_btn.setEnabled(True)
        
        if not groups:
            self.status_label.setText("Keine Duplikate gefunden")
            return
        
        total_files = sum(len(group) for group in groups)
        self.status_label.setText(f"Gefunden: {len(groups)} Duplikat-Gruppen mit {total_files} Dateien")
        
        # Fülle Baum
        for group in groups:
            if not group:
                continue
            
            # Erstelle Gruppen-Item
            first = group[0]
            group_item = QtWidgets.QTreeWidgetItem(self.result_tree)
            group_item.setText(0, f"{first['filename']}{first['extension'] or ''}")
            group_item.setText(2, self.format_size(first['size']))
            group_item.setText(4, str(first['count']))
            group_item.setBackground(0, QtGui.QColor(255, 255, 200))
            
            # Füge einzelne Dateien hinzu
            for file in group:
                file_item = QtWidgets.QTreeWidgetItem(group_item)
                file_item.setText(0, f"{file['filename']}{file['extension'] or ''}")
                file_item.setText(1, file['path'])
                file_item.setText(2, self.format_size(file['size']))
                file_item.setText(3, file['modified'] or "")
                file_item.setData(0, QtCore.Qt.UserRole, file)
        
        # Expandiere erste Gruppen
        for i in range(min(5, self.result_tree.topLevelItemCount())):
            self.result_tree.topLevelItem(i).setExpanded(True)
    
    def format_size(self, size):
        """Formatiert Dateigröße"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"
    
    def show_context_menu(self, position):
        """Zeigt Kontextmenü für ausgewählte Datei"""
        item = self.result_tree.itemAt(position)
        if not item or not item.parent():  # Nur für Datei-Items, nicht Gruppen
            return
        
        file_data = item.data(0, QtCore.Qt.UserRole)
        if not file_data:
            return
        
        menu = QtWidgets.QMenu(self)
        
        open_action = menu.addAction("Datei öffnen")
        open_folder_action = menu.addAction("Ordner öffnen")
        menu.addSeparator()
        delete_action = menu.addAction("Datei löschen")
        delete_action.setStyleSheet("color: red;")
        
        action = menu.exec_(self.result_tree.mapToGlobal(position))
        
        if action == open_action:
            full_path = os.path.join(file_data['path'], f"{file_data['filename']}{file_data['extension'] or ''}")
            os.startfile(full_path)
        elif action == open_folder_action:
            os.startfile(file_data['path'])
        elif action == delete_action:
            reply = QtWidgets.QMessageBox.question(
                self, 'Bestätigung', 
                f"Wirklich löschen?\n{file_data['filename']}{file_data['extension'] or ''}",
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No
            )
            if reply == QtWidgets.QMessageBox.Yes:
                full_path = os.path.join(file_data['path'], f"{file_data['filename']}{file_data['extension'] or ''}")
                try:
                    os.remove(full_path)
                    item.parent().removeChild(item)
                    self.status_label.setText("Datei gelöscht")
                except Exception as e:
                    QtWidgets.QMessageBox.warning(self, "Fehler", f"Konnte nicht löschen: {e}")

def main():
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle('Fusion')
    window = FastDuplicateFinder()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()