#!/usr/bin/env python3
"""
Optimierter Ordner-Duplikat-Finder
Nutzt SQL-Abfragen um Ordner mit gemeinsamen Dateien zu finden
VIEL schneller als paarweiser Vergleich!
"""

import sys
import os
import sqlite3
from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import QThread, pyqtSignal
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class FolderAnalysisThread(QThread):
    """Thread für optimierte Ordner-Duplikat-Suche"""
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
            
            min_common_files = self.options.get('min_common_files', 3)
            min_folder_size = self.options.get('min_folder_size', 5)
            selected_drives = self.options.get('drives', [])
            match_criteria = self.options.get('match_criteria', 'name_size')  # 'name', 'size', 'name_size'
            exclude_patterns = self.options.get('exclude_patterns', [])
            
            self.progress.emit("Analysiere Ordner-Strukturen mit SQL...")
            start_time = time.time()
            
            # Baue WHERE-Klausel für Laufwerk-Filter
            drive_filter = ""
            params = []
            if selected_drives:
                placeholders = ','.join('?' * len(selected_drives))
                drive_filter = f"AND d.drive_id IN ({placeholders})"
                params.extend(selected_drives)
            
            # Baue Exclude-Pattern Filter
            exclude_filter = ""
            for pattern in exclude_patterns:
                exclude_filter += " AND d.full_path NOT LIKE ?"
                params.append(f"%{pattern}%")
            
            # Schritt 1: Finde Ordner-Paare mit gemeinsamen Dateien
            # Diese Abfrage ist der Schlüssel zur Performance!
            
            if match_criteria == 'name_size':
                # Match nach Name UND Größe (genaueste Methode)
                query = f"""
                WITH folder_files AS (
                    -- Hole alle Dateien mit ihren Ordnern
                    SELECT 
                        d.id as folder_id,
                        d.full_path,
                        dr.name as drive,
                        f.filename || COALESCE(e.name, '') as file_key,
                        f.size,
                        f.filename || '_' || f.size as match_key
                    FROM directories d
                    JOIN drives dr ON d.drive_id = dr.id
                    JOIN files f ON f.directory_id = d.id
                    LEFT JOIN extensions e ON f.extension_id = e.id
                    WHERE 1=1
                    {drive_filter}
                    {exclude_filter}
                ),
                shared_files AS (
                    -- Finde Dateien die in mehreren Ordnern vorkommen
                    SELECT 
                        f1.folder_id as folder1_id,
                        f1.full_path as folder1_path,
                        f1.drive as folder1_drive,
                        f2.folder_id as folder2_id,
                        f2.full_path as folder2_path,
                        f2.drive as folder2_drive,
                        COUNT(*) as common_files
                    FROM folder_files f1
                    JOIN folder_files f2 ON f1.match_key = f2.match_key
                    WHERE f1.folder_id < f2.folder_id  -- Verhindere Duplikate und Selbst-Vergleich
                    GROUP BY f1.folder_id, f2.folder_id
                    HAVING COUNT(*) >= ?
                ),
                folder_stats AS (
                    -- Hole Statistiken für jeden Ordner
                    SELECT 
                        d.id,
                        d.full_path,
                        COUNT(f.id) as total_files,
                        SUM(f.size) as total_size
                    FROM directories d
                    JOIN files f ON f.directory_id = d.id
                    WHERE d.id IN (
                        SELECT folder1_id FROM shared_files
                        UNION
                        SELECT folder2_id FROM shared_files
                    )
                    GROUP BY d.id
                    HAVING total_files >= ?
                )
                SELECT 
                    sf.folder1_id,
                    sf.folder1_path,
                    sf.folder1_drive,
                    fs1.total_files as folder1_files,
                    fs1.total_size as folder1_size,
                    sf.folder2_id,
                    sf.folder2_path,
                    sf.folder2_drive,
                    fs2.total_files as folder2_files,
                    fs2.total_size as folder2_size,
                    sf.common_files,
                    CAST(sf.common_files AS FLOAT) * 100 / 
                        CASE 
                            WHEN fs1.total_files < fs2.total_files THEN fs1.total_files
                            ELSE fs2.total_files
                        END as similarity_percent
                FROM shared_files sf
                JOIN folder_stats fs1 ON sf.folder1_id = fs1.id
                JOIN folder_stats fs2 ON sf.folder2_id = fs2.id
                ORDER BY sf.common_files DESC, similarity_percent DESC
                LIMIT 500
                """
                params.append(min_common_files)
                params.append(min_folder_size)
                
            elif match_criteria == 'name':
                # Match nur nach Dateiname
                query = f"""
                WITH folder_files AS (
                    SELECT 
                        d.id as folder_id,
                        d.full_path,
                        dr.name as drive,
                        f.filename || COALESCE(e.name, '') as match_key
                    FROM directories d
                    JOIN drives dr ON d.drive_id = dr.id
                    JOIN files f ON f.directory_id = d.id
                    LEFT JOIN extensions e ON f.extension_id = e.id
                    WHERE 1=1
                    {drive_filter}
                    {exclude_filter}
                ),
                shared_files AS (
                    SELECT 
                        f1.folder_id as folder1_id,
                        f1.full_path as folder1_path,
                        f1.drive as folder1_drive,
                        f2.folder_id as folder2_id,
                        f2.full_path as folder2_path,
                        f2.drive as folder2_drive,
                        COUNT(*) as common_files
                    FROM folder_files f1
                    JOIN folder_files f2 ON f1.match_key = f2.match_key
                    WHERE f1.folder_id < f2.folder_id
                    GROUP BY f1.folder_id, f2.folder_id
                    HAVING COUNT(*) >= ?
                )
                SELECT 
                    folder1_id, folder1_path, folder1_drive,
                    0 as folder1_files, 0 as folder1_size,
                    folder2_id, folder2_path, folder2_drive,
                    0 as folder2_files, 0 as folder2_size,
                    common_files, 0 as similarity_percent
                FROM shared_files
                ORDER BY common_files DESC
                LIMIT 500
                """
                params.append(min_common_files)
                
            else:  # size only
                # Match nur nach Größe
                query = f"""
                WITH folder_files AS (
                    SELECT 
                        d.id as folder_id,
                        d.full_path,
                        dr.name as drive,
                        f.size as match_key
                    FROM directories d
                    JOIN drives dr ON d.drive_id = dr.id
                    JOIN files f ON f.directory_id = d.id
                    WHERE f.size > 0
                    {drive_filter}
                    {exclude_filter}
                ),
                shared_files AS (
                    SELECT 
                        f1.folder_id as folder1_id,
                        f1.full_path as folder1_path,
                        f1.drive as folder1_drive,
                        f2.folder_id as folder2_id,
                        f2.full_path as folder2_path,
                        f2.drive as folder2_drive,
                        COUNT(*) as common_files
                    FROM folder_files f1
                    JOIN folder_files f2 ON f1.match_key = f2.match_key
                    WHERE f1.folder_id < f2.folder_id
                    GROUP BY f1.folder_id, f2.folder_id
                    HAVING COUNT(*) >= ?
                )
                SELECT 
                    folder1_id, folder1_path, folder1_drive,
                    0 as folder1_files, 0 as folder1_size,
                    folder2_id, folder2_path, folder2_drive,
                    0 as folder2_files, 0 as folder2_size,
                    common_files, 0 as similarity_percent
                FROM shared_files
                ORDER BY common_files DESC
                LIMIT 500
                """
                params.append(min_common_files)
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            elapsed = time.time() - start_time
            self.progress.emit(f"SQL-Analyse in {elapsed:.2f} Sekunden abgeschlossen")
            
            # Gruppiere Ergebnisse
            folder_groups = []
            processed = set()
            
            for row in results:
                folder1_id = row[0]
                folder2_id = row[5]
                
                # Skip wenn schon verarbeitet
                if folder1_id in processed and folder2_id in processed:
                    continue
                
                # Erstelle Ordner-Paar
                group = [
                    {
                        'id': folder1_id,
                        'path': row[1],
                        'drive': row[2],
                        'total_files': row[3],
                        'total_size': row[4],
                        'common_files': row[10],
                        'similarity': row[11] if row[11] else 0
                    },
                    {
                        'id': folder2_id,
                        'path': row[6],
                        'drive': row[7],
                        'total_files': row[8],
                        'total_size': row[9],
                        'common_files': row[10],
                        'similarity': row[11] if row[11] else 0
                    }
                ]
                
                folder_groups.append(group)
                processed.add(folder1_id)
                processed.add(folder2_id)
            
            self.progress.emit(f"Gefunden: {len(folder_groups)} Ordner-Paare mit gemeinsamen Dateien")
            self.result.emit(folder_groups)
            conn.close()
            
        except Exception as e:
            self.progress.emit(f"FEHLER: {str(e)}")
            import traceback
            print(traceback.format_exc())
            self.result.emit([])

class FastFolderDuplicateFinder(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Dateien.db')
        self.scan_thread = None
        self.init_ui()
        self.load_drives()
        
    def init_ui(self):
        self.setWindowTitle("Schneller Ordner-Duplikat-Finder (SQL-optimiert)")
        self.setGeometry(100, 100, 1300, 800)
        _icon = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'icons', 'duplicate_fast_folder.ico')
        if os.path.exists(_icon):
            self.setWindowIcon(QtGui.QIcon(_icon))
        
        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
        layout = QtWidgets.QVBoxLayout(central_widget)
        
        # Optionen
        options_group = QtWidgets.QGroupBox("Such-Optionen")
        options_layout = QtWidgets.QGridLayout(options_group)
        
        # Laufwerk-Auswahl
        options_layout.addWidget(QtWidgets.QLabel("Laufwerke:"), 0, 0)
        self.drive_list = QtWidgets.QListWidget()
        self.drive_list.setSelectionMode(QtWidgets.QAbstractItemView.MultiSelection)
        self.drive_list.setMaximumHeight(100)
        options_layout.addWidget(self.drive_list, 0, 1)
        
        # Match-Kriterien
        options_layout.addWidget(QtWidgets.QLabel("Vergleichskriterium:"), 1, 0)
        criteria_layout = QtWidgets.QHBoxLayout()
        self.match_name_size = QtWidgets.QRadioButton("Name + Größe (genau)")
        self.match_name_size.setChecked(True)
        self.match_name = QtWidgets.QRadioButton("Nur Name")
        self.match_size = QtWidgets.QRadioButton("Nur Größe")
        criteria_layout.addWidget(self.match_name_size)
        criteria_layout.addWidget(self.match_name)
        criteria_layout.addWidget(self.match_size)
        criteria_layout.addStretch()
        options_layout.addLayout(criteria_layout, 1, 1)
        
        # Parameter
        param_layout = QtWidgets.QHBoxLayout()
        
        param_layout.addWidget(QtWidgets.QLabel("Min. gemeinsame Dateien:"))
        self.min_common_spin = QtWidgets.QSpinBox()
        self.min_common_spin.setRange(1, 1000)
        self.min_common_spin.setValue(5)
        self.min_common_spin.setToolTip("Mindestanzahl gemeinsamer Dateien zwischen Ordnern")
        param_layout.addWidget(self.min_common_spin)
        
        param_layout.addWidget(QtWidgets.QLabel("Min. Ordnergröße:"))
        self.min_folder_size_spin = QtWidgets.QSpinBox()
        self.min_folder_size_spin.setRange(1, 10000)
        self.min_folder_size_spin.setValue(10)
        self.min_folder_size_spin.setSuffix(" Dateien")
        self.min_folder_size_spin.setToolTip("Ignoriere Ordner mit weniger Dateien")
        param_layout.addWidget(self.min_folder_size_spin)
        
        param_layout.addStretch()
        options_layout.addWidget(QtWidgets.QLabel("Parameter:"), 2, 0)
        options_layout.addLayout(param_layout, 2, 1)
        
        # Ausschluss-Muster
        options_layout.addWidget(QtWidgets.QLabel("Ausschließen:"), 3, 0)
        self.exclude_edit = QtWidgets.QLineEdit()
        self.exclude_edit.setPlaceholderText("z.B. backup, temp, cache, node_modules (Komma-getrennt)")
        self.exclude_edit.setText("node_modules, .git, cache, temp, backup")
        options_layout.addWidget(self.exclude_edit, 3, 1)
        
        layout.addWidget(options_group)
        
        # Info-Box
        info_label = QtWidgets.QLabel(
            "⚡ Diese Version nutzt optimierte SQL-Abfragen mit CTEs (Common Table Expressions)\n"
            "statt paarweiser Vergleiche. Dadurch ist sie 100-1000x schneller!"
        )
        info_label.setStyleSheet("""
            QLabel {
                background-color: #e3f2fd;
                padding: 10px;
                border: 1px solid #2196F3;
                border-radius: 5px;
                color: #1565C0;
            }
        """)
        layout.addWidget(info_label)
        
        # Such-Button
        self.search_btn = QtWidgets.QPushButton("🚀 Ordner-Duplikate finden (SQL-optimiert)")
        self.search_btn.clicked.connect(self.start_search)
        self.search_btn.setStyleSheet("""
            QPushButton {
                font-size: 14px;
                padding: 12px;
                background-color: #2196F3;
                color: white;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #1976D2;
            }
        """)
        layout.addWidget(self.search_btn)
        
        # Status
        self.status_label = QtWidgets.QLabel("Bereit")
        layout.addWidget(self.status_label)
        
        # Ergebnis-Tabelle
        self.result_tree = QtWidgets.QTreeWidget()
        self.result_tree.setHeaderLabels(["Ordner", "Laufwerk", "Dateien", "Größe", "Gemeinsame Dateien", "Ähnlichkeit"])
        self.result_tree.setAlternatingRowColors(True)
        self.result_tree.setSortingEnabled(True)
        layout.addWidget(self.result_tree)
        
        # Kontext-Menü
        self.result_tree.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.result_tree.customContextMenuRequested.connect(self.show_context_menu)
        
    def load_drives(self):
        """Lädt verfügbare Laufwerke"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT d.id, d.name, COUNT(DISTINCT dir.id) as folder_count, COUNT(f.id) as file_count
                FROM drives d
                LEFT JOIN directories dir ON dir.drive_id = d.id
                LEFT JOIN files f ON f.directory_id = dir.id
                GROUP BY d.id
                ORDER BY d.name
            """)
            
            for drive_id, drive_name, folder_count, file_count in cursor.fetchall():
                item = QtWidgets.QListWidgetItem(
                    f"{drive_name} ({folder_count:,} Ordner, {file_count:,} Dateien)"
                )
                item.setData(QtCore.Qt.UserRole, drive_id)
                self.drive_list.addItem(item)
                # Standardmäßig alle auswählen
                item.setSelected(True)
            
            conn.close()
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Fehler", f"Konnte Laufwerke nicht laden: {e}")
    
    def start_search(self):
        """Startet die Suche"""
        if self.scan_thread and self.scan_thread.isRunning():
            QtWidgets.QMessageBox.information(self, "Info", "Suche läuft bereits...")
            return
        
        # Sammle ausgewählte Laufwerke
        selected_drives = []
        for i in range(self.drive_list.count()):
            item = self.drive_list.item(i)
            if item.isSelected():
                selected_drives.append(item.data(QtCore.Qt.UserRole))
        
        if not selected_drives:
            QtWidgets.QMessageBox.warning(self, "Warnung", "Bitte mindestens ein Laufwerk auswählen!")
            return
        
        # Match-Kriterium
        if self.match_name_size.isChecked():
            match_criteria = 'name_size'
        elif self.match_name.isChecked():
            match_criteria = 'name'
        else:
            match_criteria = 'size'
        
        # Ausschluss-Muster
        exclude_patterns = [p.strip() for p in self.exclude_edit.text().split(',') if p.strip()]
        
        options = {
            'drives': selected_drives,
            'min_common_files': self.min_common_spin.value(),
            'min_folder_size': self.min_folder_size_spin.value(),
            'match_criteria': match_criteria,
            'exclude_patterns': exclude_patterns
        }
        
        # UI vorbereiten
        self.result_tree.clear()
        self.search_btn.setEnabled(False)
        self.status_label.setText("Führe SQL-Analyse aus...")
        
        # Thread starten
        self.scan_thread = FolderAnalysisThread(self.db_path, options)
        self.scan_thread.progress.connect(self.update_status)
        self.scan_thread.result.connect(self.show_results)
        self.scan_thread.start()
    
    def update_status(self, message):
        """Aktualisiert Status"""
        self.status_label.setText(message)
    
    def show_results(self, folder_groups):
        """Zeigt Ergebnisse an"""
        self.search_btn.setEnabled(True)
        
        if not folder_groups:
            self.status_label.setText("Keine Ordner-Duplikate gefunden")
            return
        
        total_common = 0
        total_size = 0
        
        for group in folder_groups:
            if len(group) < 2:
                continue
            
            # Erstelle Gruppen-Item
            group_item = QtWidgets.QTreeWidgetItem(self.result_tree)
            common_files = group[0]['common_files']
            similarity = group[0]['similarity']
            
            group_item.setText(0, f"Ordner-Paar ({common_files} gemeinsame Dateien)")
            group_item.setText(4, str(common_files))
            group_item.setText(5, f"{similarity:.1f}%" if similarity > 0 else "N/A")
            
            # Färbe Gruppe basierend auf Ähnlichkeit
            if similarity > 80:
                color = QtGui.QColor(255, 200, 200)  # Rot = sehr ähnlich
            elif similarity > 50:
                color = QtGui.QColor(255, 230, 200)  # Orange = ähnlich
            else:
                color = QtGui.QColor(255, 255, 200)  # Gelb = einige gemeinsame
            
            for col in range(6):
                group_item.setBackground(col, color)
            
            total_common += common_files
            
            # Füge Ordner hinzu
            for folder in group:
                folder_item = QtWidgets.QTreeWidgetItem(group_item)
                folder_item.setText(0, folder['path'])
                folder_item.setText(1, folder['drive'])
                folder_item.setText(2, str(folder['total_files']) if folder['total_files'] else "?")
                folder_item.setText(3, self.format_size(folder['total_size']) if folder['total_size'] else "?")
                folder_item.setText(4, str(folder['common_files']))
                folder_item.setText(5, f"{folder['similarity']:.1f}%" if folder['similarity'] > 0 else "")
                
                folder_item.setData(0, QtCore.Qt.UserRole, folder)
                total_size += folder['total_size'] if folder['total_size'] else 0
            
            group_item.setExpanded(True)
        
        self.status_label.setText(
            f"Gefunden: {len(folder_groups)} Ordner-Paare | "
            f"Gesamt: {total_common} gemeinsame Dateien | "
            f"Größe: {self.format_size(total_size)}"
        )
    
    def format_size(self, size):
        """Formatiert Größe"""
        if size is None:
            return "?"
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"
    
    def show_context_menu(self, position):
        """Zeigt Kontextmenü"""
        item = self.result_tree.itemAt(position)
        if not item or not item.parent():
            return
        
        folder_data = item.data(0, QtCore.Qt.UserRole)
        if not folder_data:
            return
        
        menu = QtWidgets.QMenu(self)
        
        open_action = menu.addAction("📁 Ordner öffnen")
        menu.addSeparator()
        compare_action = menu.addAction("🔍 Beide Ordner zum Vergleich öffnen")
        menu.addSeparator()
        details_action = menu.addAction("📊 Gemeinsame Dateien anzeigen")
        
        action = menu.exec_(self.result_tree.mapToGlobal(position))
        
        if action == open_action:
            os.startfile(folder_data['path'])
        elif action == compare_action:
            # Öffne beide Ordner
            parent = item.parent()
            for i in range(parent.childCount()):
                child = parent.child(i)
                child_data = child.data(0, QtCore.Qt.UserRole)
                if child_data:
                    os.startfile(child_data['path'])
        elif action == details_action:
            self.show_common_files(folder_data, item.parent())
    
    def show_common_files(self, folder_data, group_item):
        """Zeigt die gemeinsamen Dateien in einem Dialog"""
        # Hole beide Ordner-IDs
        folder_ids = []
        for i in range(group_item.childCount()):
            child = group_item.child(i)
            child_data = child.data(0, QtCore.Qt.UserRole)
            if child_data:
                folder_ids.append(child_data['id'])
        
        if len(folder_ids) < 2:
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Hole gemeinsame Dateien
            cursor.execute("""
                SELECT 
                    f1.filename || COALESCE(e1.name, '') as filename,
                    f1.size
                FROM files f1
                LEFT JOIN extensions e1 ON f1.extension_id = e1.id
                WHERE f1.directory_id = ?
                AND EXISTS (
                    SELECT 1 FROM files f2
                    LEFT JOIN extensions e2 ON f2.extension_id = e2.id
                    WHERE f2.directory_id = ?
                    AND f2.filename = f1.filename
                    AND f2.size = f1.size
                    AND COALESCE(e2.name, '') = COALESCE(e1.name, '')
                )
                ORDER BY f1.size DESC
            """, (folder_ids[0], folder_ids[1]))
            
            files = cursor.fetchall()
            conn.close()
            
            # Zeige Dialog
            dialog = QtWidgets.QDialog(self)
            dialog.setWindowTitle("Gemeinsame Dateien")
            dialog.resize(600, 400)
            layout = QtWidgets.QVBoxLayout(dialog)
            
            text = QtWidgets.QTextEdit()
            text.setReadOnly(True)
            
            content = f"Gemeinsame Dateien zwischen:\n"
            content += f"Ordner 1: {group_item.child(0).text(0)}\n"
            content += f"Ordner 2: {group_item.child(1).text(0)}\n"
            content += f"\n{len(files)} gemeinsame Dateien:\n"
            content += "=" * 50 + "\n"
            
            for filename, size in files:
                content += f"{filename:50} {self.format_size(size):>15}\n"
            
            text.setPlainText(content)
            layout.addWidget(text)
            
            close_btn = QtWidgets.QPushButton("Schließen")
            close_btn.clicked.connect(dialog.close)
            layout.addWidget(close_btn)
            
            dialog.exec_()
            
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Fehler", f"Konnte gemeinsame Dateien nicht laden: {e}")

def main():
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle('Fusion')
    window = FastFolderDuplicateFinder()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()