#!/usr/bin/env python3
"""
Erweiterter Duplikat-Manager mit:
- Multi-Laufwerk und Multi-Pfad Auswahl
- Backup-Pfade ausschließen
- Duplikat-Ordner finden (nicht nur einzelne Dateien)
- Intelligente Lösch-Vorschläge (niemals Backups löschen)
"""

import sys
import os
import sqlite3
from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import QThread, pyqtSignal
import time
from collections import defaultdict
import re

# Import parent directory for models
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class DuplicateScanThread(QThread):
    """Thread für erweiterte Duplikat-Suche"""
    progress = pyqtSignal(str)
    result_files = pyqtSignal(list)
    result_folders = pyqtSignal(list)
    
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
            include_drives = self.options.get('include_drives', [])
            exclude_drives = self.options.get('exclude_drives', [])
            include_paths = self.options.get('include_paths', [])
            exclude_paths = self.options.get('exclude_paths', [])
            backup_patterns = self.options.get('backup_patterns', [])
            min_size = self.options.get('min_size', 0)
            max_size = self.options.get('max_size', None)
            search_type = self.options.get('search_type', 'files')  # 'files' oder 'folders'
            folder_similarity = self.options.get('folder_similarity', 70)  # % Ähnlichkeit für Ordner
            
            if search_type == 'folders':
                self.find_duplicate_folders(cursor, include_drives, exclude_drives, 
                                           include_paths, exclude_paths, backup_patterns,
                                           folder_similarity)
            else:
                self.find_duplicate_files(cursor, include_drives, exclude_drives,
                                         include_paths, exclude_paths, backup_patterns,
                                         min_size, max_size)
            
            conn.close()
            
        except Exception as e:
            self.progress.emit(f"FEHLER: {str(e)}")
            import traceback
            print(traceback.format_exc())
    
    def build_where_clause(self, include_drives, exclude_drives, include_paths, exclude_paths):
        """Baut WHERE-Klausel für Laufwerk und Pfad-Filter"""
        where_clauses = []
        params = []
        
        # Include Laufwerke
        if include_drives:
            placeholders = ','.join('?' * len(include_drives))
            where_clauses.append(f"d.drive_id IN ({placeholders})")
            params.extend(include_drives)
        
        # Exclude Laufwerke
        if exclude_drives:
            placeholders = ','.join('?' * len(exclude_drives))
            where_clauses.append(f"d.drive_id NOT IN ({placeholders})")
            params.extend(exclude_drives)
        
        # Include Pfade
        if include_paths:
            path_conditions = []
            for path in include_paths:
                path_conditions.append("d.full_path LIKE ?")
                params.append(f"{path}%")
            if path_conditions:
                where_clauses.append(f"({' OR '.join(path_conditions)})")
        
        # Exclude Pfade
        if exclude_paths:
            for path in exclude_paths:
                where_clauses.append("d.full_path NOT LIKE ?")
                params.append(f"{path}%")
        
        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)
        
        return where_sql, params
    
    def is_backup_path(self, path, backup_patterns):
        """Prüft ob ein Pfad ein Backup-Pfad ist"""
        path_lower = path.lower()
        for pattern in backup_patterns:
            if pattern.lower() in path_lower:
                return True
        return False
    
    def find_duplicate_files(self, cursor, include_drives, exclude_drives,
                            include_paths, exclude_paths, backup_patterns,
                            min_size, max_size):
        """Findet doppelte Dateien"""

        self.progress.emit("Suche doppelte Dateien...")

        where_clauses, params = self.build_where_clause(include_drives, exclude_drives,
                                                        include_paths, exclude_paths)

        # Groessen-Filter hinzufuegen
        extra_clauses = []
        extra_params = []
        if min_size > 0:
            extra_clauses.append("f.size >= ?")
            extra_params.append(min_size)
        if max_size is not None:
            extra_clauses.append("f.size <= ?")
            extra_params.append(max_size)

        # WHERE zusammenbauen (sauber ohne rstrip)
        all_clauses = []
        if where_clauses:
            all_clauses.append(where_clauses.replace("WHERE ", "", 1))
        all_clauses.extend(extra_clauses)

        where_sql = ("WHERE " + " AND ".join(all_clauses)) if all_clauses else ""
        all_params = params + extra_params

        # Hauptabfrage mit CTEs — WHERE in CTE UND aeusserem Query
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
            LIMIT 1000
        )
        SELECT
            f.id,
            d.full_path,
            f.filename,
            CASE WHEN e.name IS NULL OR e.name = '[none]' THEN '' ELSE e.name END as extension,
            f.size,
            f.hash,
            f.modified_date,
            dg.dup_count,
            dg.total_size,
            dr.name as drive_name
        FROM duplicate_groups dg
        JOIN files f ON f.filename = dg.filename
            AND f.extension_id IS dg.extension_id
            AND f.size = dg.size
        JOIN directories d ON f.directory_id = d.id
        JOIN drives dr ON d.drive_id = dr.id
        LEFT JOIN extensions e ON f.extension_id = e.id
        {where_sql}
        ORDER BY dg.total_size DESC, f.filename, d.full_path
        """

        # Params fuer CTE + LIMIT ist implizit + Params fuer aeusseres WHERE
        final_params = all_params + all_params
        cursor.execute(query, final_params)
        results = cursor.fetchall()

        # Gruppiere und markiere Backup-Status
        grouped = defaultdict(list)
        for row in results:
            ext = row[3] or ''
            key = f"{row[2]}{ext}_{row[4]}"  # filename + extension + size

            file_info = {
                'id': row[0],
                'path': row[1],
                'filename': row[2],
                'extension': row[3],
                'size': row[4],
                'hash': row[5],
                'modified': row[6],
                'count': row[7],
                'total_size': row[8],
                'drive': row[9],
                'is_backup': self.is_backup_path(row[1], backup_patterns),
                'full_path': os.path.join(row[1], f"{row[2]}{ext}")
            }
            grouped[key].append(file_info)
        
        # Sortiere Gruppen nach Einsparpotential
        sorted_groups = sorted(grouped.values(), 
                              key=lambda g: g[0]['total_size'], 
                              reverse=True)
        
        self.progress.emit(f"Gefunden: {len(sorted_groups)} Duplikat-Gruppen")
        self.result_files.emit(sorted_groups)
    
    def find_duplicate_folders(self, cursor, include_drives, exclude_drives,
                              include_paths, exclude_paths, backup_patterns,
                              similarity_threshold):
        """Findet ähnliche/doppelte Ordner"""
        
        self.progress.emit("Analysiere Ordner-Strukturen...")
        
        where_sql, params = self.build_where_clause(include_drives, exclude_drives,
                                                   include_paths, exclude_paths)
        
        # Hole alle Ordner mit ihren Dateien
        query = f"""
        SELECT 
            d.id,
            d.full_path,
            dr.name as drive,
            COUNT(f.id) as file_count,
            SUM(f.size) as total_size,
            GROUP_CONCAT(f.filename || CASE WHEN e.name IS NULL OR e.name = '[none]' THEN '' ELSE e.name END || '_' || COALESCE(f.size, 0)) as file_signatures
        FROM directories d
        JOIN drives dr ON d.drive_id = dr.id
        LEFT JOIN files f ON f.directory_id = d.id
        LEFT JOIN extensions e ON f.extension_id = e.id
        {where_sql}
        GROUP BY d.id
        HAVING file_count > 5  -- Nur Ordner mit mehr als 5 Dateien
        """
        
        cursor.execute(query, params)
        folders = cursor.fetchall()
        
        self.progress.emit(f"Vergleiche {len(folders)} Ordner...")
        
        # Vergleiche Ordner paarweise
        folder_pairs = []
        processed = set()
        
        for i, folder1 in enumerate(folders):
            if i % 100 == 0:
                self.progress.emit(f"Verarbeite Ordner {i}/{len(folders)}...")
            
            folder1_id = folder1[0]
            if folder1_id in processed:
                continue
            
            folder1_files = set(folder1[5].split(',')) if folder1[5] else set()
            
            similar_folders = []
            
            for folder2 in folders[i+1:]:
                folder2_id = folder2[0]
                if folder2_id in processed:
                    continue
                
                folder2_files = set(folder2[5].split(',')) if folder2[5] else set()
                
                # Berechne Ähnlichkeit
                if folder1_files and folder2_files:
                    intersection = folder1_files & folder2_files
                    union = folder1_files | folder2_files
                    similarity = (len(intersection) / len(union)) * 100
                    
                    if similarity >= similarity_threshold:
                        similar_folders.append({
                            'id': folder2[0],
                            'path': folder2[1],
                            'drive': folder2[2],
                            'file_count': folder2[3],
                            'size': folder2[4],
                            'similarity': similarity,
                            'is_backup': self.is_backup_path(folder2[1], backup_patterns)
                        })
                        processed.add(folder2_id)
            
            if similar_folders:
                # Füge ersten Ordner hinzu
                main_folder = {
                    'id': folder1[0],
                    'path': folder1[1],
                    'drive': folder1[2],
                    'file_count': folder1[3],
                    'size': folder1[4],
                    'similarity': 100,
                    'is_backup': self.is_backup_path(folder1[1], backup_patterns)
                }
                
                folder_group = [main_folder] + similar_folders
                folder_pairs.append(folder_group)
                processed.add(folder1_id)
        
        # Sortiere nach Größe
        folder_pairs.sort(key=lambda g: sum(f['size'] or 0 for f in g), reverse=True)
        
        self.progress.emit(f"Gefunden: {len(folder_pairs)} ähnliche Ordner-Gruppen")
        self.result_folders.emit(folder_pairs[:100])  # Limit auf 100 Gruppen

class AdvancedDuplicateManager(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Dateien.db')
        self.scan_thread = None
        self.init_ui()
        self.load_drives_and_paths()
        
    def init_ui(self):
        self.setWindowTitle("Erweiterter Duplikat-Manager")
        self.setGeometry(100, 100, 1400, 900)
        _icon = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'icons', 'duplicate_advanced.ico')
        if os.path.exists(_icon):
            self.setWindowIcon(QtGui.QIcon(_icon))
        
        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QtWidgets.QVBoxLayout(central_widget)
        
        # Tab Widget für verschiedene Modi
        self.tab_widget = QtWidgets.QTabWidget()
        main_layout.addWidget(self.tab_widget)
        
        # Tab 1: Datei-Duplikate
        self.file_tab = QtWidgets.QWidget()
        self.setup_file_tab()
        self.tab_widget.addTab(self.file_tab, "Datei-Duplikate")
        
        # Tab 2: Ordner-Duplikate
        self.folder_tab = QtWidgets.QWidget()
        self.setup_folder_tab()
        self.tab_widget.addTab(self.folder_tab, "Ordner-Duplikate")
        
        # Status Bar
        self.status_bar = self.statusBar()
        self.status_bar.showMessage("Bereit")
    
    def setup_file_tab(self):
        """Erstellt das UI für Datei-Duplikate"""
        layout = QtWidgets.QVBoxLayout(self.file_tab)
        
        # Filter-Bereich
        filter_group = QtWidgets.QGroupBox("Filter-Einstellungen")
        filter_layout = QtWidgets.QGridLayout(filter_group)
        
        # Laufwerk-Auswahl (Include/Exclude)
        filter_layout.addWidget(QtWidgets.QLabel("Einschließen:"), 0, 0)
        self.include_drives_list = QtWidgets.QListWidget()
        self.include_drives_list.setSelectionMode(QtWidgets.QAbstractItemView.MultiSelection)
        self.include_drives_list.setMaximumHeight(80)
        filter_layout.addWidget(self.include_drives_list, 0, 1)
        
        filter_layout.addWidget(QtWidgets.QLabel("Ausschließen:"), 0, 2)
        self.exclude_drives_list = QtWidgets.QListWidget()
        self.exclude_drives_list.setSelectionMode(QtWidgets.QAbstractItemView.MultiSelection)
        self.exclude_drives_list.setMaximumHeight(80)
        filter_layout.addWidget(self.exclude_drives_list, 0, 3)
        
        # Pfad-Filter
        filter_layout.addWidget(QtWidgets.QLabel("Pfade einschließen:"), 1, 0)
        self.include_paths_edit = QtWidgets.QTextEdit()
        self.include_paths_edit.setMaximumHeight(60)
        self.include_paths_edit.setPlaceholderText("Ein Pfad pro Zeile, z.B.:\nC:/Users\nD:/Projekte")
        filter_layout.addWidget(self.include_paths_edit, 1, 1)
        
        filter_layout.addWidget(QtWidgets.QLabel("Pfade ausschließen:"), 1, 2)
        self.exclude_paths_edit = QtWidgets.QTextEdit()
        self.exclude_paths_edit.setMaximumHeight(60)
        self.exclude_paths_edit.setPlaceholderText("Ein Pfad pro Zeile, z.B.:\nC:/Windows\nD:/Backup")
        filter_layout.addWidget(self.exclude_paths_edit, 1, 3)
        
        # Backup-Pattern
        filter_layout.addWidget(QtWidgets.QLabel("Backup-Muster:"), 2, 0)
        self.backup_patterns_edit = QtWidgets.QLineEdit()
        self.backup_patterns_edit.setPlaceholderText("backup, bak, archiv, kopie (Komma-getrennt)")
        self.backup_patterns_edit.setText("backup, bak, archiv, kopie, old, sicherung")
        filter_layout.addWidget(self.backup_patterns_edit, 2, 1, 1, 3)
        
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
        self.max_size_spin.setSuffix(" MB")
        self.max_size_spin.setSpecialValueText("Unbegrenzt")
        size_layout.addWidget(self.max_size_spin)
        size_layout.addStretch()
        
        filter_layout.addWidget(QtWidgets.QLabel("Größen-Filter:"), 3, 0)
        filter_layout.addLayout(size_layout, 3, 1, 1, 3)
        
        layout.addWidget(filter_group)
        
        # Such-Button
        self.search_files_btn = QtWidgets.QPushButton("Datei-Duplikate suchen")
        self.search_files_btn.clicked.connect(self.search_file_duplicates)
        self.search_files_btn.setStyleSheet("""
            QPushButton {
                font-size: 14px;
                padding: 10px;
                background-color: #4CAF50;
                color: white;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
        """)
        layout.addWidget(self.search_files_btn)
        
        # Ergebnis-Baum
        self.file_tree = QtWidgets.QTreeWidget()
        self.file_tree.setHeaderLabels(["Datei", "Pfad", "Laufwerk", "Größe", "Änderung", "Status"])
        self.file_tree.setAlternatingRowColors(True)
        self.file_tree.setSortingEnabled(True)
        layout.addWidget(self.file_tree)
        
        # Kontext-Menü
        self.file_tree.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.file_tree.customContextMenuRequested.connect(self.show_file_context_menu)
    
    def setup_folder_tab(self):
        """Erstellt das UI für Ordner-Duplikate"""
        layout = QtWidgets.QVBoxLayout(self.folder_tab)
        
        # Filter (vereinfacht für Ordner)
        filter_group = QtWidgets.QGroupBox("Ordner-Such-Einstellungen")
        filter_layout = QtWidgets.QVBoxLayout(filter_group)
        
        # Ähnlichkeits-Schwellwert
        similarity_layout = QtWidgets.QHBoxLayout()
        similarity_layout.addWidget(QtWidgets.QLabel("Mindest-Ähnlichkeit:"))
        self.similarity_slider = QtWidgets.QSlider(QtCore.Qt.Horizontal)
        self.similarity_slider.setRange(50, 100)
        self.similarity_slider.setValue(70)
        self.similarity_slider.setTickPosition(QtWidgets.QSlider.TicksBelow)
        self.similarity_slider.setTickInterval(10)
        similarity_layout.addWidget(self.similarity_slider)
        self.similarity_label = QtWidgets.QLabel("70%")
        self.similarity_slider.valueChanged.connect(lambda v: self.similarity_label.setText(f"{v}%"))
        similarity_layout.addWidget(self.similarity_label)
        filter_layout.addLayout(similarity_layout)
        
        layout.addWidget(filter_group)
        
        # Such-Button
        self.search_folders_btn = QtWidgets.QPushButton("Ähnliche Ordner suchen")
        self.search_folders_btn.clicked.connect(self.search_folder_duplicates)
        self.search_folders_btn.setStyleSheet("""
            QPushButton {
                font-size: 14px;
                padding: 10px;
                background-color: #2196F3;
                color: white;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #0b7dda;
            }
        """)
        layout.addWidget(self.search_folders_btn)
        
        # Ergebnis-Baum
        self.folder_tree = QtWidgets.QTreeWidget()
        self.folder_tree.setHeaderLabels(["Ordner", "Laufwerk", "Dateien", "Größe", "Ähnlichkeit", "Status"])
        self.folder_tree.setAlternatingRowColors(True)
        layout.addWidget(self.folder_tree)
        
        # Kontext-Menü
        self.folder_tree.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.folder_tree.customContextMenuRequested.connect(self.show_folder_context_menu)
    
    def load_drives_and_paths(self):
        """Lädt verfügbare Laufwerke"""
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
                # Include Liste
                item1 = QtWidgets.QListWidgetItem(f"{drive_name} ({file_count:,} Dateien)")
                item1.setData(QtCore.Qt.UserRole, drive_id)
                self.include_drives_list.addItem(item1)
                
                # Exclude Liste (gleiche Daten)
                item2 = QtWidgets.QListWidgetItem(f"{drive_name} ({file_count:,} Dateien)")
                item2.setData(QtCore.Qt.UserRole, drive_id)
                self.exclude_drives_list.addItem(item2)
            
            conn.close()
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Fehler", f"Konnte Laufwerke nicht laden: {e}")
    
    def get_selected_options(self):
        """Sammelt alle ausgewählten Optionen"""
        # Include Laufwerke
        include_drives = []
        for i in range(self.include_drives_list.count()):
            item = self.include_drives_list.item(i)
            if item.isSelected():
                include_drives.append(item.data(QtCore.Qt.UserRole))
        
        # Exclude Laufwerke
        exclude_drives = []
        for i in range(self.exclude_drives_list.count()):
            item = self.exclude_drives_list.item(i)
            if item.isSelected():
                exclude_drives.append(item.data(QtCore.Qt.UserRole))
        
        # Pfade
        include_paths = [p.strip() for p in self.include_paths_edit.toPlainText().split('\n') if p.strip()]
        exclude_paths = [p.strip() for p in self.exclude_paths_edit.toPlainText().split('\n') if p.strip()]
        
        # Backup Patterns
        backup_patterns = [p.strip() for p in self.backup_patterns_edit.text().split(',') if p.strip()]
        
        return {
            'include_drives': include_drives,
            'exclude_drives': exclude_drives,
            'include_paths': include_paths,
            'exclude_paths': exclude_paths,
            'backup_patterns': backup_patterns,
            'min_size': self.min_size_spin.value() * 1024 * 1024,
            'max_size': self.max_size_spin.value() * 1024 * 1024 if self.max_size_spin.value() > 0 else None
        }
    
    def search_file_duplicates(self):
        """Startet die Datei-Duplikat-Suche"""
        if self.scan_thread and self.scan_thread.isRunning():
            return
        
        options = self.get_selected_options()
        options['search_type'] = 'files'
        
        self.file_tree.clear()
        self.search_files_btn.setEnabled(False)
        self.status_bar.showMessage("Suche Datei-Duplikate...")
        
        self.scan_thread = DuplicateScanThread(self.db_path, options)
        self.scan_thread.progress.connect(self.update_status)
        self.scan_thread.result_files.connect(self.show_file_results)
        self.scan_thread.start()
    
    def search_folder_duplicates(self):
        """Startet die Ordner-Duplikat-Suche"""
        if self.scan_thread and self.scan_thread.isRunning():
            return
        
        options = self.get_selected_options()
        options['search_type'] = 'folders'
        options['folder_similarity'] = self.similarity_slider.value()
        
        self.folder_tree.clear()
        self.search_folders_btn.setEnabled(False)
        self.status_bar.showMessage("Suche ähnliche Ordner...")
        
        self.scan_thread = DuplicateScanThread(self.db_path, options)
        self.scan_thread.progress.connect(self.update_status)
        self.scan_thread.result_folders.connect(self.show_folder_results)
        self.scan_thread.start()
    
    def update_status(self, message):
        """Aktualisiert die Statusleiste"""
        self.status_bar.showMessage(message)
    
    def show_file_results(self, groups):
        """Zeigt Datei-Duplikate an"""
        self.search_files_btn.setEnabled(True)
        
        if not groups:
            self.status_bar.showMessage("Keine Duplikate gefunden")
            return
        
        total_waste = 0
        for group in groups:
            if not group:
                continue
            
            # Sortiere: Backups zuletzt
            group.sort(key=lambda f: (f['is_backup'], f['path']))
            
            # Erstelle Gruppen-Item
            first = group[0]
            group_item = QtWidgets.QTreeWidgetItem(self.file_tree)
            group_item.setText(0, f"{first['filename']}{first['extension'] or ''}")
            group_item.setText(3, self.format_size(first['size']))
            group_item.setText(5, f"{len(group)} Kopien")
            
            # Berechne verschwendeten Speicher (ohne eine Kopie)
            waste = first['size'] * (len(group) - 1)
            total_waste += waste
            group_item.setToolTip(3, f"Verschwendet: {self.format_size(waste)}")
            
            # Faerbe Gruppe (dunkles Orange, lesbar auf dunklem Theme)
            for col in range(6):
                group_item.setBackground(col, QtGui.QColor(80, 60, 20))
                group_item.setForeground(col, QtGui.QColor(255, 200, 100))

            # Fuege Dateien hinzu
            for file in group:
                file_item = QtWidgets.QTreeWidgetItem(group_item)
                file_item.setText(0, f"{file['filename']}{file['extension'] or ''}")
                file_item.setText(1, file['path'])
                file_item.setText(2, file['drive'])
                file_item.setText(3, self.format_size(file['size']))
                file_item.setText(4, file['modified'] or "")

                if file['is_backup']:
                    file_item.setText(5, "BACKUP")
                    for col in range(6):
                        file_item.setForeground(col, QtGui.QColor(100, 180, 255))
                        file_item.setFont(col, QtGui.QFont("", -1, QtGui.QFont.Bold))
                else:
                    file_item.setText(5, "Normal")
                
                file_item.setData(0, QtCore.Qt.UserRole, file)
        
        # Expandiere erste Gruppen
        for i in range(min(5, self.file_tree.topLevelItemCount())):
            self.file_tree.topLevelItem(i).setExpanded(True)
        
        self.status_bar.showMessage(
            f"Gefunden: {len(groups)} Gruppen | "
            f"Verschwendet: {self.format_size(total_waste)}"
        )
    
    def show_folder_results(self, groups):
        """Zeigt ähnliche Ordner an"""
        self.search_folders_btn.setEnabled(True)
        
        if not groups:
            self.status_bar.showMessage("Keine ähnlichen Ordner gefunden")
            return
        
        total_size = 0
        for group in groups:
            if not group:
                continue
            
            # Sortiere: Backups zuletzt
            group.sort(key=lambda f: (f['is_backup'], f['path']))
            
            # Erstelle Gruppen-Item
            group_item = QtWidgets.QTreeWidgetItem(self.folder_tree)
            group_item.setText(0, f"Ähnliche Ordner ({len(group)})")
            group_size = sum(f['size'] or 0 for f in group)
            group_item.setText(3, self.format_size(group_size))
            total_size += group_size
            
            # Faerbe Gruppe (dunkles Blau, lesbar auf dunklem Theme)
            for col in range(6):
                group_item.setBackground(col, QtGui.QColor(20, 50, 80))
                group_item.setForeground(col, QtGui.QColor(100, 200, 255))

            # Fuege Ordner hinzu
            for folder in group:
                folder_item = QtWidgets.QTreeWidgetItem(group_item)
                folder_item.setText(0, folder['path'])
                folder_item.setText(1, folder['drive'])
                folder_item.setText(2, str(folder['file_count']))
                folder_item.setText(3, self.format_size(folder['size'] or 0))
                folder_item.setText(4, f"{folder['similarity']:.1f}%")

                if folder['is_backup']:
                    folder_item.setText(5, "BACKUP")
                    for col in range(6):
                        folder_item.setForeground(col, QtGui.QColor(100, 180, 255))
                        folder_item.setFont(col, QtGui.QFont("", -1, QtGui.QFont.Bold))
                else:
                    folder_item.setText(5, "Normal")
                
                folder_item.setData(0, QtCore.Qt.UserRole, folder)
        
        # Expandiere erste Gruppen
        for i in range(min(10, self.folder_tree.topLevelItemCount())):
            self.folder_tree.topLevelItem(i).setExpanded(True)
        
        self.status_bar.showMessage(
            f"Gefunden: {len(groups)} Ordner-Gruppen | "
            f"Gesamtgröße: {self.format_size(total_size)}"
        )
    
    def format_size(self, size):
        """Formatiert Dateigröße"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"
    
    def show_file_context_menu(self, position):
        """Kontextmenü für Dateien"""
        item = self.file_tree.itemAt(position)
        if not item or not item.parent():
            return
        
        file_data = item.data(0, QtCore.Qt.UserRole)
        if not file_data:
            return
        
        menu = QtWidgets.QMenu(self)
        
        open_action = menu.addAction("Datei öffnen")
        open_folder_action = menu.addAction("Ordner öffnen")
        menu.addSeparator()
        
        if not file_data['is_backup']:
            delete_action = menu.addAction("Datei löschen (kein Backup)")
            delete_action.setStyleSheet("color: red;")
        else:
            info_action = menu.addAction("Dies ist ein BACKUP - nicht löschen!")
            info_action.setEnabled(False)
        
        action = menu.exec_(self.file_tree.mapToGlobal(position))
        
        if action == open_action:
            os.startfile(file_data['full_path'])
        elif action == open_folder_action:
            os.startfile(file_data['path'])
        elif action and action.text().startswith("Datei löschen"):
            reply = QtWidgets.QMessageBox.question(
                self, 'Bestätigung',
                f"Wirklich löschen?\n{file_data['full_path']}",
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No
            )
            if reply == QtWidgets.QMessageBox.Yes:
                try:
                    os.remove(file_data['full_path'])
                    item.parent().removeChild(item)
                    self.status_bar.showMessage("Datei gelöscht")
                except Exception as e:
                    QtWidgets.QMessageBox.warning(self, "Fehler", f"Konnte nicht löschen: {e}")
    
    def show_folder_context_menu(self, position):
        """Kontextmenü für Ordner"""
        item = self.folder_tree.itemAt(position)
        if not item or not item.parent():
            return
        
        folder_data = item.data(0, QtCore.Qt.UserRole)
        if not folder_data:
            return
        
        menu = QtWidgets.QMenu(self)
        
        open_action = menu.addAction("Ordner öffnen")
        compare_action = menu.addAction("Ordner vergleichen")
        menu.addSeparator()
        
        if not folder_data['is_backup']:
            info_action = menu.addAction("Hinweis: Ordner-Löschung nur manuell")
            info_action.setEnabled(False)
        else:
            info_action = menu.addAction("Dies ist ein BACKUP-Ordner!")
            info_action.setEnabled(False)
        
        action = menu.exec_(self.folder_tree.mapToGlobal(position))
        
        if action == open_action:
            os.startfile(folder_data['path'])
        elif action == compare_action:
            # Öffne alle Ordner der Gruppe zum Vergleich
            parent = item.parent()
            for i in range(parent.childCount()):
                child = parent.child(i)
                child_data = child.data(0, QtCore.Qt.UserRole)
                if child_data:
                    os.startfile(child_data['path'])

def main():
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle('Fusion')
    
    # Dunkles Theme
    palette = QtGui.QPalette()
    palette.setColor(QtGui.QPalette.Window, QtGui.QColor(53, 53, 53))
    palette.setColor(QtGui.QPalette.WindowText, QtCore.Qt.white)
    palette.setColor(QtGui.QPalette.Base, QtGui.QColor(25, 25, 25))
    palette.setColor(QtGui.QPalette.AlternateBase, QtGui.QColor(53, 53, 53))
    palette.setColor(QtGui.QPalette.ToolTipBase, QtCore.Qt.black)
    palette.setColor(QtGui.QPalette.ToolTipText, QtCore.Qt.white)
    palette.setColor(QtGui.QPalette.Text, QtCore.Qt.white)
    palette.setColor(QtGui.QPalette.Button, QtGui.QColor(53, 53, 53))
    palette.setColor(QtGui.QPalette.ButtonText, QtCore.Qt.white)
    palette.setColor(QtGui.QPalette.BrightText, QtCore.Qt.red)
    palette.setColor(QtGui.QPalette.Link, QtGui.QColor(42, 130, 218))
    palette.setColor(QtGui.QPalette.Highlight, QtGui.QColor(42, 130, 218))
    palette.setColor(QtGui.QPalette.HighlightedText, QtCore.Qt.black)
    app.setPalette(palette)
    
    window = AdvancedDuplicateManager()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()