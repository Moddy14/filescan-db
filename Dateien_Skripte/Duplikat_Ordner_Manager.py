#!/usr/bin/env python3
r"""
Duplikat-Ordner-Manager
Findet Ordner mit vielen doppelten Dateien und ermöglicht Vergleich und Löschung.
WICHTIG: Berücksichtigt Laufwerk-Aliases (T: -> C:\Laufwerk T\USB16GB) um falsche Duplikate zu vermeiden.
"""

import sys
import os
import sqlite3
import shutil
from collections import defaultdict, Counter
from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import QThread, pyqtSignal

# Import parent directory for drive_alias_detector
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from drive_alias_detector import is_path_alias_of, normalize_path_with_aliases, get_drive_mapping

class DuplicateFolderAnalyzer(QThread):
    """Thread für die Analyse von Duplikat-Ordner-Paaren"""
    progress = pyqtSignal(str)
    finished = pyqtSignal(list)  # Liste von Ordner-Paaren mit Übereinstimmungsgrad
    
    def __init__(self, db_path, min_duplicates=5, min_similarity=50):
        super().__init__()
        self.db_path = db_path
        self.min_duplicates = min_duplicates
        self.min_similarity = min_similarity  # Mindest-Ähnlichkeit in %
    
    def run(self):
        try:
            self.progress.emit("Analysiere Datenbank...")
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # SCHRITT 1: Finde erst alle Duplikat-Dateien (Name + Größe)
            self.progress.emit("1/4: Suche doppelte Dateien...")
            cursor.execute("""
                SELECT 
                    files.filename || COALESCE(extensions.name, '') as full_filename,
                    files.size,
                    COUNT(*) as duplicate_count
                FROM files
                LEFT JOIN extensions ON files.extension_id = extensions.id
                WHERE files.filename IS NOT NULL AND files.filename != ''
                GROUP BY files.filename, COALESCE(extensions.name, ''), files.size
                HAVING COUNT(*) > 1
            """)
            
            duplicate_files = set((row[0], row[1]) for row in cursor.fetchall())
            
            if not duplicate_files:
                self.progress.emit("Keine doppelten Dateien gefunden")
                self.finished.emit([])
                conn.close()
                return
            
            self.progress.emit(f"2/4: Gefunden {len(duplicate_files)} Duplikat-Gruppen")
            
            # SCHRITT 2: Finde alle Ordner mit Duplikat-Dateien (super-optimiert)
            self.progress.emit("3/4: Analysiere betroffene Ordner (ultra-optimiert)...")
            
            # Erstelle temporäre Tabelle für bessere Performance
            cursor.execute("CREATE TEMP TABLE temp_duplicates (filename TEXT, size INTEGER)")
            cursor.executemany("INSERT INTO temp_duplicates VALUES (?, ?)", list(duplicate_files))
            
            # Ein JOIN mit der temporären Tabelle - viel schneller als OR-Klauseln!
            batch_query = """
                SELECT 
                    directories.full_path,
                    files.filename || COALESCE(extensions.name, '') as full_filename,
                    files.size
                FROM files
                JOIN directories ON files.directory_id = directories.id
                LEFT JOIN extensions ON files.extension_id = extensions.id
                JOIN temp_duplicates ON (
                    temp_duplicates.filename = files.filename || COALESCE(extensions.name, '') 
                    AND temp_duplicates.size = files.size
                )
            """
            
            cursor.execute(batch_query)
            results = cursor.fetchall()
            
            # Gruppiere Ergebnisse nach Ordnern
            duplicate_folders = defaultdict(set)
            for folder_path, filename, size in results:
                duplicate_folders[folder_path].add((filename, size))
            
            # Temporäre Tabelle löschen
            cursor.execute("DROP TABLE temp_duplicates")
            
            # Filter: Nur Ordner mit genug Duplikaten behalten
            candidate_folders = {
                folder: files for folder, files in duplicate_folders.items() 
                if len(files) >= self.min_duplicates
            }
            
            if len(candidate_folders) < 2:
                self.progress.emit("Nicht genug Ordner mit ausreichend Duplikaten gefunden")
                self.finished.emit([])
                conn.close()
                return
            
            self.progress.emit(f"4/4: Vergleiche {len(candidate_folders)} relevante Ordner...")
            
            # SCHRITT 3: Lade alle Dateien nur für relevante Ordner
            folder_all_files = {}
            folder_list = list(candidate_folders.keys())
            
            for folder_path in folder_list:
                cursor.execute("""
                    SELECT 
                        files.filename || COALESCE(extensions.name, '') as full_filename,
                        files.size
                    FROM files
                    JOIN directories ON files.directory_id = directories.id
                    LEFT JOIN extensions ON files.extension_id = extensions.id
                    WHERE directories.full_path = ?
                """, (folder_path,))
                
                folder_all_files[folder_path] = set((row[0], row[1]) for row in cursor.fetchall())
            
            # SCHRITT 4: Vergleiche nur relevante Ordner-Paare (viel weniger!)
            folder_pairs = []
            pairs_checked = 0
            total_pairs = len(folder_list) * (len(folder_list) - 1) // 2
            
            for i in range(len(folder_list)):
                for j in range(i + 1, len(folder_list)):
                    pairs_checked += 1
                    if pairs_checked % 100 == 0:
                        progress = (pairs_checked / total_pairs) * 100
                        self.progress.emit(f"Vergleiche Paare: {progress:.1f}% ({pairs_checked}/{total_pairs})")
                    
                    folder1 = folder_list[i]
                    folder2 = folder_list[j]
                    
                    # WICHTIG: Überspringe Vergleich wenn Ordner Aliases sind (gleiche physische Stelle)
                    if is_path_alias_of(folder1, folder2):
                        self.progress.emit(f"Überspringe Alias-Paar: {folder1} <-> {folder2}")
                        continue
                    
                    files1 = folder_all_files[folder1]
                    files2 = folder_all_files[folder2]
                    
                    # Berechne Überschneidung
                    common_files = files1 & files2
                    
                    if len(common_files) >= self.min_duplicates:
                        unique_to_1 = files1 - files2
                        unique_to_2 = files2 - files1
                        
                        # Berechne Ähnlichkeits-Metriken
                        total_files = len(files1) + len(files2)
                        similarity_percent = (len(common_files) * 2 / max(total_files, 1)) * 100
                        
                        if similarity_percent >= self.min_similarity:
                            # Bestimme welcher Ordner gelöscht werden kann
                            can_delete_folder1 = len(unique_to_1) == 0
                            can_delete_folder2 = len(unique_to_2) == 0
                            
                            folder_pairs.append({
                                'folder1': folder1,
                                'folder2': folder2,
                                'folder1_files': len(files1),
                                'folder2_files': len(files2),
                                'common_files': len(common_files),
                                'unique_to_1': len(unique_to_1),
                                'unique_to_2': len(unique_to_2),
                                'similarity_percent': similarity_percent,
                                'can_delete_folder1': can_delete_folder1,
                                'can_delete_folder2': can_delete_folder2,
                                'recommendation': self.get_recommendation(can_delete_folder1, can_delete_folder2, folder1, folder2)
                            })
            
            # Sortiere nach Ähnlichkeit (absteigend)
            folder_pairs.sort(key=lambda x: x['similarity_percent'], reverse=True)
            
            conn.close()
            self.progress.emit(f"✓ Analyse abgeschlossen: {len(folder_pairs)} relevante Ordner-Paare gefunden")
            self.finished.emit(folder_pairs)
            
        except Exception as e:
            self.progress.emit(f"Fehler bei der Analyse: {str(e)}")
            self.finished.emit([])
    
    def get_recommendation(self, can_delete_1, can_delete_2, folder1, folder2):
        """Erstellt eine Löschempfehlung"""
        if can_delete_1 and not can_delete_2:
            return f"Lösche {os.path.basename(folder1)} (alle Dateien sind in {os.path.basename(folder2)} vorhanden)"
        elif can_delete_2 and not can_delete_1:
            return f"Lösche {os.path.basename(folder2)} (alle Dateien sind in {os.path.basename(folder1)} vorhanden)"
        elif can_delete_1 and can_delete_2:
            return "Beide Ordner sind identisch - einen beliebigen löschen"
        else:
            return "Manuelle Prüfung erforderlich - beide Ordner haben einzigartige Dateien"

class FolderComparisonWorker(QThread):
    """Thread für detaillierten Vergleich zweier Ordner"""
    progress = pyqtSignal(str)
    finished = pyqtSignal(dict)  # Vergleichsresultat
    
    def __init__(self, db_path, folder1, folder2):
        super().__init__()
        self.db_path = db_path
        self.folder1 = folder1
        self.folder2 = folder2
    
    def run(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Lade alle Dateien beider Ordner (Dateiname + Größe als Schlüssel)
            self.progress.emit(f"Lade Dateien von {os.path.basename(self.folder1)}...")
            cursor.execute("""
                SELECT files.filename, COALESCE(extensions.name, '') as ext, files.size, files.hash
                FROM files
                JOIN directories ON files.directory_id = directories.id
                LEFT JOIN extensions ON files.extension_id = extensions.id
                WHERE directories.full_path = ?
            """, (self.folder1,))
            files1 = {(filename + ext, size): hash_val for filename, ext, size, hash_val in cursor.fetchall()}
            
            self.progress.emit(f"Lade Dateien von {os.path.basename(self.folder2)}...")
            cursor.execute("""
                SELECT files.filename, COALESCE(extensions.name, '') as ext, files.size, files.hash
                FROM files
                JOIN directories ON files.directory_id = directories.id
                LEFT JOIN extensions ON files.extension_id = extensions.id
                WHERE directories.full_path = ?
            """, (self.folder2,))
            files2 = {(filename + ext, size): hash_val for filename, ext, size, hash_val in cursor.fetchall()}
            
            # Vergleiche die Dateien basierend auf Name + Größe
            self.progress.emit("Vergleiche Dateien...")
            
            # Dateien die in beiden Ordnern sind (gleicher Name + Größe)
            identical_files = []
            unique_to_folder1 = []
            unique_to_folder2 = []
            
            # Schlüssel sind (filename, size)
            keys1 = set(files1.keys())
            keys2 = set(files2.keys())
            
            # Finde identische Dateien (gleicher Name + Größe)
            common_keys = keys1 & keys2
            for (filename, size) in common_keys:
                identical_files.append({
                    'filename': filename,
                    'size': size,
                    'hash1': files1.get((filename, size)),
                    'hash2': files2.get((filename, size))
                })
            
            # Finde einzigartige Dateien
            unique_keys_1 = keys1 - keys2
            unique_keys_2 = keys2 - keys1
            
            for (filename, size) in unique_keys_1:
                unique_to_folder1.append({
                    'filename': filename, 
                    'size': size, 
                    'hash': files1.get((filename, size))
                })
            
            for (filename, size) in unique_keys_2:
                unique_to_folder2.append({
                    'filename': filename, 
                    'size': size, 
                    'hash': files2.get((filename, size))
                })
            
            # Berechne Ähnlichkeitsprozent
            total_files = len(files1) + len(files2)
            identical_count = len(identical_files)
            similarity_percent = (identical_count * 2 / max(total_files, 1)) * 100 if total_files > 0 else 0
            
            result = {
                'folder1': self.folder1,
                'folder2': self.folder2,
                'folder1_file_count': len(files1),
                'folder2_file_count': len(files2),
                'identical_files': identical_files,
                'unique_to_folder1': unique_to_folder1,
                'unique_to_folder2': unique_to_folder2,
                'similarity_percent': similarity_percent,
                'can_delete_folder1': len(unique_to_folder1) == 0,
                'can_delete_folder2': len(unique_to_folder2) == 0
            }
            
            conn.close()
            self.progress.emit("Vergleich abgeschlossen")
            self.finished.emit(result)
            
        except Exception as e:
            self.progress.emit(f"Fehler beim Vergleich: {str(e)}")
            self.finished.emit({})

class DuplicateFolderManager(QtWidgets.QMainWindow):
    def __init__(self, db_path):
        super().__init__()
        self.db_path = db_path
        self.folder_pairs = []
        self.selected_pair = None
        self.init_ui()
        
    def init_ui(self):
        self.setWindowTitle("Duplikat-Ordner-Manager")
        self.setGeometry(200, 200, 1200, 800)
        
        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QtWidgets.QVBoxLayout(central_widget)
        
        # Header
        header_label = QtWidgets.QLabel("🔍 Duplikat-Ordner-Manager")
        header_label.setStyleSheet("font-size: 18px; font-weight: bold; margin: 10px;")
        main_layout.addWidget(header_label)
        
        # Analyse-Bereich
        analysis_group = QtWidgets.QGroupBox("1. Ordner-Paar-Analyse")
        analysis_layout = QtWidgets.QVBoxLayout(analysis_group)
        
        # Einstellungen
        settings_layout = QtWidgets.QHBoxLayout()
        settings_layout.addWidget(QtWidgets.QLabel("Mindestanzahl Duplikate:"))
        self.min_duplicates_spin = QtWidgets.QSpinBox()
        self.min_duplicates_spin.setRange(1, 100)
        self.min_duplicates_spin.setValue(5)
        settings_layout.addWidget(self.min_duplicates_spin)
        
        settings_layout.addWidget(QtWidgets.QLabel("Mindest-Ähnlichkeit (%):"))
        self.min_similarity_spin = QtWidgets.QSpinBox()
        self.min_similarity_spin.setRange(1, 100)
        self.min_similarity_spin.setValue(50)
        settings_layout.addWidget(self.min_similarity_spin)
        
        self.analyze_btn = QtWidgets.QPushButton("🔍 Ordner-Paare analysieren")
        self.analyze_btn.clicked.connect(self.start_analysis)
        settings_layout.addWidget(self.analyze_btn)
        settings_layout.addStretch()
        
        analysis_layout.addLayout(settings_layout)
        
        # Ergebnis-Tabelle für Ordner-Paare
        self.results_table = QtWidgets.QTableWidget()
        self.results_table.setColumnCount(6)
        self.results_table.setHorizontalHeaderLabels([
            "Ordner 1", "Ordner 2", "Gemeinsame Dateien", "Ähnlichkeit %", "Empfehlung", "Aktion"
        ])
        self.results_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.results_table.setAlternatingRowColors(True)
        analysis_layout.addWidget(self.results_table)
        
        main_layout.addWidget(analysis_group)
        
        # Detail-Bereich
        details_group = QtWidgets.QGroupBox("2. Paar-Details")
        details_layout = QtWidgets.QVBoxLayout(details_group)
        
        # Details-Anzeige
        self.details_text = QtWidgets.QTextEdit()
        self.details_text.setMaximumHeight(150)
        self.details_text.setReadOnly(True)
        self.details_text.setPlainText("Wähle ein Ordner-Paar aus der Tabelle für Details")
        details_layout.addWidget(self.details_text)
        
        main_layout.addWidget(details_group)
        
        # Aktions-Bereich
        actions_group = QtWidgets.QGroupBox("3. Aktionen")
        actions_layout = QtWidgets.QHBoxLayout(actions_group)
        
        self.delete_folder1_btn = QtWidgets.QPushButton("🗑️ Ersten Ordner löschen")
        self.delete_folder1_btn.clicked.connect(lambda: self.delete_selected_folder(0))
        self.delete_folder1_btn.setEnabled(False)
        self.delete_folder1_btn.setStyleSheet("background-color: #ff6b6b; color: white; font-weight: bold;")
        
        self.delete_folder2_btn = QtWidgets.QPushButton("🗑️ Zweiten Ordner löschen")
        self.delete_folder2_btn.clicked.connect(lambda: self.delete_selected_folder(1))
        self.delete_folder2_btn.setEnabled(False)
        self.delete_folder2_btn.setStyleSheet("background-color: #ff6b6b; color: white; font-weight: bold;")
        
        actions_layout.addWidget(self.delete_folder1_btn)
        actions_layout.addWidget(self.delete_folder2_btn)
        actions_layout.addStretch()
        
        main_layout.addWidget(actions_group)
        
        # Status-Bereich
        self.status_label = QtWidgets.QLabel("Bereit für Analyse")
        self.status_label.setStyleSheet("padding: 10px; background-color: #f0f0f0; border: 1px solid #ccc;")
        main_layout.addWidget(self.status_label)
        
        # Event-Connections
        self.results_table.selectionModel().selectionChanged.connect(self.on_selection_changed)
        
    def start_analysis(self):
        """Startet die Ordner-Paar-Analyse"""
        self.analyze_btn.setEnabled(False)
        self.status_label.setText("Analysiere...")
        
        min_dups = self.min_duplicates_spin.value()
        min_sim = self.min_similarity_spin.value()
        self.analyzer = DuplicateFolderAnalyzer(self.db_path, min_dups, min_sim)
        self.analyzer.progress.connect(self.status_label.setText)
        self.analyzer.finished.connect(self.on_analysis_finished)
        self.analyzer.start()
    
    def on_analysis_finished(self, results):
        """Verarbeitet die Ordner-Paar-Ergebnisse"""
        self.analyze_btn.setEnabled(True)
        self.folder_pairs = results
        
        # Tabelle füllen
        self.results_table.setRowCount(len(results))
        for row, pair in enumerate(results):
            # Ordner-Namen kürzen für bessere Darstellung
            folder1_name = os.path.basename(pair['folder1']) or pair['folder1']
            folder2_name = os.path.basename(pair['folder2']) or pair['folder2']
            
            self.results_table.setItem(row, 0, QtWidgets.QTableWidgetItem(folder1_name))
            self.results_table.setItem(row, 1, QtWidgets.QTableWidgetItem(folder2_name))
            self.results_table.setItem(row, 2, QtWidgets.QTableWidgetItem(str(pair['common_files'])))
            self.results_table.setItem(row, 3, QtWidgets.QTableWidgetItem(f"{pair['similarity_percent']:.1f}%"))
            self.results_table.setItem(row, 4, QtWidgets.QTableWidgetItem(pair['recommendation']))
            
            # Aktions-Button
            action_text = "Kann gelöscht werden" if (pair['can_delete_folder1'] or pair['can_delete_folder2']) else "Manuelle Prüfung"
            self.results_table.setItem(row, 5, QtWidgets.QTableWidgetItem(action_text))
        
        self.results_table.resizeColumnsToContents()
        self.status_label.setText(f"✓ Analyse abgeschlossen: {len(results)} Ordner-Paare gefunden")
    
    def on_selection_changed(self):
        """Reagiert auf Änderungen der Tabellenauswahl"""
        selected_rows = list(set(index.row() for index in self.results_table.selectedIndexes()))
        
        if len(selected_rows) == 1 and selected_rows[0] < len(self.folder_pairs):
            # Ein Paar ausgewählt - zeige Details
            pair = self.folder_pairs[selected_rows[0]]
            self.selected_pair = pair
            self.show_pair_details(pair)
            self.update_action_buttons(pair)
        else:
            # Kein oder mehrere Paare ausgewählt
            self.selected_pair = None
            self.details_text.setPlainText("Wähle ein Ordner-Paar aus der Tabelle für Details")
            self.delete_folder1_btn.setEnabled(False)
            self.delete_folder2_btn.setEnabled(False)
    
    def show_pair_details(self, pair):
        """Zeigt Details für ein Ordner-Paar"""
        details = f"""
📁 ORDNER-PAAR DETAILS

Ordner 1: {pair['folder1']}
  └─ Dateien: {pair['folder1_files']}
  └─ Nur hier: {pair['unique_to_1']} Dateien

Ordner 2: {pair['folder2']}
  └─ Dateien: {pair['folder2_files']}
  └─ Nur hier: {pair['unique_to_2']} Dateien

🔄 GEMEINSAME DATEIEN: {pair['common_files']}
📊 ÄHNLICHKEIT: {pair['similarity_percent']:.1f}%

💡 EMPFEHLUNG: {pair['recommendation']}
"""
        self.details_text.setPlainText(details.strip())
    
    def update_action_buttons(self, pair):
        """Aktualisiert die Lösch-Buttons basierend auf dem ausgewählten Paar"""
        folder1_name = os.path.basename(pair['folder1'])
        folder2_name = os.path.basename(pair['folder2'])
        
        self.delete_folder1_btn.setEnabled(pair['can_delete_folder1'])
        self.delete_folder2_btn.setEnabled(pair['can_delete_folder2'])
        
        self.delete_folder1_btn.setText(f"🗑️ {folder1_name} löschen")
        self.delete_folder2_btn.setText(f"🗑️ {folder2_name} löschen")
    
    
    def delete_selected_folder(self, folder_index):
        """Löscht einen Ordner aus dem ausgewählten Paar nach Bestätigung"""
        if not self.selected_pair:
            return
        
        folder_path = self.selected_pair['folder1'] if folder_index == 0 else self.selected_pair['folder2']
        file_count = self.selected_pair['folder1_files'] if folder_index == 0 else self.selected_pair['folder2_files']
        folder_name = os.path.basename(folder_path)
        
        # Sicherheitsabfrage
        reply = QtWidgets.QMessageBox.question(
            self,
            "🗑️ ORDNER LÖSCHEN",
            f"WARNUNG: Du bist dabei, den folgenden Ordner DAUERHAFT zu löschen:\n\n"
            f"📁 {folder_path}\n\n"
            f"Dateien: {file_count}\n\n"
            f"⚠️ DIESE AKTION KANN NICHT RÜCKGÄNGIG GEMACHT WERDEN!\n\n"
            f"Bist du sicher?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
            QtWidgets.QMessageBox.No
        )
        
        if reply == QtWidgets.QMessageBox.Yes:
            # Zweite Bestätigung
            final_reply = QtWidgets.QMessageBox.question(
                self,
                "🚨 LETZTE WARNUNG",
                f"LETZTE CHANCE!\n\n"
                f"Der Ordner {folder_name} wird jetzt gelöscht.\n\n"
                f"Wirklich fortfahren?",
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                QtWidgets.QMessageBox.No
            )
            
            if final_reply == QtWidgets.QMessageBox.Yes:
                try:
                    self.status_label.setText(f"Lösche {folder_name}...")
                    shutil.rmtree(folder_path)
                    
                    QtWidgets.QMessageBox.information(
                        self, 
                        "✅ Erfolgreich", 
                        f"Ordner {folder_name} wurde erfolgreich gelöscht."
                    )
                    
                    # Interface zurücksetzen
                    self.selected_pair = None
                    self.delete_folder1_btn.setEnabled(False)
                    self.delete_folder2_btn.setEnabled(False)
                    self.details_text.setPlainText("Ordner gelöscht - Analyse kann wiederholt werden")
                    self.status_label.setText("Ordner erfolgreich gelöscht")
                    
                except Exception as e:
                    QtWidgets.QMessageBox.critical(
                        self,
                        "❌ Fehler",
                        f"Fehler beim Löschen des Ordners:\n{str(e)}"
                    )
                    self.status_label.setText(f"Fehler beim Löschen: {str(e)}")

def main():
    if len(sys.argv) < 2:
        QtWidgets.QMessageBox.critical(
            None, 
            "Fehler", 
            "Usage: python Duplikat_Ordner_Manager.py <database_path>"
        )
        sys.exit(1)
    
    db_path = sys.argv[1]
    if not os.path.exists(db_path):
        QtWidgets.QMessageBox.critical(
            None, 
            "Fehler", 
            f"Datenbank nicht gefunden: {db_path}"
        )
        sys.exit(1)
    
    app = QtWidgets.QApplication(sys.argv)
    
    # Anwendungsmetadaten
    app.setApplicationName("Duplikat-Ordner-Manager")
    app.setApplicationVersion("1.0")
    
    window = DuplicateFolderManager(db_path)
    window.show()
    
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()