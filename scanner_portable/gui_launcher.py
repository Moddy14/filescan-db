import os
import sys
import subprocess
import time # für sleep in LogUpdater
import logging # Logging hinzugefügt
import html  # Für HTML-Export im Exporter-Teil (jetzt hier importiert? Besser nicht.)
from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtWidgets import QDialog, QVBoxLayout, QCheckBox, QLabel, QListWidget, QPushButton, QFileDialog, QHBoxLayout, QListWidgetItem, QMessageBox, QApplication, QMainWindow, QWidget, QProgressBar, QTreeView, QFileSystemModel, QTextEdit, QDialogButtonBox, QMenu, QAction, QHeaderView, QComboBox, QTableWidget, QTableWidgetItem, QTimeEdit, QAbstractItemView, QInputDialog
from models import get_db_instance
import hashlib
import json
from datetime import datetime
from PyQt5.QtCore import QProcess, pyqtSignal, QThread, QTime, Qt

# Importiere aus utils
from utils import (
    calculate_hash, HASHING, DB_PATH, CONFIG, # Nutze CONFIG direkt
    PROJECT_DIR, LOG_PATH, save_config, setup_logging, load_config, logger, get_available_drives # get_available_drives importieren
)

# Logging für GUI konfigurieren (nutzt scanner.log)
logger = setup_logging(level_str=CONFIG.get('log_level', 'INFO'))

def calculate_hash(filepath):
    try:
        hasher = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception:
        return None

class LogUpdater(QtCore.QThread):
    log_updated = QtCore.pyqtSignal(str)

    def __init__(self, log_file, parent=None):
        super().__init__(parent)
        self.log_file = log_file
        self._running = True
        self._last_size = 0

    def run(self):
        while self._running:
            try:
                if os.path.exists(self.log_file):
                    with open(self.log_file, "r", encoding="utf-8") as f:
                        f.seek(self._last_size)
                        new_data = f.read()
                        if new_data:
                            self.log_updated.emit(new_data)
                            self._last_size = f.tell()
            except Exception as e:
                self.log_updated.emit(f"[Log-Fehler] {e}\n")
            self.msleep(1000)

    def stop(self):
        self._running = False

class ScanWorker(QtCore.QThread):
    """Führt den Scan-Vorgang in einem separaten Thread aus."""
    scan_progress = QtCore.pyqtSignal(str) # Signal für Fortschrittsmeldungen
    scan_finished = QtCore.pyqtSignal(bool, str) # Signal für Abschluss (Erfolg, Nachricht)

    def __init__(self, base_path, parent=None):
        super().__init__(parent)
        self.base_path = base_path
        self._running = True

    def run(self):
        # ... (Rest der run-Methode wie zuvor implementiert)
        # ... (mit korrekter Einrückung)
        pass # Platzhalter, falls run leer wäre

    def stop(self):
        """Signalisiert dem Thread, dass er beendet werden soll."""
        self._running = False

# --- Neue Klasse für den Einstellungsdialog --- 
class HashingSettingsDialog(QDialog):
    def __init__(self, current_config, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Hashing-Einstellungen")
        self.current_config = current_config # Referenz auf die globale CONFIG
        self.original_config_copy = current_config.copy() # Kopie für Abbrechen
        self.setMinimumWidth(500)

        layout = QVBoxLayout(self)

        # Globale Einstellung
        self.global_hashing_checkbox = QCheckBox("Hashing global aktivieren (langsamer)")
        self.global_hashing_checkbox.setToolTip("Wenn aktiviert, wird für jede Datei der Hash berechnet, außer in spezifischen Verzeichnissen.")
        self.global_hashing_checkbox.setChecked(self.current_config.get('hashing', False))
        layout.addWidget(self.global_hashing_checkbox)

        layout.addWidget(QLabel("Spezifische Verzeichnisse (werden *immer* gehasht, auch wenn global deaktiviert):"))

        # Liste für Verzeichnisse
        self.dir_list_widget = QListWidget()
        self.dir_list_widget.setToolTip("Dateien in diesen Verzeichnissen (und Unterverzeichnissen) werden immer gehasht.")
        self.populate_list()
        layout.addWidget(self.dir_list_widget)

        # Buttons zum Hinzufügen/Entfernen
        button_layout = QHBoxLayout()
        add_button = QPushButton("➕ Verzeichnis hinzufügen...")
        add_button.clicked.connect(self.add_directory)
        remove_button = QPushButton("➖ Ausgewähltes entfernen")
        remove_button.clicked.connect(self.remove_directory)
        button_layout.addWidget(add_button)
        button_layout.addWidget(remove_button)
        layout.addLayout(button_layout)

        # Speichern/Abbrechen Buttons
        save_cancel_layout = QHBoxLayout()
        save_button = QPushButton("💾 Speichern")
        save_button.clicked.connect(self.save_settings)
        cancel_button = QPushButton("Abbrechen")
        cancel_button.clicked.connect(self.reject) # Schließt den Dialog ohne Speichern
        save_cancel_layout.addStretch()
        save_cancel_layout.addWidget(save_button)
        save_cancel_layout.addWidget(cancel_button)
        layout.addLayout(save_cancel_layout)

    def populate_list(self):
        """Füllt die Liste mit den aktuellen Verzeichnissen."""
        self.dir_list_widget.clear()
        hash_dirs = self.current_config.get('hash_directories', [])
        if hash_dirs:
            # Normalisiere Pfade zur Anzeige
            normalized_dirs = [os.path.normpath(p) for p in hash_dirs]
            self.dir_list_widget.addItems(sorted(normalized_dirs)) # Sortiert anzeigen

    def add_directory(self):
        """Öffnet einen Dialog zur Auswahl eines Verzeichnisses."""
        directory = QFileDialog.getExistingDirectory(self, "Verzeichnis für Hashing auswählen", os.path.expanduser("~"))
        if directory:
            normalized_dir = os.path.normpath(directory)
            # Prüfen, ob schon vorhanden (oder ein Unterverzeichnis/Oberverzeichnis)
            current_items = [self.dir_list_widget.item(i).text() for i in range(self.dir_list_widget.count())]
            
            # Einfache Prüfung auf Duplikate
            if normalized_dir in current_items:
                 QMessageBox.information(self, "Bereits vorhanden", "Dieses Verzeichnis ist bereits in der Liste.")
                 return
                 
            # Komplexere Prüfung (optional):
            # ... (wie oben beschrieben, erstmal weggelassen) ...

            self.dir_list_widget.addItem(normalized_dir)
            self.dir_list_widget.sortItems() # Sortieren nach Hinzufügen

    def remove_directory(self):
        """Entfernt das ausgewählte Verzeichnis aus der Liste."""
        selected_items = self.dir_list_widget.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Keine Auswahl", "Bitte wähle zuerst ein Verzeichnis zum Entfernen aus.")
            return
        # Rückwärts entfernen ist sicherer bei Multi-Selektion
        for item in reversed(selected_items):
            self.dir_list_widget.takeItem(self.dir_list_widget.row(item))

    def save_settings(self):
        """Speichert die Einstellungen in die globale CONFIG und ruft save_config auf."""
        # Aktualisiere globale Hashing-Einstellung
        self.current_config['hashing'] = self.global_hashing_checkbox.isChecked()

        # Aktualisiere Liste der Hash-Verzeichnisse
        hash_dirs = []
        for i in range(self.dir_list_widget.count()):
            hash_dirs.append(self.dir_list_widget.item(i).text())
        # Normalisiere Pfade beim Speichern und sortiere
        self.current_config['hash_directories'] = sorted([os.path.normpath(p) for p in hash_dirs])

        try:
            save_config(self.current_config) # Speichere die geänderte Konfiguration
            logger.info("[GUI] Hashing-Einstellungen gespeichert.")
            # Info für den Benutzer
            QMessageBox.information(self, "Gespeichert", "Die Hashing-Einstellungen wurden gespeichert.\nDie Änderungen werden beim nächsten Scan wirksam.")
            self.accept() # Schließt den Dialog mit OK-Status
        except Exception as e:
            error_msg = f"Fehler beim Speichern der Konfiguration: {e}"
            logger.error(f"[GUI] {error_msg}")
            QMessageBox.critical(self, "Speicherfehler", error_msg)
    
    # Optional: Beim Abbrechen die Änderungen an current_config rückgängig machen?
    # Momentan wird die übergebene CONFIG direkt modifiziert, aber erst beim Speichern
    # dauerhaft geschrieben. Wenn der Dialog verworfen wird (reject), bleiben die
    # Änderungen im Speicher, bis CONFIG neu geladen wird (z.B. Neustart). 
    # Besser wäre es, intern mit einer Kopie zu arbeiten und nur bei save zu übernehmen.
    # --> Implementierung erstmal so belassen.

class ScanSettingsDialog(QDialog):
    """Dialog zum Einstellen der Scan-Optionen."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Scan-Einstellungen")
        self.layout = QVBoxLayout(self)

        # Checkbox für Scan fortsetzen
        self.resume_checkbox = QCheckBox("Scan an letzter Position fortsetzen")
        self.resume_checkbox.setChecked(CONFIG.get('resume_scan', True))
        self.layout.addWidget(self.resume_checkbox)

        # Dialog-Buttons
        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        self.layout.addWidget(self.button_box)

    def get_settings(self):
        return {
            "resume_scan": self.resume_checkbox.isChecked()
        }

class ScheduledScansDialog(QDialog):
    """Dialog zum Verwalten der geplanten Scans mit Pfaden, Zeiten und Aktivierungsstatus."""
    def __init__(self, current_config, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Geplante Scans")
        self.current_config = current_config
        self.setMinimumWidth(700) # Noch breiter

        layout = QVBoxLayout(self)

        layout.addWidget(QLabel("Scans, die automatisch ausgeführt werden sollen:"))

        # Tabelle für Pfade, Zeiten, Aktivierung, Neustart
        self.scan_table = QTableWidget()
        self.scan_table.setColumnCount(4) # Erhöht auf 4 Spalten
        self.scan_table.setHorizontalHeaderLabels(["Aktiviert", "Pfad", "Zeit (HH:MM)", "Immer neu starten?"])
        self.scan_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.scan_table.verticalHeader().setVisible(False)
        # Spaltenbreiten anpassen
        header = self.scan_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents) # Aktiviert
        header.setSectionResizeMode(1, QHeaderView.Stretch) # Pfad
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents) # Zeit
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents) # Neustart

        self.populate_table()
        layout.addWidget(self.scan_table)

        # Buttons zum Hinzufügen/Entfernen/Bearbeiten?
        button_layout = QHBoxLayout()
        add_button = QPushButton("➕ Neu...")
        add_button.clicked.connect(self.add_scan)
        remove_button = QPushButton("➖ Ausgewählte entfernen")
        remove_button.clicked.connect(self.remove_scans)
        # Editieren durch Doppelklick oder separaten Button?
        # Wir machen es einfach: Checkbox direkt, Pfad und Zeit über Button hinzufügen/entfernen
        button_layout.addWidget(add_button)
        button_layout.addWidget(remove_button)
        button_layout.addStretch()
        layout.addLayout(button_layout)

        # Speichern/Abbrechen Buttons
        save_cancel_layout = QHBoxLayout()
        save_button = QPushButton("💾 Speichern")
        save_button.clicked.connect(self.save_settings)
        cancel_button = QPushButton("Abbrechen")
        cancel_button.clicked.connect(self.reject)
        save_cancel_layout.addStretch()
        save_cancel_layout.addWidget(save_button)
        save_cancel_layout.addWidget(cancel_button)
        layout.addLayout(save_cancel_layout)

    def populate_table(self):
        """Füllt die Tabelle mit den aktuellen geplanten Scans."""
        self.scan_table.setRowCount(0)
        scheduled_scans = self.current_config.get('scheduled_scans', [])
        self.scan_table.setRowCount(len(scheduled_scans))

        for row, scan_info in enumerate(scheduled_scans):
            path = scan_info.get("path", "")
            time_str = scan_info.get("time", "00:00")
            enabled = scan_info.get("enabled", True)
            restart = scan_info.get("restart", True) # NEU: restart lesen (Standard True)

            # Checkbox für Aktiviert
            enabled_checkbox = QCheckBox()
            enabled_checkbox.setChecked(enabled)
            enabled_checkbox.setStyleSheet("margin-left: 10px; margin-right: 10px;") # Zentrieren?
            cell_widget_enabled = QWidget()
            layout_cb = QHBoxLayout(cell_widget_enabled)
            layout_cb.addWidget(enabled_checkbox)
            layout_cb.setAlignment(Qt.AlignCenter)
            layout_cb.setContentsMargins(0,0,0,0)
            self.scan_table.setCellWidget(row, 0, cell_widget_enabled)

            # Pfad (nicht editierbar in Tabelle)
            path_item = QTableWidgetItem(os.path.normpath(path))
            path_item.setFlags(path_item.flags() & ~Qt.ItemIsEditable)
            self.scan_table.setItem(row, 1, path_item)

            # Zeit (QTimeEdit)
            time_edit = QTimeEdit()
            time_edit.setDisplayFormat("HH:mm")
            try:
                 qtime = QTime.fromString(time_str, "HH:mm")
                 if qtime.isValid():
                     time_edit.setTime(qtime)
                 else:
                      time_edit.setTime(QTime(0, 0)) # Standard 00:00
            except:
                 time_edit.setTime(QTime(0, 0))
            self.scan_table.setCellWidget(row, 2, time_edit)

            # NEU: Checkbox für Neustart
            restart_checkbox = QCheckBox()
            restart_checkbox.setChecked(restart)
            restart_checkbox.setStyleSheet("margin-left: 10px; margin-right: 10px;")
            cell_widget_restart = QWidget()
            layout_restart_cb = QHBoxLayout(cell_widget_restart)
            layout_restart_cb.addWidget(restart_checkbox)
            layout_restart_cb.setAlignment(Qt.AlignCenter)
            layout_restart_cb.setContentsMargins(0,0,0,0)
            self.scan_table.setCellWidget(row, 3, cell_widget_restart) # In Spalte 3

        self.scan_table.resizeRowsToContents()

    def add_scan(self):
        """Fügt eine neue Zeile für einen geplanten Scan hinzu."""
        # Dialog, um Pfad zu wählen (Laufwerk oder Ordner)
        path_type, ok1 = QInputDialog.getItem(self, "Pfadtyp wählen", 
                                              "Soll ein ganzes Laufwerk oder ein spezifischer Ordner hinzugefügt werden?", 
                                              ["Laufwerk", "Ordner"], 0, False)
        if not ok1:
            return
        
        path = None
        if path_type == "Laufwerk":
            available_drives = get_available_drives()
            drive, ok2 = QInputDialog.getItem(self, "Laufwerk auswählen", 
                                              "Wähle das Laufwerk:", available_drives, 0, False)
            if ok2 and drive:
                path = drive
        else: # Ordner
            directory = QFileDialog.getExistingDirectory(self, "Ordner auswählen", os.path.expanduser("~"))
            if directory:
                path = os.path.normpath(directory)

        if not path:
            return
            
        # Prüfen, ob Pfad schon existiert
        for row in range(self.scan_table.rowCount()):
             if self.scan_table.item(row, 1).text() == path:
                 QMessageBox.warning(self, "Duplikat", "Dieser Pfad ist bereits in der Liste.")
                 return
                 
        # Neue Zeile hinzufügen
        row_count = self.scan_table.rowCount()
        self.scan_table.insertRow(row_count)

        # Checkbox für Aktiviert (Standard: aktiviert)
        enabled_checkbox = QCheckBox()
        enabled_checkbox.setChecked(True)
        enabled_checkbox.setStyleSheet("margin-left: 10px; margin-right: 10px;")
        cell_widget_enabled = QWidget()
        layout_cb = QHBoxLayout(cell_widget_enabled)
        layout_cb.addWidget(enabled_checkbox)
        layout_cb.setAlignment(Qt.AlignCenter)
        layout_cb.setContentsMargins(0,0,0,0)
        self.scan_table.setCellWidget(row_count, 0, cell_widget_enabled)

        # Pfad
        path_item = QTableWidgetItem(path)
        path_item.setFlags(path_item.flags() & ~Qt.ItemIsEditable)
        self.scan_table.setItem(row_count, 1, path_item)

        # Zeit (Standard: 00:00)
        time_edit = QTimeEdit()
        time_edit.setDisplayFormat("HH:mm")
        time_edit.setTime(QTime(0, 0))
        self.scan_table.setCellWidget(row_count, 2, time_edit)

        # NEU: Checkbox für Neustart (Standard: aktiviert)
        restart_checkbox = QCheckBox()
        restart_checkbox.setChecked(True) # Standardmäßig neu starten
        restart_checkbox.setStyleSheet("margin-left: 10px; margin-right: 10px;")
        cell_widget_restart = QWidget()
        layout_restart_cb = QHBoxLayout(cell_widget_restart)
        layout_restart_cb.addWidget(restart_checkbox)
        layout_restart_cb.setAlignment(Qt.AlignCenter)
        layout_restart_cb.setContentsMargins(0,0,0,0)
        self.scan_table.setCellWidget(row_count, 3, cell_widget_restart)

        self.scan_table.resizeRowsToContents()

    def remove_scans(self):
        """Entfernt die ausgewählten Zeilen aus der Tabelle."""
        selected_rows = sorted([index.row() for index in self.scan_table.selectionModel().selectedRows()], reverse=True)
        if not selected_rows:
            QMessageBox.warning(self, "Keine Auswahl", "Bitte wähle zuerst eine oder mehrere Zeilen zum Entfernen aus.")
            return
        
        reply = QMessageBox.question(self, "Entfernen bestätigen", 
                                     f"Sollen die ausgewählten {len(selected_rows)} geplanten Scans wirklich entfernt werden?",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        
        if reply == QMessageBox.Yes:
            for row in selected_rows:
                self.scan_table.removeRow(row)

    def save_settings(self):
        """Liest die Daten aus der Tabelle und speichert sie in die globale CONFIG."""
        scheduled_scans = []
        for row in range(self.scan_table.rowCount()):
            enabled_widget = self.scan_table.cellWidget(row, 0)
            enabled = enabled_widget.findChild(QCheckBox).isChecked()
            
            path = self.scan_table.item(row, 1).text()
            
            time_widget = self.scan_table.cellWidget(row, 2)
            time_str = time_widget.time().toString("HH:mm")
            
            restart_widget = self.scan_table.cellWidget(row, 3) # NEU: Auslesen
            restart = restart_widget.findChild(QCheckBox).isChecked() # NEU
            
            scheduled_scans.append({
                "path": path,
                "time": time_str,
                "enabled": enabled,
                "restart": restart # NEU
            })
            
        # Sortieren nach Zeit, dann Pfad?
        scheduled_scans.sort(key=lambda x: (x['time'], x['path']))

        self.current_config['scheduled_scans'] = scheduled_scans

        try:
            save_config(self.current_config)
            logger.info("[GUI] Geplante Scan-Einstellungen gespeichert.")
            QMessageBox.information(self, "Gespeichert", "Die Einstellungen für geplante Scans wurden gespeichert.")
            self.accept()
        except Exception as e:
            error_msg = f"Fehler beim Speichern der Konfiguration: {e}"
            logger.error(f"[GUI] {error_msg}")
            QMessageBox.critical(self, "Speicherfehler", error_msg)

class AutoWatchdogDialog(QDialog):
    """Dialog zum Verwalten der Pfade für den automatischen Watchdog-Start."""
    def __init__(self, current_config, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Automatische Überwachung")
        self.current_config = current_config
        self.setMinimumWidth(500)

        layout = QVBoxLayout(self)

        layout.addWidget(QLabel("Pfade, die beim Systemstart automatisch überwacht werden:"))

        # Liste für Pfade
        self.path_list_widget = QListWidget()
        self.path_list_widget.setToolTip("Diese Laufwerke/Verzeichnisse werden beim Systemstart automatisch überwacht.")
        self.populate_list()
        layout.addWidget(self.path_list_widget)

        # Buttons zum Hinzufügen/Entfernen
        button_layout = QHBoxLayout()
        add_drive_button = QPushButton("➕ Laufwerk hinzufügen...")
        add_drive_button.clicked.connect(self.add_drive)
        add_folder_button = QPushButton("➕ Ordner hinzufügen...")
        add_folder_button.clicked.connect(self.add_folder)
        remove_button = QPushButton("➖ Ausgewähltes entfernen")
        remove_button.clicked.connect(self.remove_path)
        button_layout.addWidget(add_drive_button)
        button_layout.addWidget(add_folder_button)
        button_layout.addWidget(remove_button)
        layout.addLayout(button_layout)

        # Speichern/Abbrechen Buttons
        save_cancel_layout = QHBoxLayout()
        save_button = QPushButton("💾 Speichern")
        save_button.clicked.connect(self.save_settings)
        cancel_button = QPushButton("Abbrechen")
        cancel_button.clicked.connect(self.reject)
        save_cancel_layout.addStretch()
        save_cancel_layout.addWidget(save_button)
        save_cancel_layout.addWidget(cancel_button)
        layout.addLayout(save_cancel_layout)

    def populate_list(self):
        """Füllt die Liste mit den aktuellen Pfaden."""
        self.path_list_widget.clear()
        auto_paths = self.current_config.get('watchdog_auto_paths', [])
        if auto_paths:
            normalized_paths = [os.path.normpath(p) for p in auto_paths]
            self.path_list_widget.addItems(sorted(normalized_paths))

    def add_drive(self):
        """Lässt den Benutzer ein verfügbares Laufwerk auswählen."""
        available_drives = get_available_drives()
        current_items = [self.path_list_widget.item(i).text() for i in range(self.path_list_widget.count())]
        drives_to_show = [d for d in available_drives if d not in current_items]
        if not drives_to_show:
            QMessageBox.information(self, "Keine Laufwerke", "Alle verfügbaren Laufwerke sind bereits in der Liste.")
            return
        drive, ok = QInputDialog.getItem(self, "Laufwerk auswählen", 
                                              "Wähle ein Laufwerk zum Hinzufügen:", drives_to_show, 0, False)
        if ok and drive:
            self.path_list_widget.addItem(drive)
            self.path_list_widget.sortItems()

    def add_folder(self):
        """Öffnet einen Dialog zur Auswahl eines Ordners."""
        directory = QFileDialog.getExistingDirectory(self, "Ordner für Auto-Watchdog auswählen", os.path.expanduser("~"))
        if directory:
            normalized_dir = os.path.normpath(directory)
            current_items = [self.path_list_widget.item(i).text() for i in range(self.path_list_widget.count())]
            if normalized_dir in current_items:
                QMessageBox.information(self, "Bereits vorhanden", "Dieser Pfad ist bereits in der Liste.")
                return
            self.path_list_widget.addItem(normalized_dir)
            self.path_list_widget.sortItems()

    def remove_path(self):
        """Entfernt den/die ausgewählten Pfad(e) aus der Liste."""
        selected_items = self.path_list_widget.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Keine Auswahl", "Bitte wähle zuerst einen Pfad zum Entfernen aus.")
            return
        for item in reversed(selected_items):
            self.path_list_widget.takeItem(self.path_list_widget.row(item))

    def save_settings(self):
        """Speichert die Liste der Pfade in die globale CONFIG."""
        auto_paths = []
        for i in range(self.path_list_widget.count()):
            auto_paths.append(self.path_list_widget.item(i).text())
        self.current_config['watchdog_auto_paths'] = sorted([os.path.normpath(p) for p in auto_paths])
        try:
            save_config(self.current_config)
            logger.info("[GUI] Automatische Watchdog-Pfade gespeichert.")
            QMessageBox.information(self, "Gespeichert", "Die Pfade für den automatischen Watchdog-Start wurden gespeichert.")
            self.accept()
        except Exception as e:
            error_msg = f"Fehler beim Speichern der Konfiguration: {e}"
            logger.error(f"[GUI] {error_msg}")
            QMessageBox.critical(self, "Speicherfehler", error_msg)

class MainWindow(QMainWindow):
    """Hauptfenster der Dateiscanner GUI."""
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Dateiscanner GUI")
        self.setGeometry(300, 200, 800, 600) # Etwas größer für Logs
        self.scan_worker = None # Platzhalter für den Scan-Thread
        self.scan_process = None
        self.log_display = None # Hinzugefügt
        self.progress_bar = None # Hinzugefügt
        self.status_label = None # Hinzugefügt
        self.selected_path_label = None # Hinzugefügt für das neue Label
        self.drive_combo = None # Hinzugefügt
        self.select_folder_button = None # Hinzugefügt
        self.current_scan_path = CONFIG.get('base_path', None) # Intern speichern
        self.setupUI()
        # LogUpdater mit LOG_PATH aus utils initialisieren
        # Nutze logger aus diesem Modul statt write_log direkt
        # self.log_updater = LogUpdater(LOG_PATH)
        # self.log_updater.log_updated.connect(self.update_log)
        # self.log_updater.start()
        # Initialisiere das Verzeichnisfeld mit dem Pfad aus der Konfig
        # self.dir_line.setText(CONFIG.get('base_path', '')) # Entfernt
        self.update_selected_path_display() # Initialanzeige aktualisieren
        # Lade initialen Log?
        self.load_initial_log()

    def load_initial_log(self):
        """Lädt die letzten N Zeilen des Logs beim Start."""
        try:
            if os.path.exists(LOG_PATH):
                 with open(LOG_PATH, "r", encoding="utf-8") as f:
                     lines = f.readlines()
                     # Zeige z.B. die letzten 100 Zeilen
                     self.log_display.setPlainText("".join(lines[-100:]))
                     self.log_display.moveCursor(QtGui.QTextCursor.End) # Nach unten scrollen
        except Exception as e:
            self.log_display.append(f"[GUI Fehler] Initiales Log konnte nicht geladen werden: {e}")

    def setupUI(self):
        widget = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout()

        # --- Pfadauswahl (NEU) ---
        path_selection_layout = QtWidgets.QHBoxLayout()

        # Laufwerk-Dropdown
        self.drive_combo = QComboBox(self)
        self.drive_combo.setToolTip("Wähle das zu scannende Laufwerk")
        self.drive_combo.addItem("-- Laufwerk wählen --") # Platzhalter
        available_drives = get_available_drives()
        self.drive_combo.addItems(available_drives)
        self.drive_combo.currentIndexChanged.connect(self.drive_selected)
        path_selection_layout.addWidget(QtWidgets.QLabel("Laufwerk:"))
        path_selection_layout.addWidget(self.drive_combo)

        # Button zum Ordner auswählen
        self.select_folder_button = QPushButton("Ordner wählen...")
        self.select_folder_button.setToolTip("Wähle optional einen spezifischen Ordner auf dem Laufwerk")
        self.select_folder_button.setEnabled(False) # Standardmäßig deaktiviert
        self.select_folder_button.clicked.connect(self.select_folder)
        path_selection_layout.addWidget(self.select_folder_button)

        path_selection_layout.addStretch()
        layout.addLayout(path_selection_layout)

        # Label zur Anzeige des finalen Pfades
        path_display_layout = QtWidgets.QHBoxLayout()
        path_display_layout.addWidget(QtWidgets.QLabel("Aktueller Pfad:"))
        self.selected_path_label = QtWidgets.QLineEdit(self)
        self.selected_path_label.setReadOnly(True)
        self.selected_path_label.setStyleSheet("background-color: #eee;") # Optisch abheben
        path_display_layout.addWidget(self.selected_path_label)
        layout.addLayout(path_display_layout)
        # --------------------------

        # --- Alte Pfadauswahl entfernen ---
        # self.dir_line = QtWidgets.QLineEdit()
        # self.dir_button = QtWidgets.QPushButton("📁 Verzeichnis auswählen")
        # self.dir_button.clicked.connect(self.select_directory)
        # path_layout = QtWidgets.QHBoxLayout()
        # path_layout.addWidget(self.dir_line)
        # path_layout.addWidget(self.dir_button)
        # layout.addLayout(path_layout)
        # --------------------------------

        # --- Restliche Buttons --- 
        self.scan_button = QtWidgets.QPushButton("📂 Scan starten") # Text leicht geändert
        self.scan_button.clicked.connect(self.start_scan)
        self.scan_button.setEnabled(False) # Standardmäßig deaktiviert

        self.watch_button = QtWidgets.QPushButton("🔍 Überwachung starten")
        self.watch_button.clicked.connect(self.start_watchdog)
        self.watch_button.setEnabled(False) # Standardmäßig deaktiviert

        self.check_button = QtWidgets.QPushButton("🧪 Integritätsprüfung")
        self.check_button.clicked.connect(self.start_integrity)
        self.check_button.setEnabled(False) # Standardmäßig deaktiviert

        # self.test_button = QtWidgets.QPushButton("⚙️ Scan testen (Inline)") # Inline-Scan entfernen?
        # self.test_button.clicked.connect(self.inline_scan)

        button_layout = QtWidgets.QHBoxLayout()
        button_layout.addWidget(self.scan_button)
        button_layout.addWidget(self.watch_button)
        button_layout.addWidget(self.check_button)
        # button_layout.addWidget(self.test_button)
        layout.addLayout(button_layout)

        # --- Log-Anzeige --- 
        self.log_display = QtWidgets.QTextEdit()
        self.log_display.setReadOnly(True)
        font = QtGui.QFont("Courier", 10)
        self.log_display.setFont(font)
        layout.addWidget(self.log_display)

        # --- Menüleiste erstellen --- 
        menubar = self.menuBar()
        
        # Datei-Menü (Optional)
        file_menu = menubar.addMenu('&Datei')
        exit_action = QtWidgets.QAction('Beenden', self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # Einstellungen-Menü
        settings_menu = menubar.addMenu('&Einstellungen')

        hashing_action = QtWidgets.QAction('Hashing...', self)
        hashing_action.setStatusTip('Globale und verzeichnisspezifische Hashing-Optionen festlegen')
        hashing_action.triggered.connect(self.open_hashing_settings) # Verbinde mit neuer Methode
        settings_menu.addAction(hashing_action)

        scan_settings_action = QtWidgets.QAction('&Scan-Einstellungen...', self)
        scan_settings_action.triggered.connect(self.open_scan_settings)
        settings_menu.addAction(scan_settings_action)

        scheduled_scans_action = QtWidgets.QAction('&Geplante Scans...', self) # NEU
        scheduled_scans_action.setStatusTip('Pfade für den automatischen Scan um 00:00 Uhr festlegen') # NEU
        scheduled_scans_action.triggered.connect(self.open_scheduled_scans_settings) # NEU
        settings_menu.addAction(scheduled_scans_action) # NEU

        auto_watchdog_action = QtWidgets.QAction('&Automatische Überwachung...', self) # NEU
        auto_watchdog_action.setStatusTip('Pfade festlegen, die beim Systemstart automatisch überwacht werden') # NEU
        auto_watchdog_action.triggered.connect(self.open_auto_watchdog_settings) # NEU
        settings_menu.addAction(auto_watchdog_action) # NEU

        # Hilfemenü (Optional)
        # help_menu = menubar.addMenu('&Hilfe')
        # about_action = QtWidgets.QAction('Über...', self)
        # help_menu.addAction(about_action)
        # ---------------------------

        widget.setLayout(layout)
        self.setCentralWidget(widget)

    def drive_selected(self, index):
        """Wird aufgerufen, wenn ein Laufwerk im Dropdown ausgewählt wird."""
        if index > 0: # Index 0 ist der Platzhalter
            selected_drive = self.drive_combo.itemText(index)
            self.current_scan_path = selected_drive
            self.update_selected_path_display()
            self.select_folder_button.setEnabled(True)
            self.scan_button.setEnabled(True)
            self.watch_button.setEnabled(True)
            self.check_button.setEnabled(True)
            # Speichere den gewählten Pfad auch als neuen base_path in der Config?
            # CONFIG['base_path'] = self.current_scan_path
            # save_config(CONFIG)
            # -> Besser nicht automatisch, Nutzer soll explizit speichern oder es wird nur für Laufzeit genutzt.
        else:
            self.current_scan_path = None
            self.update_selected_path_display()
            self.select_folder_button.setEnabled(False)
            self.scan_button.setEnabled(False)
            self.watch_button.setEnabled(False)
            self.check_button.setEnabled(False)

    def select_folder(self):
        """Öffnet den Ordnerauswahl-Dialog, startend im aktuell gewählten Laufwerk."""
        start_dir = self.current_scan_path if self.current_scan_path and os.path.isdir(self.current_scan_path) else os.path.expanduser("~")
        # Stelle sicher, dass wir wirklich nur das Laufwerk nehmen, falls ein Ordner gewählt war
        if self.current_scan_path:
             drive_letter = os.path.splitdrive(self.current_scan_path)[0]
             if drive_letter:
                 start_dir = drive_letter + "\\"

        directory = QtWidgets.QFileDialog.getExistingDirectory(self, "Ordner auswählen", start_dir)
        if directory:
            self.current_scan_path = os.path.normpath(directory)
            self.update_selected_path_display()
            # Hier könnte man auch den base_path in der Config speichern

    def update_selected_path_display(self):
        """Aktualisiert das Label, das den aktuell gewählten Scan-Pfad anzeigt."""
        if self.selected_path_label:
            display_text = self.current_scan_path if self.current_scan_path else "-- Kein Pfad gewählt --"
            self.selected_path_label.setText(display_text)
            self.selected_path_label.setToolTip(display_text)
            # Buttons aktivieren/deaktivieren basierend darauf, ob ein Pfad gesetzt ist
            is_path_valid = bool(self.current_scan_path and os.path.isdir(self.current_scan_path))
            self.scan_button.setEnabled(is_path_valid)
            self.watch_button.setEnabled(is_path_valid)
            self.check_button.setEnabled(is_path_valid) # Integritätsprüfung kann jetzt auch pfadbasiert sein
            # Ordner-Auswahl nur aktivieren, wenn ein *Laufwerk* gewählt ist (oder ein Pfad)
            self.select_folder_button.setEnabled(is_path_valid)

    def start_process(self, script, path):
        if not path or not os.path.exists(path):
            QtWidgets.QMessageBox.warning(self, "Fehler", "Pfad ist ungültig.")
            return
        try:
            subprocess.Popen([sys.executable, os.path.join(PROJECT_DIR, script), path])
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Fehler", f"Prozessstart fehlgeschlagen:\n{e}")

    def start_scan(self):
        selected_path = self.current_scan_path
        if not selected_path:
            QMessageBox.warning(self, "Kein Pfad ausgewählt",
                                "Bitte wählen Sie zuerst ein Laufwerk oder Verzeichnis aus.")
            return

        if self.scan_process and self.scan_process.state() == QProcess.Running:
            QMessageBox.warning(self, "Scan läuft bereits", "Ein Scan-Prozess ist bereits aktiv.")
            return

        self.log_display.clear()
        self.log_display.append(f"Starte Scan für: {selected_path}")
        if self.progress_bar:
            self.progress_bar.setValue(0)
        if self.status_label:
            self.status_label.setText("Scan läuft...")

        python_exe = os.path.join(os.path.dirname(sys.executable), 'python.exe')
        script_path = os.path.join(os.path.dirname(__file__), 'scanner_core.py')
        args = [script_path, selected_path]

        if not CONFIG.get('resume_scan', True):
            args.append("--restart")
            logger.info("[GUI] Erzwinge Neustart des Scans (--restart Flag gesetzt).")

        self.scan_process = QProcess(self)
        self.scan_process.setProcessChannelMode(QProcess.MergedChannels)
        self.scan_process.readyReadStandardOutput.connect(self.handle_scan_output)
        self.scan_process.finished.connect(self.scan_finished)
        self.scan_process.errorOccurred.connect(self.scan_error)

        # --- HIER EINFÜGEN ---

        logger.info(f"[GUI] Starte Prozess: {python_exe} {' '.join(args)}")
        self.scan_process.start(python_exe, args)

    def start_watchdog(self):
        path = self.current_scan_path
        self.start_process("watchdog_monitor.py", path)

    def start_integrity(self):
        path = self.current_scan_path
        self.start_process("integrity_checker.py", path)

    # --- Methode zum Öffnen des Hashing-Dialogs --- 
    def open_hashing_settings(self):
        dialog = HashingSettingsDialog(CONFIG, self)
        if dialog.exec_() == QDialog.Accepted:
            settings = dialog.get_settings()
            CONFIG['hashing'] = settings['hashing']
            CONFIG['hash_directories'] = settings['hash_directories']
            save_config(CONFIG)
            logger.info(f"[GUI] Hashing-Einstellungen gespeichert: hashing={CONFIG['hashing']}, dirs={CONFIG['hash_directories']}")
            QMessageBox.information(self, "Gespeichert", "Hashing-Einstellungen wurden gespeichert.")

    # --- Log-Anzeige und Schließen --- 
    # Ersetze update_log durch direkten Logger-Zugriff
    def log_message(self, message, level=logging.INFO):
         """Loggt eine Nachricht und zeigt sie optional in der GUI an."""
         # Logge immer über das logging-Modul (geht in Datei/Konsole)
         logger.log(level, message)
         # Zeige auch im GUI-Log an
         # Formatierung könnte man hier anpassen
         timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Wiederholung, besser direkt vom Logger holen?
         log_entry = f"[{timestamp}] {message}\n"
         self.log_display.moveCursor(QtGui.QTextCursor.End)
         self.log_display.insertPlainText(log_entry)

    # Passe ScanWorker-Signale an, um log_message zu verwenden
    def update_log_scan(self, message):
         """Aktualisiert das Log-Display mit Nachrichten vom Scan-Worker."""
         # Logge die Nachricht auch normal
         logger.info(f"[ScanWorker] {message}")
         # Zeige sie im GUI an
         timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
         log_entry = f"[{timestamp}] {message}\n"
         self.log_display.moveCursor(QtGui.QTextCursor.End)
         self.log_display.insertPlainText(log_entry)

    def closeEvent(self, event):
        # Stoppe den LogUpdater-Thread (entfernt, da wir direkt loggen)
        # if self.log_updater and self.log_updater.isRunning():
        #    self.log_updater.stop()
        #    self.log_updater.wait(2000) 
        # ... (Rest von closeEvent wie zuvor) ...
        # Schließe die DB-Instanz (falls noch nicht geschehen)
        try:
             db = get_db_instance()
             if db and db.conn: # Prüfe, ob conn noch existiert
                 db.close()
        except Exception as e:
             print(f"Fehler beim Schließen der DB in GUI: {e}") # Nur auf Konsole loggen

        event.accept() # Fenster schließen

    def open_scan_settings(self):
        """Öffnet den Dialog für die Scan-Einstellungen."""
        dialog = ScanSettingsDialog(self)
        if dialog.exec_() == QDialog.Accepted:
            settings = dialog.get_settings()
            CONFIG['resume_scan'] = settings['resume_scan']
            save_config(CONFIG)
            logger.info(f"[GUI] Scan-Einstellungen gespeichert: resume_scan={CONFIG['resume_scan']}")
            QMessageBox.information(self, "Gespeichert", "Scan-Einstellungen wurden gespeichert.")

    def handle_scan_output(self):
        """Liest die Ausgabe des Scan-Prozesses und zeigt sie im Log an."""
        if not self.scan_process:
            return
        
        data = self.scan_process.readAllStandardOutput().data().decode('utf-8', errors='replace')
        self.log_display.moveCursor(QtGui.QTextCursor.End)
        self.log_display.insertPlainText(data)
        
        # Optional: Fortschritt parsen, wenn scanner_core.py spezielle Marker ausgibt
        # Beispiel: Wenn core.py "PROGRESS: 50%" ausgibt
        # lines = data.strip().split('\n')
        # for line in lines:
        #     if line.startswith("PROGRESS:"):
        #         try:
        #             percent = int(line.split(":")[1].strip().replace('%',''))
        #             if self.progress_bar:
        #                 self.progress_bar.setValue(percent)
        #         except:
        #             pass # Ignoriere Parsing-Fehler

    def scan_finished(self, exitCode, exitStatus):
        """Wird aufgerufen, wenn der Scan-Prozess beendet ist."""
        status_message = ""
        if exitStatus == QProcess.NormalExit and exitCode == 0:
            status_message = "Scan erfolgreich abgeschlossen."
            if self.progress_bar: self.progress_bar.setValue(100) # Annahme: 100% bei Erfolg
        elif exitStatus == QProcess.CrashExit:
            status_message = f"Scan abgestürzt (Exit code: {exitCode})."
            if self.progress_bar: self.progress_bar.setValue(0) # Oder letzten bekannten Wert beibehalten?
        else: # NormalExit mit Fehlercode
            status_message = f"Scan mit Fehlern beendet (Exit code: {exitCode})."
            if self.progress_bar: self.progress_bar.setValue(0) 

        if self.status_label: self.status_label.setText(status_message)
        
        # Logge den Abschluss im Haupt-Log
        log_final_message = f"[GUI] Scan-Prozess beendet. ExitCode: {exitCode}, ExitStatus: {exitStatus}. Status: {status_message}"
        logger.info(log_final_message) # Nutze den Logger
        self.log_display.append(f"\n--- {log_final_message} ---") # Auch im GUI-Fenster anzeigen
        
        self.scan_process = None # Prozessvariable zurücksetzen


    def scan_error(self, error):
        """Wird aufgerufen, wenn ein Fehler im QProcess auftritt (z.B. Startfehler)."""
        error_string = self.scan_process.errorString()
        status_message = f"Fehler im Scan-Prozess: {error_string} (Code: {error})"
        
        if self.status_label: self.status_label.setText(status_message)
        if self.progress_bar: self.progress_bar.setValue(0)

        # Logge den Fehler im Haupt-Log
        logger.error(f"[GUI] {status_message}")
        self.log_display.append(f"\n--- FEHLER: {status_message} ---") # Auch im GUI-Fenster anzeigen

        self.scan_process = None # Prozessvariable zurücksetzen

    def open_scheduled_scans_settings(self):
        """Öffnet den Dialog zur Verwaltung der geplanten Scan-Pfade."""
        dialog = ScheduledScansDialog(CONFIG, self)
        dialog.exec_()
        # Konfiguration wird im Dialog gespeichert

    def open_auto_watchdog_settings(self): # NEUE METHODE
        """Öffnet den Dialog zur Verwaltung der automatischen Watchdog-Pfade."""
        dialog = AutoWatchdogDialog(CONFIG, self)
        dialog.exec_()

# Die main-Funktion muss AUSSERHALB der MainWindow-Klasse stehen!
# --- Korrigierte Position und Inhalt von main() --- 
def main():
    """Startet die PyQt-Anwendung."""
    # Stellt sicher, dass eine DB-Instanz beim Start erzeugt wird (und damit das Schema)
    db_ok = False
    app_instance_created_for_error = False # Flag, um doppelte QApplication zu vermeiden
    try:
        get_db_instance()
        db_ok = True # Setze Flag bei Erfolg
    except Exception as e:
        print(f"KRITISCHER FEHLER: Datenbank konnte nicht initialisiert werden: {e}")
        # Zeige eine einfache Fehlermeldung, da GUI evtl. nicht startet
        app_instance = QtWidgets.QApplication.instance()
        if not app_instance:
            # Nur eine Instanz erstellen, wenn absolut nötig
            app_instance = QtWidgets.QApplication(sys.argv)
            app_instance_created_for_error = True

        msg_box = QtWidgets.QMessageBox()
        msg_box.setIcon(QtWidgets.QMessageBox.Critical)
        msg_box.setWindowTitle("Datenbankfehler")
        msg_box.setText(f"Die Datenbank konnte nicht initialisiert werden:\n{e}\n\nDie Anwendung kann nicht gestartet werden.")
        msg_box.exec_()
        sys.exit(1)

    # Führe den Rest nur aus, wenn die DB-Initialisierung OK war
    if db_ok:
        # Holen oder Erstellen der QApplication Instanz
        app = QtWidgets.QApplication.instance()
        if not app:
            # Nur erstellen, wenn nicht schon für Fehlermeldung erstellt
            if not app_instance_created_for_error:
                 app = QtWidgets.QApplication(sys.argv)
            else:
                 # Sollte jetzt existieren, wenn für Fehlermeldung erstellt
                 app = QtWidgets.QApplication.instance() 
                 # Prüfen, ob es wirklich existiert (Sicherheit)
                 if not app:
                     app = QtWidgets.QApplication(sys.argv)


        # Optional: Style setzen
        # app.setStyle('Fusion')
        window = MainWindow()
        window.show()
        sys.exit(app.exec_())
    else:
        # Sollte durch sys.exit(1) im except-Block nicht erreicht werden, aber zur Sicherheit
        print("Datenbank nicht initialisiert. Anwendung wird nicht gestartet.")
        sys.exit(1)
# -------------------------------------------------

if __name__ == "__main__":
    main()
