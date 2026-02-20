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
from PyQt5.QtCore import QProcess, pyqtSignal, QThread, QTime, Qt, QTimer
import socket
import threading

# Importiere aus utils
from utils import (
    calculate_hash, HASHING, DB_PATH, CONFIG, # Nutze CONFIG direkt
    PROJECT_DIR, LOG_PATH, save_config, setup_logging, load_config, logger, get_available_drives # get_available_drives importieren
)

# --- Constants ---
SERVICE_NAME = "DateiScannerWatchdog" # Name des Windows-Dienstes

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
        self.setMinimumWidth(800) # Noch breiter für neue Spalte

        layout = QVBoxLayout(self)

        layout.addWidget(QLabel("Scans, die automatisch ausgeführt werden sollen:"))

        # Tabelle für Pfade, Zeiten, Aktivierung, Neustart, Typ
        self.scan_table = QTableWidget()
        self.scan_table.setColumnCount(5) # Erhöht auf 5 Spalten
        self.scan_table.setHorizontalHeaderLabels(["Aktiviert", "Typ", "Pfad", "Zeit (HH:MM)", "Immer neu starten?"])
        self.scan_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.scan_table.verticalHeader().setVisible(False)
        # Spaltenbreiten anpassen
        header = self.scan_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents) # Aktiviert
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents) # Typ
        header.setSectionResizeMode(2, QHeaderView.Stretch) # Pfad
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents) # Zeit
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents) # Neustart

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
            scan_type = scan_info.get("scan_type", "drive") # NEU
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

            # Typ (nicht editierbar in Tabelle)
            if scan_type == "full":
                type_text = "Gesamt"
            elif scan_type == "integrity":
                type_text = "Integritätsprüfung"
            else:
                type_text = "Laufwerk/Ordner"
            type_item = QTableWidgetItem(type_text)
            type_item.setFlags(type_item.flags() & ~Qt.ItemIsEditable)
            self.scan_table.setItem(row, 1, type_item)

            # Pfad (nicht editierbar in Tabelle)
            path_text = os.path.normpath(path) if path else "-- Gesamtscan --"
            path_item = QTableWidgetItem(path_text)
            path_item.setFlags(path_item.flags() & ~Qt.ItemIsEditable)
            self.scan_table.setItem(row, 2, path_item)

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
            self.scan_table.setCellWidget(row, 3, time_edit)

            # NEU: Checkbox für Neustart
            restart_checkbox = QCheckBox()
            restart_checkbox.setChecked(restart)
            restart_checkbox.setStyleSheet("margin-left: 10px; margin-right: 10px;")
            cell_widget_restart = QWidget()
            layout_restart_cb = QHBoxLayout(cell_widget_restart)
            layout_restart_cb.addWidget(restart_checkbox)
            layout_restart_cb.setAlignment(Qt.AlignCenter)
            layout_restart_cb.setContentsMargins(0,0,0,0)
            self.scan_table.setCellWidget(row, 4, cell_widget_restart) # In Spalte 4

        self.scan_table.resizeRowsToContents()

    def add_scan(self):
        """Fügt eine neue Zeile für einen geplanten Scan hinzu."""
        # Dialog, um Pfad zu wählen (Laufwerk oder Ordner)
        scan_type_choice, ok1 = QInputDialog.getItem(self, "Scantyp wählen", 
                                              "Welche Art von Scan soll geplant werden?", 
                                              ["Laufwerk/Ordner scannen", "Gesamter Scan (alle Laufwerke)", "Integritätsprüfung"], 0, False)
        if not ok1:
            return
        
        path = None
        scan_type = "drive" # Standard
        
        if scan_type_choice == "Laufwerk/Ordner scannen":
            path_type, ok_path_type = QInputDialog.getItem(self, "Pfadtyp wählen", 
                                                "Soll ein ganzes Laufwerk oder ein spezifischer Ordner hinzugefügt werden?", 
                                                ["Laufwerk", "Ordner"], 0, False)
            if not ok_path_type:
                return
            
            if path_type == "Laufwerk":
                available_drives = get_available_drives()
                drive, ok_drive = QInputDialog.getItem(self, "Laufwerk auswählen", 
                                              "Wähle das Laufwerk:", available_drives, 0, False)
                if ok_drive and drive:
                    path = drive
            else: # Ordner
                directory = QFileDialog.getExistingDirectory(self, "Ordner auswählen", os.path.expanduser("~"))
                if directory:
                    path = os.path.normpath(directory)

            if not path:
                return
        elif scan_type_choice == "Gesamter Scan (alle Laufwerke)":
            scan_type = "full"
            path = None # Kein spezifischer Pfad für Gesamtscan
        else: # Integritätsprüfung
            scan_type = "integrity"
            # Optional: Pfad für Teil-Integritätsprüfung
            path_choice = QMessageBox.question(self, "Integritätsprüfung",
                                              "Gesamte Datenbank prüfen?",
                                              QMessageBox.Yes | QMessageBox.No)
            if path_choice == QMessageBox.No:
                # Spezifischer Pfad
                directory = QFileDialog.getExistingDirectory(self, "Pfad für Integritätsprüfung", os.path.expanduser("~"))
                if directory:
                    path = os.path.normpath(directory)
                else:
                    return
            else:
                path = None  # Gesamte DB prüfen
            
        # Prüfen, ob Scan schon existiert (Typ und Pfad)
        for row in range(self.scan_table.rowCount()):
             table_type_item = self.scan_table.item(row, 1)
             table_path_item = self.scan_table.item(row, 2)
             # Extrahiere Typ aus dem Text
             if table_type_item.text() == "Gesamt":
                 table_type = "full"
             elif table_type_item.text() == "Integritätsprüfung":
                 table_type = "integrity"
             else:
                 table_type = "drive"
             # Extrahiere Pfad
             table_path_text = table_path_item.text()
             if table_type == "drive" or (table_type == "integrity" and table_path_text not in ["-- Gesamte DB --", "-- Gesamtscan --"]):
                 table_path = table_path_text
             else:
                 table_path = None
             
             if scan_type == table_type:
                 if scan_type == "full": # Nur ein Gesamtscan erlaubt?
                     QMessageBox.warning(self, "Duplikat", "Es kann nur ein Gesamtscan geplant werden.")
                     return
                 elif scan_type == "integrity" and not path and not table_path:
                     QMessageBox.warning(self, "Duplikat", "Es kann nur eine globale Integritätsprüfung geplant werden.")
                     return
                 elif scan_type in ["drive", "integrity"] and path == table_path and path is not None:
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

        # Typ
        if scan_type == "full":
            type_text = "Gesamt"
        elif scan_type == "integrity":
            type_text = "Integritätsprüfung"
        else:
            type_text = "Laufwerk/Ordner"
        type_item = QTableWidgetItem(type_text)
        type_item.setFlags(type_item.flags() & ~Qt.ItemIsEditable)
        self.scan_table.setItem(row_count, 1, type_item)

        # Pfad
        if scan_type == "drive" and path:
            path_text = path
        elif scan_type == "integrity":
            path_text = path if path else "-- Gesamte DB --"
        else:
            path_text = "-- Gesamtscan --"
        path_item = QTableWidgetItem(path_text)
        path_item.setFlags(path_item.flags() & ~Qt.ItemIsEditable)
        self.scan_table.setItem(row_count, 2, path_item)

        # Zeit (Standard: 00:00)
        time_edit = QTimeEdit()
        time_edit.setDisplayFormat("HH:mm")
        time_edit.setTime(QTime(0, 0))
        self.scan_table.setCellWidget(row_count, 3, time_edit)

        # NEU: Checkbox für Neustart (Standard: aktiviert)
        restart_checkbox = QCheckBox()
        # Für Gesamtscans macht --restart Sinn (immer neu aufbauen)
        # Für Laufwerk/Ordner ist es konfigurierbar
        restart_checkbox.setChecked(scan_type != "integrity")
        restart_checkbox.setEnabled(scan_type != "integrity")  # Bei Integrity deaktiviert
        restart_checkbox.setStyleSheet("margin-left: 10px; margin-right: 10px;")
        cell_widget_restart = QWidget()
        layout_restart_cb = QHBoxLayout(cell_widget_restart)
        layout_restart_cb.addWidget(restart_checkbox)
        layout_restart_cb.setAlignment(Qt.AlignCenter)
        layout_restart_cb.setContentsMargins(0,0,0,0)
        self.scan_table.setCellWidget(row_count, 4, cell_widget_restart)

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
            
            type_text = self.scan_table.item(row, 1).text()
            if type_text == "Gesamt":
                scan_type = "full"
            elif type_text == "Integritätsprüfung":
                scan_type = "integrity"
            else:
                scan_type = "drive"
            
            path_text = self.scan_table.item(row, 2).text()
            # Bei Integrity und Drive kann es einen Pfad geben
            if scan_type == "drive" or (scan_type == "integrity" and path_text not in ["-- Gesamte DB --", "-- Gesamtscan --"]):
                path = path_text
            else:
                path = None
            
            time_widget = self.scan_table.cellWidget(row, 3)
            time_str = time_widget.time().toString("HH:mm")
            
            restart_widget = self.scan_table.cellWidget(row, 4) # NEU: Auslesen in Spalte 4
            restart = restart_widget.findChild(QCheckBox).isChecked() # NEU
            
            scheduled_scans.append({
                "scan_type": scan_type,
                "path": path,
                "time": time_str,
                "enabled": enabled,
                "restart": restart # NEU
            })
            
        # Sortieren nach Zeit, dann Typ, dann Pfad?
        scheduled_scans.sort(key=lambda x: (x['time'], x['scan_type'], x.get('path') or ""))

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

class MainWindow(QMainWindow):
    """Hauptfenster der Dateiscanner GUI."""
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Dateiscanner GUI")
        self.setGeometry(300, 200, 850, 650) # Etwas größer
        self.scan_worker = None
        self.scan_process = None
        self.integrity_process = None
        self.log_display = None
        self.progress_bar = None # Hinzugefügt in setupUI? -> Nein, aktuell nicht
        self.status_label = None # Hinzugefügt in setupUI? -> Nein, aktuell nicht
        self.selected_path_label = None
        self.drive_combo = None
        self.select_folder_button = None
        # UI Elemente für Dienststatus
        self.service_status_label = None
        self.service_status_indicator = None
        self.current_scan_path = CONFIG.get('base_path', None)
        
        # Signal für Fensteraktivierung (aus dem SingleInstanceServer)
        self.activate_signal = None
        
        # LogUpdater Instanz
        self.log_updater = None

        self.setupUI() # UI erstellen

        self.update_selected_path_display()
        self.load_initial_log()
        
        # LogUpdater starten
        self.start_log_updater()

        # Timer für Dienststatus-Aktualisierung
        self.status_timer = QTimer(self)
        self.status_timer.timeout.connect(self.update_service_status_display)
        self.status_timer.start(5000) # Prüfe alle 5 Sekunden
        self.update_service_status_display() # Initialprüfung beim Start
        
    # Neuer Slot zur Aktivierung des Fensters
    @QtCore.pyqtSlot()
    def activate_existing_window(self):
        """Aktiviert dieses Fenster und bringt es in den Vordergrund."""
        try:
            logger.info("[GUI Slot] Aktiviere Fenster...")
            if self.isMinimized():
                self.showNormal()
            self.show()
            self.activateWindow()
            self.raise_()
        except Exception as e:
            logger.error(f"[GUI Slot] Fehler beim Aktivieren des Fensters: {e}")

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

    def get_watchdog_service_status(self):
        """Fragt den Status des Windows-Dienstes ab."""
        try:
            # Timeout hinzufügen, um Blockieren zu verhindern
            result = subprocess.run(['sc', 'query', SERVICE_NAME], capture_output=True, text=True, timeout=5, check=False, creationflags=subprocess.CREATE_NO_WINDOW)
            output = result.stdout.lower() # In Kleinbuchstaben für einfacheres Parsen

            if result.returncode != 0 or "failed" in output or "fehler" in output:
                # Prüfen ob Dienst nicht existiert
                if "does not exist" in output or "nicht vorhanden" in output:
                    logger.warning(f"[GUI] Dienst '{SERVICE_NAME}' nicht gefunden.")
                    # Prüfe ob Watchdog als normaler Prozess läuft
                    if self.is_watchdog_process_running():
                        logger.info("[GUI] Watchdog läuft als Prozess (nicht als Dienst)")
                        return "RUNNING_AS_PROCESS"
                    return "NOT_FOUND"
                logger.error(f"[GUI] Fehler beim Abfragen von Dienst '{SERVICE_NAME}'. Exit code: {result.returncode}. Output: {result.stderr or result.stdout}")
                return "ERROR"

            # Suche nach dem STATE
            for line in output.splitlines():
                if "state" in line: # Zeile enthält den Status
                    state_line = line.lower() # Sicherstellen, dass alles klein ist
                    if "running" in state_line:
                        return "RUNNING"
                    elif "paused" in state_line:
                        # Prüfe ob trotz pausiertem Dienst ein Watchdog-Prozess läuft
                        if self.is_watchdog_process_running():
                            logger.info("[GUI] Dienst ist pausiert, aber Watchdog-Prozess läuft")
                            return "RUNNING_AS_PROCESS"
                        return "PAUSED"
                    elif "stopped" in state_line:
                        return "STOPPED"
                    # NEU: Pending-Status erkennen
                    elif "start_pending" in state_line:
                        return "START_PENDING"
                    elif "stop_pending" in state_line:
                        return "STOP_PENDING"
                    elif "continue_pending" in state_line:
                        return "CONTINUE_PENDING"
                    elif "pause_pending" in state_line:
                        return "PAUSE_PENDING"
                    # Fallback, falls ein unerwarteter STATE auftritt
                    logger.warning(f"[GUI] Unbekannter Dienst-STATE in Zeile gefunden: {line.strip()}")
                    return "UNKNOWN" 
            logger.warning(f"[GUI] Konnte Status für Dienst '{SERVICE_NAME}' nicht aus der Ausgabe parsen: {output}")
            return "UNKNOWN" # Status konnte nicht ermittelt werden

        except FileNotFoundError:
            logger.error(f"[GUI] Befehl 'sc' nicht gefunden. Kann Dienststatus nicht prüfen.")
            return "ERROR"
        except subprocess.TimeoutExpired:
            logger.error(f"[GUI] Timeout beim Abfragen von Dienst '{SERVICE_NAME}'.")
            return "ERROR"
        except Exception as e:
            logger.error(f"[GUI] Unerwarteter Fehler beim Abfragen von Dienst '{SERVICE_NAME}': {e}")
            return "ERROR"
    
    def is_watchdog_process_running(self):
        """Prüft ob watchdog_service.py oder watchdog_monitor.py als Prozess läuft."""
        try:
            # Prüfe ob watchdog_service.py oder watchdog_monitor.py läuft
            result = subprocess.run(['wmic', 'process', 'where', 
                                   "name='python.exe' or name='pythonw.exe'", 
                                   'get', 'CommandLine'], 
                                  capture_output=True, text=True, timeout=5, 
                                  creationflags=subprocess.CREATE_NO_WINDOW)
            
            if result.returncode == 0:
                output_lower = result.stdout.lower()
                if 'watchdog_service' in output_lower or 'watchdog_monitor' in output_lower:
                    logger.debug("[GUI] Watchdog-Prozess gefunden via WMIC")
                    return True
                
            # Alternative: Prüfe mit watchdog_control.py status
            control_script = os.path.join(PROJECT_DIR, 'watchdog_control.py')
            if os.path.exists(control_script):
                result = subprocess.run([sys.executable, control_script, 'status'], 
                                      capture_output=True, text=True, timeout=5, 
                                      creationflags=subprocess.CREATE_NO_WINDOW)
                
                if result.returncode == 0 and 'läuft' in result.stdout.lower():
                    logger.debug("[GUI] Watchdog läuft laut watchdog_control.py")
                    return True
                
        except Exception as e:
            logger.debug(f"[GUI] Fehler beim Prüfen des Watchdog-Prozesses: {e}")
        
        return False

    def update_service_status_display(self):
        """Aktualisiert die GUI-Anzeige für den Dienststatus."""
        if not self.service_status_label or not self.service_status_indicator:
            return # UI noch nicht initialisiert

        status = self.get_watchdog_service_status()
        status_text = "Dienststatus: "
        color = "grey" # Standardfarbe
        tooltip = f"Der Status des Hintergrund-Überwachungsdienstes '{SERVICE_NAME}'"

        if status == "RUNNING" or status == "RUNNING_AS_PROCESS":
            if status == "RUNNING_AS_PROCESS":
                status_text += "Läuft (als Prozess)"
                tooltip += " läuft als normaler Prozess (nicht als Windows-Dienst)."
            else:
                status_text += "Läuft"
                tooltip += " ist 'Läuft'."
            color = "lime" # Grün
        elif status == "STOPPED":
            status_text += "Gestoppt"
            color = "red"
            tooltip += " ist 'Gestoppt'."
        elif status == "PAUSED":
            status_text += "Pausiert"
            color = "orange"
            tooltip += " ist 'Pausiert'."
        # NEU: Pending-Status anzeigen
        elif status == "START_PENDING":
            status_text += "Wird gestartet..."
            color = "yellow"
            tooltip += " wird gerade gestartet."
        elif status == "STOP_PENDING":
            status_text += "Wird gestoppt..."
            color = "yellow"
            tooltip += " wird gerade gestoppt."
        elif status == "CONTINUE_PENDING":
            status_text += "Wird fortgesetzt..."
            color = "yellow"
            tooltip += " wird gerade fortgesetzt."
        elif status == "PAUSE_PENDING":
            status_text += "Wird pausiert..."
            color = "yellow"
            tooltip += " wird gerade pausiert."
        # --- Ende Pending-Status ---
        elif status == "NOT_FOUND":
            status_text += "Nicht gefunden"
            color = "grey"
            tooltip = f"Der Dienst '{SERVICE_NAME}' wurde nicht gefunden. Wurde er korrekt installiert?"
        elif status == "ERROR":
            status_text += "Fehler bei Abfrage"
            color = "grey"
            tooltip += " konnte nicht abgefragt werden (siehe Log)."
        else: # UNKNOWN
            status_text += "Unbekannt"
            color = "grey"
            tooltip += " ist unbekannt (siehe Log)."

        self.service_status_label.setText(status_text)
        self.service_status_indicator.setStyleSheet(f"background-color: {color}; border-radius: 5px;")
        self.service_status_label.setToolTip(tooltip)
        self.service_status_indicator.setToolTip(tooltip)

    def get_detailed_scan_status(self):
        """Holt detaillierte Informationen über laufende Scans."""
        try:
            import sqlite3
            import socket
            import datetime
            
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            # Hole aktive Scan-Locks
            cursor.execute("""
                SELECT scan_type, start_time, pid, hostname 
                FROM scan_lock 
                WHERE is_active = 1
                ORDER BY start_time DESC
            """)
            active_scans = cursor.fetchall()
            
            # Hole Scan-Progress Einträge
            cursor.execute("""
                SELECT sp.drive_id, sp.last_path, sp.timestamp, d.name
                FROM scan_progress sp
                LEFT JOIN drives d ON sp.drive_id = d.id
            """)
            progress_entries = cursor.fetchall()
            
            conn.close()
            
            # Formatiere die Ausgabe
            details = []
            
            if active_scans:
                for scan_type, start_time, pid, hostname in active_scans:
                    # Berechne Laufzeit
                    try:
                        start_dt = datetime.datetime.fromisoformat(start_time)
                        runtime = str(datetime.datetime.now() - start_dt).split('.')[0]
                    except:
                        runtime = "Unbekannt"
                    
                    details.append(f"• Typ: {scan_type}")
                    details.append(f"  PID: {pid} @ {hostname}")
                    details.append(f"  Laufzeit: {runtime}")
                    
                    # Prüfe ob PID noch existiert (wenn auf diesem Host)
                    current_hostname = socket.gethostname()
                    if hostname == current_hostname:
                        try:
                            import psutil
                            if not psutil.pid_exists(pid):
                                details.append("  [WARNUNG] Prozess existiert nicht mehr (verwaister Lock)")
                        except ImportError:
                            try:
                                os.kill(pid, 0)
                            except OSError:
                                details.append("  [WARNUNG] Prozess existiert nicht mehr (verwaister Lock)")
            
            if progress_entries:
                details.append("\n[INFO] Scan-Progress:")
                for drive_id, last_path, timestamp, drive_name in progress_entries:
                    path_info = last_path if last_path else "Noch kein Pfad gescannt"
                    details.append(f"  • {drive_name}: {path_info}")
            
            if not details:
                return "Keine detaillierten Informationen verfügbar"
            
            return "\n".join(details)
            
        except Exception as e:
            logger.error(f"[GUI] Fehler beim Abrufen detaillierter Scan-Informationen: {e}")
            return f"Fehler beim Abrufen der Details: {e}"
    
    def open_services_msc(self):
        """Öffnet die Windows-Diensteverwaltung (services.msc) mit erhöhten Rechten (UAC)."""
        try:
            # Verwende PowerShell, um den UAC-Dialog für mmc services.msc auszulösen
            command = "Start-Process mmc -ArgumentList 'services.msc' -Verb RunAs"
            # Starte PowerShell unsichtbar und führe den Befehl aus
            subprocess.Popen(['powershell', '-WindowStyle', 'Hidden', '-Command', command], 
                             creationflags=subprocess.CREATE_NO_WINDOW)
            logger.info("[GUI] Versuch gestartet, services.msc mit erhöhten Rechten zu öffnen.")
        except FileNotFoundError:
            logger.error("[GUI] Fehler: powershell.exe nicht gefunden. Kann Diensteverwaltung nicht starten.")
            QMessageBox.warning(self, "Fehler", "Konnte PowerShell nicht finden, um die Diensteverwaltung zu starten.")
        except Exception as e:
            # Fehler [WinError 740] sollte hier *nicht* mehr auftreten, da die UAC greift.
            # Andere Fehler (z.B. wenn UAC abgelehnt wird) werden nicht direkt hier gefangen.
            logger.error(f"[GUI] Fehler beim Versuch, services.msc via PowerShell zu starten: {e}")
            QMessageBox.warning(self, "Fehler", f"Ein unerwarteter Fehler ist beim Versuch, die Diensteverwaltung zu starten, aufgetreten: {e}")

    def setupUI(self):
        widget = QtWidgets.QWidget()
        main_layout = QtWidgets.QVBoxLayout() # Benenne das Hauptlayout um

        # --- Pfadauswahl ---
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
        main_layout.addLayout(path_selection_layout)

        # Label zur Anzeige des finalen Pfades
        path_display_layout = QtWidgets.QHBoxLayout()
        path_display_layout.addWidget(QtWidgets.QLabel("Aktueller Pfad:"))
        self.selected_path_label = QtWidgets.QLineEdit(self)
        self.selected_path_label.setReadOnly(True)
        self.selected_path_label.setStyleSheet("background-color: #eee;") # Optisch abheben
        path_display_layout.addWidget(self.selected_path_label)
        main_layout.addLayout(path_display_layout)
        # --------------------------

        # --- Buttons ---
        button_layout = QtWidgets.QHBoxLayout()

        self.scan_button = QtWidgets.QPushButton("📂 Scan starten")
        self.scan_button.clicked.connect(self.start_scan)
        self.scan_button.setEnabled(False)
        button_layout.addWidget(self.scan_button)
        
        self.scan_status_button = QtWidgets.QPushButton("📊 Scan-Status")
        self.scan_status_button.clicked.connect(self.show_scan_status)
        self.scan_status_button.setToolTip("Zeigt detaillierte Informationen über laufende Scans")
        button_layout.addWidget(self.scan_status_button)

        self.check_button = QtWidgets.QPushButton("🧪 Integritätsprüfung")
        self.check_button.clicked.connect(self.start_integrity)
        self.check_button.setEnabled(True)  # Immer aktiv — globaler Check möglich
        button_layout.addWidget(self.check_button)

        button_layout.addStretch() # Fügt Platz hinzu, damit Buttons links bleiben
        main_layout.addLayout(button_layout) # Füge Button-Layout zum Hauptlayout hinzu

        # --- Statusanzeige für den Dienst ---
        status_layout = QtWidgets.QHBoxLayout()
        self.service_status_label = QLabel("Dienststatus: Prüfe...")
        self.service_status_indicator = QLabel()
        self.service_status_indicator.setFixedSize(20, 20) # Kleine quadratische Anzeige
        self.service_status_indicator.setStyleSheet("background-color: grey; border-radius: 5px;") # Startet grau, abgerundet
        
        # Button zum Öffnen von services.msc hinzufügen
        self.manage_service_button = QPushButton("Dienst verwalten...")
        self.manage_service_button.setToolTip("Öffnet die Windows-Diensteverwaltung (services.msc)")
        self.manage_service_button.clicked.connect(self.open_services_msc)

        status_layout.addWidget(QLabel("Hintergrundüberwachung:")) # Beschriftung
        status_layout.addWidget(self.service_status_indicator)
        status_layout.addWidget(self.service_status_label)
        status_layout.addWidget(self.manage_service_button) # Button hinzufügen
        status_layout.addStretch()
        main_layout.addLayout(status_layout) # Füge Status-Layout zum Hauptlayout hinzu

        # --- Log-Anzeige ---
        self.log_display = QtWidgets.QTextEdit()
        self.log_display.setReadOnly(True)
        font = QtGui.QFont("Courier", 10)
        self.log_display.setFont(font)
        main_layout.addWidget(self.log_display) # Füge Log zum Hauptlayout hinzu

        # --- Menüleiste ---
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

        widget.setLayout(main_layout) # Setze das Hauptlayout für das zentrale Widget
        self.setCentralWidget(widget)

    def drive_selected(self, index):
        """Wird aufgerufen, wenn ein Laufwerk im Dropdown ausgewählt wird."""
        if index > 0: # Index 0 ist der Platzhalter
            selected_drive = self.drive_combo.itemText(index)
            self.current_scan_path = selected_drive
            self.update_selected_path_display()
            self.select_folder_button.setEnabled(True)
            self.scan_button.setEnabled(True)
        else:
            self.current_scan_path = None
            self.update_selected_path_display()
            self.select_folder_button.setEnabled(False)
            self.scan_button.setEnabled(False)

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
            # check_button bleibt immer aktiv — globaler Check ohne Pfad möglich
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

        # Prüfe, ob Scan bereits läuft
        if self.scan_process and self.scan_process.state() == QProcess.Running:
            QMessageBox.warning(self, "Scan läuft bereits", "Ein Scan-Prozess ist bereits aktiv.")
            return
            
        # Prüfe mit dem DB-Lock-System, ob ein Scan läuft (auch von anderen Prozessen)
        db = get_db_instance()
        if db.is_scan_running():
            # Hole detaillierte Scan-Informationen
            scan_details = self.get_detailed_scan_status()
            
            message = "Es läuft bereits ein Scan-Prozess:\n\n"
            message += scan_details
            message += "\n\nMöchten Sie den Scan trotzdem starten? Dies könnte zu Datenbank-Problemen führen."
            
            reply = QMessageBox.question(self, "Scan läuft bereits", message,
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            if reply == QMessageBox.No:
                logger.info("[GUI] Scan abgebrochen, da bereits ein anderer Scan läuft.")
                # Zeige Scan-Status in der Konsole
                self.log_display.append("\n" + "="*50)
                self.log_display.append("AKTIVER SCAN ERKANNT:")
                self.log_display.append(scan_details)
                self.log_display.append("="*50 + "\n")
                return
            # Wenn Ja, dann force-Flag setzen für scanner_core.py
            force_scan = True
        else:
            force_scan = False
        
        # --- NEU: Prüfe ob Daten existieren und frage nach Scan-Modus ---
        drive_name = os.path.splitdrive(selected_path)[0] + "/"
        db.cursor.execute("SELECT id FROM drives WHERE name = ?", (drive_name,))
        drive_exists = db.cursor.fetchone()
        
        use_restart = False  # Standard: kein --restart
        
        if drive_exists:
            drive_id = drive_exists[0]
            
            # Prüfe ob Fortsetzungspunkt existiert
            resume_point = db.get_last_scan_path(drive_id)
            
            # Zähle vorhandene Daten
            db.cursor.execute("SELECT COUNT(*) FROM files f JOIN directories d ON f.directory_id = d.id WHERE d.drive_id = ?", (drive_id,))
            file_count = db.cursor.fetchone()[0]
            db.cursor.execute("SELECT COUNT(*) FROM directories WHERE drive_id = ?", (drive_id,))
            dir_count = db.cursor.fetchone()[0]
            
            if file_count > 0 or dir_count > 0:
                # Erstelle benutzerdefinierten Dialog
                dialog = QMessageBox(self)
                dialog.setWindowTitle(f"Scan-Modus für {drive_name}")
                dialog.setIcon(QMessageBox.Question)
                
                if resume_point:
                    # Mit Fortsetzungspunkt
                    dialog.setText(f"Vorhandene Daten gefunden: {file_count:,} Dateien, {dir_count:,} Verzeichnisse")
                    dialog.setInformativeText(
                        f"Letzter Scan-Punkt:\n{resume_point}\n\n"
                        "Wie möchten Sie fortfahren?"
                    )
                    
                    fortsetzen_btn = dialog.addButton("Fortsetzen\n(ab letztem Punkt)", QMessageBox.AcceptRole)
                    neustart_btn = dialog.addButton("Neu starten\n(von Anfang, Daten behalten)", QMessageBox.AcceptRole)
                    abbrechen_btn = dialog.addButton("Abbrechen", QMessageBox.RejectRole)
                    
                    dialog.exec_()
                    clicked = dialog.clickedButton()
                    
                    if clicked == fortsetzen_btn:
                        self.log_display.append(f"[INFO] Setze Scan für {drive_name} fort...")
                        use_restart = False
                    elif clicked == neustart_btn:
                        self.log_display.append(f"[INFO] Starte Scan für {drive_name} neu (--restart)...")
                        use_restart = True
                    else:
                        self.log_display.append("[INFO] Scan abgebrochen.")
                        return
                else:
                    # Ohne Fortsetzungspunkt - WARNUNG!
                    dialog.setText(f"⚠️ WARNUNG: Kein Fortsetzungspunkt vorhanden!")
                    dialog.setInformativeText(
                        f"Vorhandene Daten auf {drive_name}: {file_count:,} Dateien, {dir_count:,} Verzeichnisse\n\n"
                        "Ohne Fortsetzungspunkt werden NUR diese Daten von {drive_name} GELÖSCHT!\n"
                        "Andere Laufwerke bleiben unverändert.\n"
                        "Wie möchten Sie fortfahren?"
                    )
                    
                    loeschen_btn = dialog.addButton(f"Löschen & Neu\n({file_count:,} Dateien löschen!)", QMessageBox.DestructiveRole)
                    behalten_btn = dialog.addButton("Behalten & Neu\n(--restart verwenden)", QMessageBox.AcceptRole)
                    abbrechen_btn = dialog.addButton("Abbrechen", QMessageBox.RejectRole)
                    
                    dialog.exec_()
                    clicked = dialog.clickedButton()
                    
                    if clicked == loeschen_btn:
                        # Zusätzliche Sicherheitsabfrage
                        confirm = QMessageBox.critical(
                            self, 
                            "Bestätigung erforderlich",
                            f"Sie sind dabei, {file_count:,} Dateien und {dir_count:,} Verzeichnisse\n"
                            f"NUR für Laufwerk {drive_name} zu LÖSCHEN!\n\n"
                            f"Andere Laufwerke bleiben unverändert.\n"
                            "Sind Sie ABSOLUT SICHER?",
                            QMessageBox.Yes | QMessageBox.No,
                            QMessageBox.No
                        )
                        if confirm == QMessageBox.Yes:
                            self.log_display.append(f"[WARNUNG] Lösche {file_count:,} Dateien und {dir_count:,} Verzeichnisse NUR für {drive_name}...")
                            use_restart = False  # Normaler Scan, der automatisch löscht
                        else:
                            self.log_display.append("[INFO] Löschvorgang abgebrochen.")
                            return
                    elif clicked == behalten_btn:
                        self.log_display.append(f"[INFO] Starte Scan für {drive_name} neu mit --restart (Daten werden behalten)...")
                        use_restart = True
                    else:
                        self.log_display.append("[INFO] Scan abgebrochen.")
                        return
        # --- Ende der neuen Scan-Modus Abfrage ---

        # --- Prüfung und automatische Pausierung des Watchdog ---
        # Prüfe ob Watchdog läuft
        try:
            from watchdog_control import find_watchdog_pid, stop_watchdog
            watchdog_pid = find_watchdog_pid()
            
            if watchdog_pid:
                reply = QMessageBox.question(self, "Watchdog-Service läuft",
                                           f"Der Watchdog-Service läuft (PID: {watchdog_pid}).\n\n"
                                           "Soll der Watchdog automatisch pausiert werden?\n"
                                           "(Empfohlen um 'database is locked' Fehler zu vermeiden)",
                                           QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
                
                if reply == QMessageBox.Yes:
                    self.log_display.append("[INFO] Pausiere Watchdog-Service...")
                    if stop_watchdog():
                        self.log_display.append("[OK] Watchdog-Service pausiert")
                        self.watchdog_was_paused = True
                    else:
                        self.log_display.append("[WARNUNG] Konnte Watchdog nicht pausieren")
                        QMessageBox.warning(self, "Warnung", 
                                          "Watchdog konnte nicht pausiert werden.\n"
                                          "Scan kann zu 'database is locked' Fehlern führen.")
                else:
                    logger.warning("[GUI] Scan wird mit laufendem Watchdog gestartet")
                    self.watchdog_was_paused = False
            else:
                self.watchdog_was_paused = False
                
        except ImportError:
            logger.warning("[GUI] watchdog_control nicht verfügbar")
            self.watchdog_was_paused = False
        except Exception as e:
            logger.error(f"[GUI] Fehler bei Watchdog-Kontrolle: {e}")
            self.watchdog_was_paused = False
        # --- Ende Watchdog-Kontrolle ---

        self.log_display.clear()
        self.log_display.append(f"Starte Scan für: {selected_path}")
        if self.progress_bar:
            self.progress_bar.setValue(0)
        if self.status_label:
            self.status_label.setText("Scan läuft...")

        python_exe = os.path.join(os.path.dirname(sys.executable), 'python.exe')
        script_path = os.path.join(os.path.dirname(__file__), 'scanner_core.py')
        args = [script_path, selected_path]

        # Verwende die Entscheidung aus der Scan-Modus Abfrage
        if use_restart or (not CONFIG.get('resume_scan', True)):
            args.append("--restart")
            logger.info("[GUI] Scan mit --restart Flag gestartet.")
            
        # Force-Flag hinzufügen, wenn nötig
        if force_scan:
            args.append("--force")
            logger.warning("[GUI] Erzwinge Scan trotz laufendem Scan (--force Flag gesetzt).")

        self.scan_process = QProcess(self)
        
        # --- Encoding sicherstellen --- 
        # Erstelle ein QProcessEnvironment-Objekt aus der Systemumgebung
        env = QtCore.QProcessEnvironment.systemEnvironment()
        # Füge die Variable hinzu oder überschreibe sie
        env.insert("PYTHONIOENCODING", "utf-8") 
        # Setze die modifizierte Umgebung für den Prozess
        self.scan_process.setProcessEnvironment(env)
        # --- Ende Encoding --- 
        
        self.scan_process.setProcessChannelMode(QProcess.MergedChannels)
        # Entferne die direkte Verbindung zur stdout-Ausgabe
        # self.scan_process.readyReadStandardOutput.connect(self.handle_scan_output)
        self.scan_process.finished.connect(self.scan_finished)
        self.scan_process.errorOccurred.connect(self.scan_error)

        logger.info(f"[GUI] Starte Prozess: {python_exe} {' '.join(args)}")
        self.scan_process.start(python_exe, args)

    def show_scan_status(self):
        """Zeigt detaillierte Scan-Status Informationen in einem Dialog."""
        try:
            # Hole detaillierte Scan-Informationen
            scan_details = self.get_detailed_scan_status()
            
            # Prüfe ob Scan läuft
            db = get_db_instance()
            is_running = db.is_scan_running()
            
            # Erstelle Dialog
            dialog = QMessageBox(self)
            dialog.setWindowTitle("Scan-Status")
            dialog.setIcon(QMessageBox.Information)
            
            if is_running:
                dialog.setText("[AKTIV] SCAN LÄUFT")
                dialog.setInformativeText(scan_details)
                
                # Füge Button zum Bereinigen verwaister Locks hinzu
                clean_button = dialog.addButton("Verwaiste Locks bereinigen", QMessageBox.ActionRole)
                dialog.addButton(QMessageBox.Ok)
                
                result = dialog.exec_()
                
                # Wenn Bereinigen-Button geklickt wurde
                if dialog.clickedButton() == clean_button:
                    self.clean_orphaned_locks()
            else:
                dialog.setText("[OK] Kein aktiver Scan")
                dialog.setInformativeText("Das System ist bereit für neue Scans.\n\n" + scan_details)
                dialog.setStandardButtons(QMessageBox.Ok)
                dialog.exec_()
                
            # Zeige auch in der Konsole
            self.log_display.append("\n" + "="*50)
            self.log_display.append("SCAN-STATUS:")
            self.log_display.append(scan_details)
            self.log_display.append("="*50 + "\n")
            
        except Exception as e:
            logger.error(f"[GUI] Fehler beim Anzeigen des Scan-Status: {e}")
            QMessageBox.warning(self, "Fehler", f"Fehler beim Abrufen des Scan-Status: {e}")
    
    def clean_orphaned_locks(self):
        """Bereinigt verwaiste Scan-Locks."""
        try:
            # Nutze den scan_status_monitor zum Bereinigen
            script_path = os.path.join(os.path.dirname(__file__), 'scan_status_monitor.py')
            if os.path.exists(script_path):
                result = subprocess.run([sys.executable, script_path, '--clean'], 
                                      capture_output=True, text=True, timeout=5)
                
                if result.returncode == 0:
                    # Parse Ausgabe für Anzahl bereinigter Locks
                    output = result.stdout.strip()
                    self.log_display.append(f"[OK] {output}")
                    QMessageBox.information(self, "Bereinigung erfolgreich", output)
                else:
                    self.log_display.append(f"[FEHLER] Bereinigung fehlgeschlagen: {result.stderr}")
                    QMessageBox.warning(self, "Fehler", "Fehler beim Bereinigen der Locks")
            else:
                # Fallback: Direkte Bereinigung
                db = get_db_instance()
                db.cursor.execute("DELETE FROM scan_progress")
                db.cursor.execute("UPDATE scan_lock SET is_active = 0 WHERE is_active = 1")
                db.conn.commit()
                self.log_display.append("[OK] Scan-Locks und Progress bereinigt")
                QMessageBox.information(self, "Bereinigung erfolgreich", 
                                      "Alle Scan-Locks und Progress-Einträge wurden bereinigt.")
                
        except Exception as e:
            logger.error(f"[GUI] Fehler beim Bereinigen verwaister Locks: {e}")
            QMessageBox.warning(self, "Fehler", f"Fehler beim Bereinigen: {e}")
    
    def start_integrity(self):
        """Startet die Integritaetspruefung via QProcess mit Fortschrittsanzeige."""
        # Prüfe ob bereits eine Integritätsprüfung läuft
        if hasattr(self, 'integrity_process') and self.integrity_process and self.integrity_process.state() == QProcess.Running:
            QMessageBox.warning(self, "Bereits aktiv", "Eine Integritätsprüfung läuft bereits.")
            return

        # Prüfe ob ein Scan läuft (Scan-Lock)
        db = get_db_instance()
        if db.is_scan_running():
            scan_details = self.get_detailed_scan_status()
            message = "Es läuft bereits ein Scan-Prozess:\n\n"
            message += scan_details
            message += "\n\nMöchten Sie die Integritätsprüfung trotzdem starten?"
            reply = QMessageBox.question(self, "Scan läuft", message,
                                         QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            if reply == QMessageBox.No:
                return

        # Dialog: Gesamte DB oder nur gewählten Pfad prüfen?
        integrity_path = None
        if self.current_scan_path and os.path.isdir(self.current_scan_path):
            dialog = QMessageBox(self)
            dialog.setWindowTitle("Integritätsprüfung")
            dialog.setIcon(QMessageBox.Question)
            dialog.setText("Was soll geprüft werden?")
            gesamt_btn = dialog.addButton("Gesamte Datenbank", QMessageBox.AcceptRole)
            pfad_btn = dialog.addButton(f"Nur: {self.current_scan_path}", QMessageBox.AcceptRole)
            abbrechen_btn = dialog.addButton("Abbrechen", QMessageBox.RejectRole)
            dialog.exec_()
            clicked = dialog.clickedButton()
            if clicked == abbrechen_btn:
                return
            elif clicked == pfad_btn:
                integrity_path = self.current_scan_path
            # Bei gesamt_btn: integrity_path bleibt None -> gesamte DB
        # Kein gültiger Pfad gewählt -> automatisch gesamte DB

        # Watchdog pausieren
        self.watchdog_was_paused_integrity = False
        try:
            from watchdog_control import find_watchdog_pid, stop_watchdog
            watchdog_pid = find_watchdog_pid()
            if watchdog_pid:
                reply = QMessageBox.question(self, "Watchdog-Service läuft",
                                             f"Der Watchdog-Service läuft (PID: {watchdog_pid}).\n\n"
                                             "Soll der Watchdog automatisch pausiert werden?\n"
                                             "(Empfohlen um 'database is locked' Fehler zu vermeiden)",
                                             QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
                if reply == QMessageBox.Yes:
                    self.log_display.append("[INFO] Pausiere Watchdog-Service für Integritätsprüfung...")
                    if stop_watchdog():
                        self.log_display.append("[OK] Watchdog-Service pausiert")
                        self.watchdog_was_paused_integrity = True
                    else:
                        self.log_display.append("[WARNUNG] Konnte Watchdog nicht pausieren")
        except ImportError:
            logger.warning("[GUI] watchdog_control nicht verfügbar")
        except Exception as e:
            logger.error(f"[GUI] Fehler bei Watchdog-Kontrolle: {e}")

        # UI vorbereiten
        self.check_button.setEnabled(False)
        self.log_display.append("\n" + "=" * 50)
        if integrity_path:
            self.log_display.append(f"Starte Integritätsprüfung für: {integrity_path}")
        else:
            self.log_display.append("Starte globale Integritätsprüfung (gesamte Datenbank)")
        self.log_display.append("=" * 50)

        # QProcess starten
        python_exe = os.path.join(os.path.dirname(sys.executable), 'python.exe')
        script_path = os.path.join(os.path.dirname(__file__), 'integrity_checker.py')
        args = [script_path]
        if integrity_path:
            args.append(integrity_path)

        self.integrity_process = QProcess(self)

        # Encoding sicherstellen
        env = QtCore.QProcessEnvironment.systemEnvironment()
        env.insert("PYTHONIOENCODING", "utf-8")
        self.integrity_process.setProcessEnvironment(env)

        self.integrity_process.setProcessChannelMode(QProcess.MergedChannels)
        self.integrity_process.readyReadStandardOutput.connect(self.handle_integrity_output)
        self.integrity_process.finished.connect(self.integrity_finished)

        logger.info(f"[GUI] Starte Integritätsprüfung: {python_exe} {' '.join(args)}")
        self.integrity_process.start(python_exe, args)

    def handle_integrity_output(self):
        """Parst die stdout-Ausgabe des Integritäts-Prozesses."""
        if not self.integrity_process:
            return
        raw = self.integrity_process.readAllStandardOutput()
        text = bytes(raw).decode('utf-8', errors='replace')

        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue

            if line.startswith("@@PHASE:"):
                phase = line[8:]
                if phase == "dirs":
                    self.log_display.append("[Integritätsprüfung] Phase: Verzeichnisse prüfen...")
                elif phase == "files":
                    self.log_display.append("[Integritätsprüfung] Phase: Dateien prüfen...")

            elif line.startswith("@@PROGRESS:"):
                parts = line[11:].split(":")
                if len(parts) == 2:
                    try:
                        current = int(parts[0])
                        total = int(parts[1])
                        if total > 0:
                            pct = int(current / total * 100)
                            # Aktualisiere letzte Zeile im Log mit Fortschritt
                            self.log_display.append(f"  Fortschritt: {current:,}/{total:,} ({pct}%)")
                    except ValueError:
                        pass

            elif line.startswith("@@RESULT:"):
                json_str = line[9:]
                try:
                    self._integrity_result = json.loads(json_str)
                except (json.JSONDecodeError, ValueError):
                    logger.warning(f"[GUI] Konnte Integritäts-Ergebnis nicht parsen: {json_str}")

            else:
                # Normale Log-Ausgabe
                self.log_display.append(line)

        # Auto-Scroll
        self.log_display.moveCursor(QtGui.QTextCursor.End)

    def integrity_finished(self, exitCode, exitStatus):
        """Wird aufgerufen, wenn der Integritäts-Prozess beendet ist."""
        self.check_button.setEnabled(True)

        if exitStatus == QProcess.NormalExit and exitCode == 0:
            # Zeige Zusammenfassung
            result = getattr(self, '_integrity_result', None)
            if result:
                summary = (
                    f"Integritätsprüfung abgeschlossen ({result.get('duration', '?')} Sek.)\n\n"
                    f"Geprüft:\n"
                    f"  Verzeichnisse: {result.get('checked_dirs', 0):,}\n"
                    f"  Dateien: {result.get('checked_files', 0):,}\n\n"
                    f"Ergebnis:\n"
                    f"  Fehlende Verzeichnisse: {result.get('missing_dirs', 0):,}\n"
                    f"  Fehlende Dateien: {result.get('missing_files', 0):,}\n"
                    f"  Aktualisierte Dateien: {result.get('updated_files', 0):,}"
                )
                self.log_display.append(f"\n--- {summary.replace(chr(10), chr(10) + '    ')} ---")
                QMessageBox.information(self, "Integritätsprüfung abgeschlossen", summary)
            else:
                self.log_display.append("\n--- Integritätsprüfung abgeschlossen ---")
                QMessageBox.information(self, "Abgeschlossen", "Integritätsprüfung erfolgreich abgeschlossen.")
        elif exitStatus == QProcess.CrashExit:
            self.log_display.append(f"\n--- FEHLER: Integritätsprüfung abgestürzt (Code: {exitCode}) ---")
            QMessageBox.critical(self, "Fehler", f"Integritätsprüfung abgestürzt (Exit code: {exitCode})")
        else:
            self.log_display.append(f"\n--- Integritätsprüfung mit Fehlern beendet (Code: {exitCode}) ---")
            QMessageBox.warning(self, "Fehler", f"Integritätsprüfung mit Fehlern beendet (Exit code: {exitCode})")

        # Watchdog neu starten wenn pausiert
        if hasattr(self, 'watchdog_was_paused_integrity') and self.watchdog_was_paused_integrity:
            try:
                from watchdog_control import start_watchdog
                self.log_display.append("[INFO] Starte Watchdog-Service neu...")
                if start_watchdog():
                    self.log_display.append("[OK] Watchdog-Service wieder gestartet")
                else:
                    self.log_display.append("[WARNUNG] Konnte Watchdog nicht neu starten")
                self.watchdog_was_paused_integrity = False
            except Exception as e:
                logger.error(f"[GUI] Fehler beim Neustarten des Watchdog: {e}")

        self.integrity_process = None
        self._integrity_result = None
        logger.info(f"[GUI] Integritätsprüfung beendet. ExitCode: {exitCode}")

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
        # Stoppe den Timer beim Schließen
        if hasattr(self, 'status_timer') and self.status_timer:
            self.status_timer.stop()
            
        # Stoppe den LogUpdater-Thread
        if self.log_updater and self.log_updater.isRunning():
            self.log_updater.stop()
            self.log_updater.wait(1500) # Warte kurz auf Beendigung
            logger.info("[GUI] LogUpdater gestoppt.")
            
        # Prüfe, ob ein Integritäts-Prozess läuft
        if hasattr(self, 'integrity_process') and self.integrity_process and self.integrity_process.state() == QProcess.Running:
            reply = QMessageBox.question(self, "Integritätsprüfung läuft",
                                 "Eine Integritätsprüfung läuft aktuell. Was möchten Sie tun?",
                                 QMessageBox.Cancel | QMessageBox.Ignore,
                                 QMessageBox.Ignore)
            if reply == QMessageBox.Cancel:
                event.ignore()
                return
            else:
                self.integrity_process.setProcessState(QProcess.NotRunning)
                logger.info("[GUI] GUI wird geschlossen, Integritätsprüfung läuft im Hintergrund weiter.")

        # Prüfe, ob ein Scan-Prozess läuft
        if self.scan_process and self.scan_process.state() == QProcess.Running:
            reply = QMessageBox.question(self, "Scan läuft",
                                 "Ein Scan läuft aktuell. Was möchten Sie tun?",
                                 QMessageBox.Cancel | QMessageBox.Ignore,
                                 QMessageBox.Ignore)
            
            if reply == QMessageBox.Cancel:
                event.ignore()  # Schließen abbrechen
                return
            else:  # Ignore - GUI schließen, Scan weiterlaufen lassen
                # Entkoppeln des Prozesses von der GUI
                self.scan_process.setProcessState(QProcess.NotRunning)
                logger.info("[GUI] GUI wird geschlossen, Scan läuft im Hintergrund weiter.")
        
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
        
        # Starte Watchdog neu wenn er vorher pausiert wurde
        if hasattr(self, 'watchdog_was_paused') and self.watchdog_was_paused:
            try:
                from watchdog_control import start_watchdog
                self.log_display.append("[INFO] Starte Watchdog-Service neu...")
                if start_watchdog():
                    self.log_display.append("[OK] Watchdog-Service wieder gestartet")
                else:
                    self.log_display.append("[WARNUNG] Konnte Watchdog nicht neu starten")
                self.watchdog_was_paused = False
            except Exception as e:
                logger.error(f"[GUI] Fehler beim Neustarten des Watchdog: {e}")
        
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

    # Methode zum Starten des LogUpdaters
    def start_log_updater(self):
        if not self.log_updater:
            self.log_updater = LogUpdater(LOG_PATH, self)
            self.log_updater.log_updated.connect(self.update_log_display)
            self.log_updater.start()
            logger.info("[GUI] LogUpdater gestartet.")
            
    # Slot zum Aktualisieren der Log-Anzeige
    @QtCore.pyqtSlot(str)
    def update_log_display(self, new_log_data):
        if self.log_display:
            # Ans Ende scrollen, nur wenn der Benutzer nicht gerade hochgescrollt hat
            scrollbar = self.log_display.verticalScrollBar()
            scroll_at_bottom = (scrollbar.value() >= (scrollbar.maximum() - 4))
            
            self.log_display.moveCursor(QtGui.QTextCursor.End)
            self.log_display.insertPlainText(new_log_data)
            
            if scroll_at_bottom:
                self.log_display.moveCursor(QtGui.QTextCursor.End)

# Die main-Funktion muss AUSSERHALB der MainWindow-Klasse stehen!
# --- Korrigierte Position und Inhalt von main() --- 
def main():
    """Startet die PyQt-Anwendung."""
    # Single-Instance Mechanismus mit lokalem Server
    import sys
    import os
    import socket
    import threading
    
    # Konstanten
    SINGLE_INSTANCE_PORT = 45982  # Beliebiger Port über 1024
    
    # Nur eine Instanz via Sockets
    def send_activation_to_running_instance():
        """Versucht, eine Verbindung zu einer laufenden Instanz herzustellen und sendet Aktivierungssignal."""
        try:
            # Versuche, eine Verbindung zum Server herzustellen
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.settimeout(2.0)  # 2 Sekunden Timeout
            client_socket.connect(('localhost', SINGLE_INSTANCE_PORT))
            
            # Sende Aktivierungssignal
            client_socket.send(b'ACTIVATE')
            
            # Warte auf Bestätigung mit Timeout
            try:
                response = client_socket.recv(1024)
                if response == b'OK':
                    logger.info("[GUI] Aktivierungssignal bestätigt. Bestehende Instanz informiert.")
                else:
                    logger.warning(f"[GUI] Unerwartete Antwort vom Server: {response}")
            except socket.timeout:
                logger.warning("[GUI] Timeout beim Warten auf Bestätigung vom Server.")
            
            client_socket.close()
            
            # Gib der anderen Instanz Zeit zum Aktivieren des Fensters
            import time
            time.sleep(0.5)
            
            logger.info("[GUI] Aktivierungssignal an laufende Instanz gesendet, diese Instanz wird beendet.")
            return True
        except ConnectionRefusedError:
            logger.info("[GUI] Keine laufende Instanz gefunden (Verbindung verweigert).")
            return False
        except socket.timeout:
            logger.info("[GUI] Timeout beim Verbinden mit laufender Instanz.")
            return False
        except Exception as e:
            logger.error(f"[GUI] Fehler beim Senden des Aktivierungssignals: {e}")
            return False
    
    # Server, der auf Aktivierungssignale von anderen Instanzen hört
    class SingleInstanceServer(QtCore.QObject):
        # Signal, das gesendet wird, wenn das Fenster aktiviert werden soll
        activate_window_signal = QtCore.pyqtSignal()
        
        def __init__(self, window_ref):
            super().__init__() # QObject Konstruktor aufrufen
            self.window_ref = window_ref
            self.running = True
            self.server_socket = None
            self.thread = None
        
        def start(self):
            """Startet den Server-Thread."""
            self.thread = threading.Thread(target=self._server_thread, daemon=True)
            self.thread.start()
        
        def _server_thread(self):
            """Thread-Funktion, die auf Socket-Verbindungen hört."""
            try:
                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # SO_REUSEADDR erlaubt Wiederverwendung des Ports im TIME_WAIT-Status
                self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.server_socket.bind(('localhost', SINGLE_INSTANCE_PORT))
                self.server_socket.listen(5)
                self.server_socket.settimeout(1)  # 1 Sekunde Timeout zum sauberen Beenden
                
                logger.info(f"[GUI] Single-Instance-Server läuft auf Port {SINGLE_INSTANCE_PORT}")
                
                while self.running:
                    try:
                        client, addr = self.server_socket.accept()
                        data = client.recv(1024)
                        if data == b'ACTIVATE':
                            # Sende Bestätigung zurück an Client
                            try:
                                client.send(b'OK')
                            except:
                                pass
                            
                            # Signal aussenden, das im Hauptthread verbunden ist
                            logger.info("[GUI] Aktivierungssignal empfangen, sende Signal an Hauptthread...")
                            self.activate_window_signal.emit()
                            
                        client.close()
                    except socket.timeout:
                        # Timeout ist normal und erlaubt uns, self.running zu prüfen
                        pass
                    except Exception as e:
                        if self.running:  # Nur loggen, wenn wir noch laufen sollen
                            logger.error(f"[GUI] Fehler im Single-Instance-Server: {e}")
            
            except Exception as e:
                logger.error(f"[GUI] Kritischer Fehler beim Starten des Single-Instance-Servers: {e}")
            finally:
                if self.server_socket:
                    self.server_socket.close()
                    logger.info("[GUI] Single-Instance-Server beendet.")
        
        def stop(self):
            """Beendet den Server-Thread sauber."""
            self.running = False
            if self.thread:
                self.thread.join(2)  # Warte bis zu 2 Sekunden auf Beendigung
    
    # Versuche, eine Verbindung herzustellen
    if send_activation_to_running_instance():
        # Eine andere Instanz läuft bereits und wurde aktiviert
        logger.info("[GUI] Bestehende Instanz aktiviert. Diese Instanz wird beendet.")
        sys.exit(0)
    
    # Ab hier sind wir die erste/einzige Instanz
    
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
        
        # Starte den Single-Instance-Server
        server = SingleInstanceServer(window)
        # Verbinde das Signal des Servers mit dem Slot des Fensters
        server.activate_window_signal.connect(window.activate_existing_window)
        server.start()
        
        # Stell sicher, dass der Server bei Beendigung gestoppt wird
        app.aboutToQuit.connect(server.stop)
        
        window.show()
        sys.exit(app.exec_())

if __name__ == "__main__":
    main()
