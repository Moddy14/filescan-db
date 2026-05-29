# -*- coding: utf-8 -*-
"""gui_dialogs.py - PyQt-Dialoge der GUI, ausgelagert aus gui_launcher.py.

Enthaelt HashingSettingsDialog, ScanSettingsDialog und ScheduledScansDialog.
"""
import os

from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QCheckBox, QLabel, QListWidget, QPushButton,
    QFileDialog, QHBoxLayout, QListWidgetItem, QMessageBox, QApplication,
    QMainWindow, QWidget, QProgressBar, QTreeView, QFileSystemModel,
    QTextEdit, QDialogButtonBox, QMenu, QAction, QHeaderView, QComboBox,
    QTableWidget, QTableWidgetItem, QTimeEdit, QAbstractItemView, QInputDialog,
)
from PyQt5.QtCore import Qt, QTime

from utils import save_config, CONFIG, logger, get_available_drives


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
