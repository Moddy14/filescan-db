#!/usr/bin/env python3
import sys
import os
import sqlite3
import shutil
from PyQt5 import QtWidgets, QtCore, QtGui, QtSql
import subprocess
import math
import datetime
import ctypes
import re
from PyQt5.QtCore import QThread, pyqtSignal

class BooleanSearchParser:
    """Parser für Boolean-Suche mit AND, OR, NOT Operatoren"""
    
    def __init__(self):
        # Regex-Pattern für Tokens
        self.token_pattern = re.compile(r'(\bAND\b|\bOR\b|\bNOT\b|\(|\)|"[^"]*"|[^\s()]+)', re.IGNORECASE)
        
    def parse(self, search_string):
        """
        Parst einen Boolean-Suchstring und gibt SQL WHERE-Klausel mit Parametern zurück
        
        Beispiele:
        - "word1 AND word2" -> beide Wörter müssen vorkommen
        - "word1 OR word2" -> eines der Wörter muss vorkommen  
        - "word1 NOT word2" -> word1 ja, aber nicht word2
        - "(word1 OR word2) AND word3" -> Gruppierung mit Klammern
        - '"exact phrase"' -> exakte Phrase in Anführungszeichen
        """
        if not search_string.strip():
            return "", []
            
        tokens = self.tokenize(search_string)
        if not tokens:
            return "", []
            
        try:
            sql_expr, params = self.parse_expression(tokens)
            return sql_expr, params
        except Exception as e:
            # Fallback: einfache LIKE-Suche wenn Boolean-Parsing fehlschlägt
            return "files.filename LIKE ?", [f"%{search_string}%"]
    
    def tokenize(self, text):
        """Tokenisiert den Suchstring"""
        return [token.strip() for token in self.token_pattern.findall(text) if token.strip()]
    
    def parse_expression(self, tokens):
        """Parst eine Boolean-Expression rekursiv"""
        if not tokens:
            return "", []
            
        # Start mit der niedrigsten Priorität (OR)
        return self.parse_or(tokens)
    
    def parse_or(self, tokens):
        """Parst OR-Verknüpfungen (niedrigste Priorität)"""
        left_expr, left_params, tokens = self.parse_and(tokens)
        
        while tokens and tokens[0].upper() == 'OR':
            tokens.pop(0)  # Entferne 'OR'
            right_expr, right_params, tokens = self.parse_and(tokens)
            left_expr = f"({left_expr} OR {right_expr})"
            left_params.extend(right_params)
            
        return left_expr, left_params, tokens
    
    def parse_and(self, tokens):
        """Parst AND-Verknüpfungen (mittlere Priorität)"""
        left_expr, left_params, tokens = self.parse_not(tokens)
        
        while tokens and (tokens[0].upper() == 'AND' or 
                         (tokens[0].upper() not in ['OR', 'AND', 'NOT', ')'] and tokens[0] != ')')):
            # Implizites AND wenn kein Operator angegeben
            if tokens[0].upper() == 'AND':
                tokens.pop(0)  # Entferne 'AND'
            
            right_expr, right_params, tokens = self.parse_not(tokens)
            left_expr = f"({left_expr} AND {right_expr})"
            left_params.extend(right_params)
            
        return left_expr, left_params, tokens
    
    def parse_not(self, tokens):
        """Parst NOT-Negation (höchste Priorität)"""
        if tokens and tokens[0].upper() == 'NOT':
            tokens.pop(0)  # Entferne 'NOT'
            expr, params, tokens = self.parse_primary(tokens)
            return f"NOT ({expr})", params, tokens
        else:
            return self.parse_primary(tokens)
    
    def parse_primary(self, tokens):
        """Parst primäre Ausdrücke (Wörter, Phrasen, Klammern)"""
        if not tokens:
            raise ValueError("Unerwartetes Ende des Suchstrings")
            
        token = tokens.pop(0)
        
        if token == '(':
            # Gruppierte Expression
            expr, params, tokens = self.parse_or(tokens)
            if not tokens or tokens[0] != ')':
                raise ValueError("Fehlende schließende Klammer")
            tokens.pop(0)  # Entferne ')'
            return expr, params, tokens
            
        elif token.startswith('"') and token.endswith('"'):
            # Exakte Phrase (ohne Anführungszeichen)
            phrase = token[1:-1]
            return "files.filename LIKE ?", [f"%{phrase}%"], tokens
            
        else:
            # Einzelnes Wort
            return "files.filename LIKE ?", [f"%{token}%"], tokens

# Helper functions (aus der ursprünglichen Datei übernommen)
def open_file_with_default_app(filepath):
    try:
        if sys.platform.startswith('win'):
            os.startfile(filepath)
        elif sys.platform.startswith('darwin'):
            os.system(f"open '{filepath}'")
        else:
            os.system(f"xdg-open '{filepath}'")
    except Exception as e:
        QtWidgets.QMessageBox.critical(None, "Fehler", f"Datei konnte nicht geöffnet werden: {e}")

def open_folder_in_explorer(folder_path, as_admin=False):
    """Öffnet den gegebenen Ordner im nativen Dateimanager."""
    try:
        if sys.platform.startswith('win'):
            if as_admin:
                result = ctypes.windll.shell32.ShellExecuteW(None, "runas", "explorer.exe", folder_path, None, 1)
                if int(result) <= 32:
                    error_msg = f"Explorer konnte nicht als Admin gestartet werden (Fehlercode: {result})."
                    QtWidgets.QMessageBox.warning(None, "Fehler bei Admin-Start", error_msg)
            else:
                os.startfile(folder_path)
        elif sys.platform.startswith('darwin'):
            subprocess.run(['open', folder_path], check=True)
        else:
            subprocess.run(['xdg-open', folder_path], check=True)
    except Exception as e:
        QtWidgets.QMessageBox.critical(None, "Fehler", f"Ordner konnte nicht im Explorer geöffnet werden: {e}")

class NumericTableWidgetItem(QtWidgets.QTableWidgetItem):
    def __init__(self, value, display_text=None):
        super().__init__(display_text if display_text is not None else str(value))
        self.numeric_value = value

    def __lt__(self, other):
        try:
            other_value = float(other.numeric_value if isinstance(other, NumericTableWidgetItem) else other.text())
            return self.numeric_value < other_value
        except (ValueError, TypeError, AttributeError):
            return super().__lt__(other)

class EnhancedSearchWorker(QThread):
    finished = pyqtSignal(list, int, str)  # results, total_count, query_info

    def __init__(self, db_path, search_criteria):
        super().__init__()
        self.db_path = db_path
        self.search_criteria = search_criteria

    def run(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Basis-Query für optimierte Datenbankstruktur
            base_query = """
                SELECT 
                    files.id,
                    drives.name as drive_name,
                    directories.full_path,
                    files.filename,
                    COALESCE(extensions.name, '[none]') as extension,
                    extensions.category,
                    files.size,
                    files.hash,
                    files.created_date,
                    files.modified_date,
                    directories.full_path || '/' || files.filename || COALESCE(extensions.name, '') as full_file_path
                FROM files
                JOIN directories ON files.directory_id = directories.id
                JOIN drives ON directories.drive_id = drives.id
                LEFT JOIN extensions ON files.extension_id = extensions.id
            """
            
            # WHERE-Klauseln und Parameter aufbauen
            where_clauses = []
            params = []
            
            # Drive-Filter
            if self.search_criteria.get('drives'):
                drive_placeholders = ','.join('?' * len(self.search_criteria['drives']))
                where_clauses.append(f"drives.name IN ({drive_placeholders})")
                params.extend(self.search_criteria['drives'])
            
            # Pfad-Filter
            if self.search_criteria.get('path_filter'):
                path_filter = self.search_criteria['path_filter'].replace('*', '%')
                where_clauses.append("directories.full_path LIKE ?")
                params.append(f"%{path_filter}%")
            
            # Dateiname-Filter aus strukturierter Suche
            if self.search_criteria.get('filename_sql'):
                filename_sql = self.search_criteria['filename_sql']
                filename_params = self.search_criteria.get('filename_params', [])
                where_clauses.append(filename_sql)
                params.extend(filename_params)
            
            # Extension-Filter
            if self.search_criteria.get('extensions'):
                ext_placeholders = ','.join('?' * len(self.search_criteria['extensions']))
                where_clauses.append(f"COALESCE(extensions.name, '[none]') IN ({ext_placeholders})")
                params.extend(self.search_criteria['extensions'])
            
            # Kategorie-Filter
            if self.search_criteria.get('categories'):
                cat_placeholders = ','.join('?' * len(self.search_criteria['categories']))
                where_clauses.append(f"extensions.category IN ({cat_placeholders})")
                params.extend(self.search_criteria['categories'])
            
            # Größen-Filter
            if self.search_criteria.get('size_filter'):
                size_op = self.search_criteria['size_filter']['operator']
                size_val1 = self.search_criteria['size_filter']['value1']
                size_val2 = self.search_criteria['size_filter'].get('value2')
                
                if size_op == '>':
                    where_clauses.append("files.size > ?")
                    params.append(size_val1)
                elif size_op == '<':
                    where_clauses.append("files.size < ?")
                    params.append(size_val1)
                elif size_op == '=':
                    where_clauses.append("files.size = ?")
                    params.append(size_val1)
                elif size_op == 'between' and size_val2:
                    where_clauses.append("files.size BETWEEN ? AND ?")
                    params.extend([min(size_val1, size_val2), max(size_val1, size_val2)])
            
            # Hash-Filter (für Duplikate)
            if self.search_criteria.get('has_hash'):
                if self.search_criteria['has_hash']:
                    where_clauses.append("files.hash IS NOT NULL AND files.hash != ''")
                else:
                    where_clauses.append("(files.hash IS NULL OR files.hash = '')")
            
            # Duplikat-Filter
            if self.search_criteria.get('show_duplicates'):
                where_clauses.append("""
                    files.hash IN (
                        SELECT hash FROM files 
                        WHERE hash IS NOT NULL AND hash != '' 
                        GROUP BY hash 
                        HAVING COUNT(*) > 1
                    )
                """)
            
            # Fertige Query zusammenbauen
            if where_clauses:
                query = base_query + " WHERE " + " AND ".join(where_clauses)
            else:
                query = base_query
                
            # Sortierung hinzufügen
            order_by = self.search_criteria.get('order_by', 'full_file_path')
            order_dir = self.search_criteria.get('order_direction', 'ASC')
            query += f" ORDER BY {order_by} {order_dir}"
            
            # Limit hinzufügen (für Performance bei großen Ergebnissen)
            limit = self.search_criteria.get('limit', 10000)
            query += f" LIMIT {limit}"
            
            # Query ausführen
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Zähle Gesamtergebnisse (ohne LIMIT)
            count_query = query.replace(base_query, "SELECT COUNT(*) FROM files JOIN directories ON files.directory_id = directories.id JOIN drives ON directories.drive_id = drives.id LEFT JOIN extensions ON files.extension_id = extensions.id")
            count_query = count_query.split('ORDER BY')[0]  # Entferne ORDER BY und LIMIT
            count_query = count_query.split('LIMIT')[0]
            cursor.execute(count_query, params)
            total_count = cursor.fetchone()[0]
            
            conn.close()
            
            # Query-Info für Debugging
            query_info = f"Parameters: {params} | Total: {total_count} | Shown: {len(results)}"
            
            self.finished.emit(results, total_count, query_info)
            
        except Exception as e:
            self.finished.emit([], 0, f"Error: {str(e)}")

class EnhancedMainWindow(QtWidgets.QMainWindow):
    def __init__(self, db_path):
        super().__init__()
        self.setWindowTitle("Erweiterte Dateisuche - Optimierte DB-Struktur")
        self.resize(1400, 900)
        self.db_path = db_path
        self.conn = None
        self.worker = None
        self.connect_db()
        self.icon_provider = QtWidgets.QFileIconProvider()
        
        # Cache für UI-Daten
        self.available_drives = []
        self.available_extensions = []
        self.available_categories = []
        
        self.setup_ui()
        self.load_ui_data()
        self._apply_stylesheet()

    def connect_db(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
        except sqlite3.Error as e:
            QtWidgets.QMessageBox.critical(self, "DB Fehler", f"Fehler beim Verbinden zur DB: {e}")
            sys.exit(1)

    def load_ui_data(self):
        """Lädt verfügbare Drives, Extensions und Kategorien für UI-Elemente"""
        cursor = self.conn.cursor()
        
        # Lade verfügbare Drives
        cursor.execute("SELECT DISTINCT name FROM drives ORDER BY name")
        self.available_drives = [row[0] for row in cursor.fetchall()]
        
        # Lade verfügbare Extensions
        cursor.execute("SELECT DISTINCT name FROM extensions WHERE name != '[none]' ORDER BY name")
        self.available_extensions = [row[0] for row in cursor.fetchall()]
        
        # Lade verfügbare Kategorien
        cursor.execute("SELECT DISTINCT category FROM extensions WHERE category IS NOT NULL ORDER BY category")
        self.available_categories = [row[0] for row in cursor.fetchall()]
        
        # Update UI-Elemente
        self.update_ui_lists()

    def update_ui_lists(self):
        """Aktualisiert die Dropdown-Listen mit den geladenen Daten"""
        # Drives
        self.drive_list.clear()
        for drive in self.available_drives:
            item = QtWidgets.QListWidgetItem(drive)
            item.setFlags(item.flags() | QtCore.Qt.ItemIsUserCheckable)
            item.setCheckState(QtCore.Qt.Unchecked)
            self.drive_list.addItem(item)
        
        # Extensions - Update nur Completer, keine Liste mehr
        if hasattr(self, 'extension_completer'):
            model = QtCore.QStringListModel(self.available_extensions)
            self.extension_completer.setModel(model)
        
        # Categories
        self.category_list.clear()
        for cat in self.available_categories:
            item = QtWidgets.QListWidgetItem(cat.title())
            item.setFlags(item.flags() | QtCore.Qt.ItemIsUserCheckable)
            item.setCheckState(QtCore.Qt.Unchecked)
            self.category_list.addItem(item)

    def setup_ui(self):
        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
        
        # Hauptlayout
        main_layout = QtWidgets.QVBoxLayout()
        
        # Filter-Tabs
        filter_tabs = QtWidgets.QTabWidget()
        
        # --- Tab 1: Grundlegende Filter ---
        basic_tab = QtWidgets.QWidget()
        basic_layout = QtWidgets.QFormLayout()
        
        # Pfad-Filter
        self.path_input = QtWidgets.QLineEdit()
        self.path_input.setPlaceholderText("z.B. *Documents* oder C:/Music/* (* als Wildcard)")
        basic_layout.addRow("Pfad-Filter:", self.path_input)
        
        # Dateiname-Filter mit strukturierter Boolean-Suche
        filename_layout = QtWidgets.QVBoxLayout()
        
        # Container für Suchbegriffe
        self.search_terms_layout = QtWidgets.QVBoxLayout()
        self.search_terms = []  # Liste der Suchbegriff-Widgets
        
        # Erstes Suchfeld hinzufügen
        self.add_search_term()
        
        # Buttons für Verwaltung der Suchbegriffe
        buttons_layout = QtWidgets.QHBoxLayout()
        add_term_btn = QtWidgets.QPushButton("+ Begriff hinzufügen")
        add_term_btn.clicked.connect(self.add_search_term)
        clear_terms_btn = QtWidgets.QPushButton("Alle löschen")
        clear_terms_btn.clicked.connect(self.clear_search_terms)
        buttons_layout.addWidget(add_term_btn)
        buttons_layout.addWidget(clear_terms_btn)
        buttons_layout.addStretch()
        
        # Hilfetext
        help_label = QtWidgets.QLabel(
            "<small><b>Strukturierte Suche:</b> Jeder Begriff wird mit dem gewählten Operator verknüpft.<br/>"
            "<b>Wildcards:</b> Verwende * für beliebige Zeichen, z.B. <code>*.pdf</code> oder <code>backup*</code><br/>"
            "<b>Operatoren:</b> AND (beide müssen vorkommen), OR (eines muss vorkommen), NOT (darf nicht vorkommen)</small>"
        )
        help_label.setWordWrap(True)
        help_label.setStyleSheet("QLabel { color: #666; margin: 5px 0; }")
        
        filename_layout.addLayout(self.search_terms_layout)
        filename_layout.addLayout(buttons_layout)
        filename_layout.addWidget(help_label)
        
        filename_widget = QtWidgets.QWidget()
        filename_widget.setLayout(filename_layout)
        basic_layout.addRow("Dateiname:", filename_widget)
        
        basic_tab.setLayout(basic_layout)
        filter_tabs.addTab(basic_tab, "🔍 Grundfilter")
        
        # --- Tab 2: Laufwerke & Pfade ---
        location_tab = QtWidgets.QWidget()
        location_layout = QtWidgets.QVBoxLayout()
        
        # Laufwerke
        location_layout.addWidget(QtWidgets.QLabel("🗄️ Laufwerke (mehrere auswählbar):"))
        self.drive_list = QtWidgets.QListWidget()
        self.drive_list.setMaximumHeight(100)
        self.drive_list.setSelectionMode(QtWidgets.QAbstractItemView.MultiSelection)
        location_layout.addWidget(self.drive_list)
        
        # Schnellauswahl für Laufwerke
        drive_buttons_layout = QtWidgets.QHBoxLayout()
        self.select_all_drives_btn = QtWidgets.QPushButton("Alle auswählen")
        self.select_no_drives_btn = QtWidgets.QPushButton("Alle abwählen")
        self.select_all_drives_btn.clicked.connect(lambda: self.toggle_all_items(self.drive_list, True))
        self.select_no_drives_btn.clicked.connect(lambda: self.toggle_all_items(self.drive_list, False))
        drive_buttons_layout.addWidget(self.select_all_drives_btn)
        drive_buttons_layout.addWidget(self.select_no_drives_btn)
        drive_buttons_layout.addStretch()
        location_layout.addLayout(drive_buttons_layout)
        
        location_tab.setLayout(location_layout)
        filter_tabs.addTab(location_tab, "📂 Laufwerke")
        
        # --- Tab 3: Dateitypen ---
        filetype_tab = QtWidgets.QWidget()
        filetype_layout = QtWidgets.QVBoxLayout()
        
        # Kategorien
        filetype_layout.addWidget(QtWidgets.QLabel("🏷️ Datei-Kategorien:"))
        self.category_list = QtWidgets.QListWidget()
        self.category_list.setMaximumHeight(120)
        filetype_layout.addWidget(self.category_list)
        
        # Extensions - NEUES DESIGN mit Textfeld statt Liste
        extension_input_layout = QtWidgets.QVBoxLayout()
        extension_input_layout.addWidget(QtWidgets.QLabel("📄 Dateiendungen (kommagetrennt, z.B. .json,.xml,.txt):"))
        
        # Eingabefeld für Extensions
        self.extension_input = QtWidgets.QLineEdit()
        self.extension_input.setPlaceholderText("z.B. .json, .xml, .config oder leer für alle")
        
        # Auto-Vervollständigung für Extensions
        self.extension_completer = QtWidgets.QCompleter(self.available_extensions)
        self.extension_completer.setCaseSensitivity(QtCore.Qt.CaseInsensitive)
        self.extension_completer.setFilterMode(QtCore.Qt.MatchContains)
        self.extension_input.setCompleter(self.extension_completer)
        
        extension_input_layout.addWidget(self.extension_input)
        
        # Info-Label für verfügbare Extensions
        self.extension_info_label = QtWidgets.QLabel()
        self.extension_info_label.setWordWrap(True)
        self.extension_info_label.setStyleSheet("QLabel { color: #666; font-size: 10px; }")
        self.update_extension_info()
        extension_input_layout.addWidget(self.extension_info_label)
        
        filetype_layout.addLayout(extension_input_layout)
        
        # Schnellauswahl Buttons
        type_buttons_layout = QtWidgets.QHBoxLayout()
        common_extensions = [
            ("Dokumente", ['.pdf', '.doc', '.docx', '.txt']),
            ("Bilder", ['.jpg', '.jpeg', '.png', '.gif']),
            ("Videos", ['.mp4', '.avi', '.mkv', '.mov']),
            ("Audio", ['.mp3', '.wav', '.flac', '.m4a']),
            ("Archive", ['.zip', '.rar', '.7z', '.tar'])
        ]
        
        for name, extensions in common_extensions:
            btn = QtWidgets.QPushButton(name)
            btn.clicked.connect(lambda checked, exts=extensions: self.select_extensions(exts))
            type_buttons_layout.addWidget(btn)
        
        filetype_layout.addLayout(type_buttons_layout)
        filetype_tab.setLayout(filetype_layout)
        filter_tabs.addTab(filetype_tab, "🗂️ Dateitypen")
        
        # --- Tab 4: Erweiterte Filter ---
        advanced_tab = QtWidgets.QWidget()
        advanced_layout = QtWidgets.QFormLayout()
        
        # Größenfilter
        size_layout = QtWidgets.QHBoxLayout()
        self.size_operator = QtWidgets.QComboBox()
        self.size_operator.addItems(['>', '<', '=', 'zwischen'])
        self.size_value1 = QtWidgets.QDoubleSpinBox()
        self.size_value1.setRange(0, 999999999)
        self.size_value1.setDecimals(2)
        self.size_unit = QtWidgets.QComboBox()
        self.size_unit.addItems(['Bytes', 'KB', 'MB', 'GB'])
        self.size_value2 = QtWidgets.QDoubleSpinBox()
        self.size_value2.setRange(0, 999999999)
        self.size_value2.setDecimals(2)
        self.size_value2.setVisible(False)
        
        size_layout.addWidget(self.size_operator)
        size_layout.addWidget(self.size_value1)
        size_layout.addWidget(self.size_unit)
        size_layout.addWidget(QtWidgets.QLabel("bis"))
        size_layout.addWidget(self.size_value2)
        size_layout.addStretch()
        
        self.size_operator.currentTextChanged.connect(
            lambda text: self.size_value2.setVisible(text == 'zwischen')
        )
        
        advanced_layout.addRow("📏 Dateigröße:", size_layout)
        
        # Hash-Optionen
        hash_layout = QtWidgets.QHBoxLayout()
        self.show_only_hashed = QtWidgets.QCheckBox("Nur Dateien mit Hash")
        self.show_duplicates = QtWidgets.QCheckBox("Nur Duplikate anzeigen")
        hash_layout.addWidget(self.show_only_hashed)
        hash_layout.addWidget(self.show_duplicates)
        hash_layout.addStretch()
        advanced_layout.addRow("🔐 Hash-Filter:", hash_layout)
        
        # Sortierung
        sort_layout = QtWidgets.QHBoxLayout()
        self.sort_by = QtWidgets.QComboBox()
        self.sort_by.addItems(['full_file_path', 'files.size', 'files.filename', 'directories.full_path', 'extensions.category'])
        self.sort_direction = QtWidgets.QComboBox()
        self.sort_direction.addItems(['ASC', 'DESC'])
        sort_layout.addWidget(self.sort_by)
        sort_layout.addWidget(self.sort_direction)
        sort_layout.addStretch()
        advanced_layout.addRow("📊 Sortierung:", sort_layout)
        
        # Limit
        self.result_limit = QtWidgets.QSpinBox()
        self.result_limit.setRange(100, 100000)
        self.result_limit.setValue(10000)
        self.result_limit.setSuffix(" Ergebnisse")
        advanced_layout.addRow("🚦 Max. Ergebnisse:", self.result_limit)
        
        advanced_tab.setLayout(advanced_layout)
        filter_tabs.addTab(advanced_tab, "⚙️ Erweitert")
        
        main_layout.addWidget(filter_tabs)
        
        # Suchbuttons
        search_buttons_layout = QtWidgets.QHBoxLayout()
        self.search_button = QtWidgets.QPushButton("🔍 Erweiterte Suche starten")
        self.search_button.setStyleSheet("QPushButton { font-weight: bold; padding: 10px; }")
        self.clear_button = QtWidgets.QPushButton("🗑️ Filter zurücksetzen")
        self.save_search_button = QtWidgets.QPushButton("💾 Suche speichern")
        
        self.search_button.clicked.connect(self.start_search)
        self.clear_button.clicked.connect(self.clear_filters)
        
        search_buttons_layout.addWidget(self.search_button)
        search_buttons_layout.addWidget(self.clear_button)
        search_buttons_layout.addWidget(self.save_search_button)
        search_buttons_layout.addStretch()
        main_layout.addLayout(search_buttons_layout)
        
        # Ergebnis-Tabelle
        self.table = QtWidgets.QTableWidget()
        self.table.setColumnCount(9)
        self.table.setHorizontalHeaderLabels([
            "", "Laufwerk", "Pfad", "Dateiname", "Extension", 
            "Kategorie", "Größe", "Hash", "Änderungsdatum"
        ])
        self.table.setSelectionBehavior(QtWidgets.QTableWidget.SelectRows)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        self.table.setEditTriggers(QtWidgets.QTableWidget.NoEditTriggers)
        self.table.setSortingEnabled(True)
        self.table.setColumnWidth(0, 30)  # Icon-Spalte
        self.table.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)
        main_layout.addWidget(self.table)
        
        # Aktionsbuttons
        action_buttons_layout = QtWidgets.QHBoxLayout()
        self.open_button = QtWidgets.QPushButton("📂 Öffnen")
        self.explorer_button = QtWidgets.QPushButton("🗂️ Im Explorer")
        self.copy_path_button = QtWidgets.QPushButton("📋 Pfad kopieren")
        self.export_button = QtWidgets.QPushButton("📤 Export")
        
        self.open_button.clicked.connect(self.open_selected)
        self.explorer_button.clicked.connect(self.open_in_explorer)
        self.copy_path_button.clicked.connect(self.copy_paths)
        self.export_button.clicked.connect(self.export_results)
        
        action_buttons_layout.addWidget(self.open_button)
        action_buttons_layout.addWidget(self.explorer_button)
        action_buttons_layout.addWidget(self.copy_path_button)
        action_buttons_layout.addWidget(self.export_button)
        action_buttons_layout.addStretch()
        main_layout.addLayout(action_buttons_layout)
        
        # Statusleiste
        self.status_label = QtWidgets.QLabel("Bereit für erweiterte Suche")
        main_layout.addWidget(self.status_label)
        
        central_widget.setLayout(main_layout)

    def _apply_stylesheet(self):
        """Wendet ein verbessertes Stylesheet an"""
        qss = """
            QMainWindow {
                background-color: #f5f5f5;
            }
            QTabWidget::pane {
                border: 1px solid #c0c0c0;
                background-color: white;
            }
            QTabBar::tab {
                background-color: #e1e1e1;
                padding: 8px 16px;
                margin: 2px;
                border-radius: 4px;
            }
            QTabBar::tab:selected {
                background-color: #4CAF50;
                color: white;
                font-weight: bold;
            }
            QPushButton {
                padding: 8px 16px;
                border: 1px solid #ccc;
                border-radius: 4px;
                background-color: #f0f0f0;
            }
            QPushButton:hover {
                background-color: #e0e0e0;
                border-color: #999;
            }
            QPushButton:pressed {
                background-color: #d0d0d0;
            }
            QListWidget {
                border: 1px solid #ccc;
                border-radius: 4px;
                background-color: white;
            }
            QTableWidget {
                border: 1px solid #ccc;
                gridline-color: #ddd;
                background-color: white;
                alternate-background-color: #f9f9f9;
            }
            QHeaderView::section {
                background-color: #e8e8e8;
                padding: 8px;
                border: 1px solid #ccc;
                font-weight: bold;
            }
        """
        self.setStyleSheet(qss)

    def toggle_all_items(self, list_widget, check_state):
        """Wählt alle/keine Items in einer QListWidget aus"""
        for i in range(list_widget.count()):
            item = list_widget.item(i)
            item.setCheckState(QtCore.Qt.Checked if check_state else QtCore.Qt.Unchecked)

    def select_extensions(self, extensions):
        """Setzt spezifische Extensions ins Eingabefeld"""
        self.extension_input.setText(", ".join(extensions))
    
    def update_extension_info(self):
        """Zeigt Info über verfügbare Extensions"""
        if hasattr(self, 'extension_info_label'):
            count = len(self.available_extensions)
            if count > 0:
                # Zeige nur die ersten 20 häufigsten Extensions als Beispiel
                cursor = self.conn.cursor()
                cursor.execute("""
                    SELECT e.name, COUNT(f.id) as count 
                    FROM extensions e 
                    JOIN files f ON e.id = f.extension_id 
                    WHERE e.name != '[none]'
                    GROUP BY e.id 
                    ORDER BY count DESC 
                    LIMIT 20
                """)
                popular_exts = [row[0] for row in cursor.fetchall()]
                if popular_exts:
                    self.extension_info_label.setText(
                        f"Häufigste Endungen: {', '.join(popular_exts[:10])}{'...' if len(popular_exts) > 10 else ''}"
                    )
                else:
                    self.extension_info_label.setText(f"{count} verschiedene Dateiendungen verfügbar")
            else:
                self.extension_info_label.setText("Keine Dateiendungen gefunden")
    
    def insert_text_at_cursor(self, line_edit, text):
        """Fügt Text an der Cursor-Position ein"""
        cursor_pos = line_edit.cursorPosition()
        current_text = line_edit.text()
        
        # Spezialbehandlung für Klammern und Anführungszeichen
        if text.strip() == "()":
            new_text = current_text[:cursor_pos] + "()" + current_text[cursor_pos:]
            line_edit.setText(new_text)
            line_edit.setCursorPosition(cursor_pos + 1)  # Cursor zwischen die Klammern
        elif text.strip() == '""':
            new_text = current_text[:cursor_pos] + '""' + current_text[cursor_pos:]
            line_edit.setText(new_text)
            line_edit.setCursorPosition(cursor_pos + 1)  # Cursor zwischen die Anführungszeichen
        else:
            new_text = current_text[:cursor_pos] + text + current_text[cursor_pos:]
            line_edit.setText(new_text)
            line_edit.setCursorPosition(cursor_pos + len(text))
        
        line_edit.setFocus()

    def add_search_term(self):
        """Fügt ein neues Suchbegriff-Widget hinzu"""
        term_widget = QtWidgets.QWidget()
        term_layout = QtWidgets.QHBoxLayout(term_widget)
        term_layout.setContentsMargins(0, 0, 0, 0)
        
        # Operator-Auswahl (außer für den ersten Begriff)
        operator_combo = None
        if len(self.search_terms) > 0:
            operator_combo = QtWidgets.QComboBox()
            operator_combo.addItems(['AND', 'OR', 'NOT'])
            operator_combo.setMaximumWidth(60)
            term_layout.addWidget(operator_combo)
        
        # Eingabefeld für den Suchbegriff
        term_input = QtWidgets.QLineEdit()
        term_input.setPlaceholderText("z.B. *.pdf, backup*, Dokument*")
        term_layout.addWidget(term_input)
        
        # Löschen-Button (nur ab dem zweiten Begriff)
        remove_btn = None
        if len(self.search_terms) > 0:
            remove_btn = QtWidgets.QPushButton("✖")
            remove_btn.setMaximumWidth(30)
            remove_btn.setStyleSheet("QPushButton { color: red; font-weight: bold; }")
            remove_btn.clicked.connect(lambda: self.remove_search_term(term_widget))
            term_layout.addWidget(remove_btn)
        
        # Widget zur Liste hinzufügen
        term_data = {
            'widget': term_widget,
            'layout': term_layout,
            'operator': operator_combo,
            'input': term_input,
            'remove_btn': remove_btn
        }
        self.search_terms.append(term_data)
        self.search_terms_layout.addWidget(term_widget)
        
        # Focus auf das neue Eingabefeld setzen
        term_input.setFocus()
    
    def remove_search_term(self, widget_to_remove):
        """Entfernt ein Suchbegriff-Widget"""
        # Widget aus der Liste finden und entfernen
        for i, term_data in enumerate(self.search_terms):
            if term_data['widget'] == widget_to_remove:
                self.search_terms_layout.removeWidget(widget_to_remove)
                widget_to_remove.deleteLater()
                del self.search_terms[i]
                break
    
    def clear_search_terms(self):
        """Löscht alle Suchbegriff-Widgets bis auf das erste"""
        while len(self.search_terms) > 1:
            term_data = self.search_terms.pop()
            self.search_terms_layout.removeWidget(term_data['widget'])
            term_data['widget'].deleteLater()
        
        # Erstes Feld leeren
        if self.search_terms:
            self.search_terms[0]['input'].clear()

    def build_filename_query(self):
        """Erstellt eine SQL-WHERE-Klausel aus den strukturierten Suchbegriffen"""
        if not self.search_terms:
            return "", []
        
        # Sammle alle nicht-leeren Suchbegriffe
        terms = []
        for i, term_data in enumerate(self.search_terms):
            search_text = term_data['input'].text().strip()
            if not search_text:
                continue
            
            # Operator (für erste Eingabe ist es implizit 'AND')
            operator = 'AND'
            if i > 0 and term_data['operator']:
                operator = term_data['operator'].currentText()
            
            # Wildcard zu SQL LIKE konvertieren
            search_pattern = search_text.replace('*', '%').replace('?', '_')
            
            terms.append({
                'operator': operator,
                'pattern': search_pattern,
                'is_first': i == 0
            })
        
        if not terms:
            return "", []
        
        # SQL-Ausdruck aufbauen
        sql_parts = []
        params = []
        
        for term in terms:
            if term['is_first']:
                # Erster Begriff ohne Operator
                sql_parts.append("files.filename LIKE ?")
                params.append(f"%{term['pattern']}%")
            else:
                if term['operator'] == 'NOT':
                    sql_parts.append("AND files.filename NOT LIKE ?")
                    params.append(f"%{term['pattern']}%")
                elif term['operator'] == 'OR':
                    sql_parts.append("OR files.filename LIKE ?")
                    params.append(f"%{term['pattern']}%")
                else:  # AND
                    sql_parts.append("AND files.filename LIKE ?")
                    params.append(f"%{term['pattern']}%")
        
        if sql_parts:
            # Gesamten Ausdruck in Klammern setzen
            sql_expr = "(" + " ".join(sql_parts) + ")"
            return sql_expr, params
        
        return "", []

    def clear_filters(self):
        """Setzt alle Filter zurück"""
        self.path_input.clear()
        self.clear_search_terms()
        self.toggle_all_items(self.drive_list, False)
        self.toggle_all_items(self.extension_list, False)
        self.toggle_all_items(self.category_list, False)
        self.size_value1.setValue(0)
        self.size_value2.setValue(0)
        self.show_only_hashed.setChecked(False)
        self.show_duplicates.setChecked(False)
        self.table.setRowCount(0)
        self.status_label.setText("Filter zurückgesetzt - Bereit für neue Suche")

    def get_search_criteria(self):
        """Sammelt alle Suchkriterien aus der UI"""
        criteria = {}
        
        # Pfad-Filter
        if self.path_input.text().strip():
            criteria['path_filter'] = self.path_input.text().strip()
        
        # Dateiname-Filter aus strukturierten Suchbegriffen
        filename_sql, filename_params = self.build_filename_query()
        if filename_sql:
            criteria['filename_sql'] = filename_sql
            criteria['filename_params'] = filename_params
        
        # Ausgewählte Drives
        selected_drives = []
        for i in range(self.drive_list.count()):
            item = self.drive_list.item(i)
            if item.checkState() == QtCore.Qt.Checked:
                selected_drives.append(item.text())
        if selected_drives:
            criteria['drives'] = selected_drives
        
        # Ausgewählte Extensions aus dem Textfeld
        if self.extension_input.text().strip():
            # Parse kommagetrennte Extensions
            extensions_text = self.extension_input.text().strip()
            selected_extensions = []
            for ext in extensions_text.split(','):
                ext = ext.strip()
                # Füge Punkt hinzu, falls nicht vorhanden
                if ext and not ext.startswith('.'):
                    ext = '.' + ext
                if ext:
                    selected_extensions.append(ext)
            if selected_extensions:
                criteria['extensions'] = selected_extensions
        
        # Ausgewählte Kategorien
        selected_categories = []
        for i in range(self.category_list.count()):
            item = self.category_list.item(i)
            if item.checkState() == QtCore.Qt.Checked:
                selected_categories.append(item.text().lower())
        if selected_categories:
            criteria['categories'] = selected_categories
        
        # Größenfilter
        if self.size_value1.value() > 0:
            size_multipliers = {'Bytes': 1, 'KB': 1024, 'MB': 1024**2, 'GB': 1024**3}
            multiplier = size_multipliers[self.size_unit.currentText()]
            
            size_filter = {
                'operator': self.size_operator.currentText(),
                'value1': int(self.size_value1.value() * multiplier)
            }
            if self.size_operator.currentText() == 'zwischen':
                size_filter['value2'] = int(self.size_value2.value() * multiplier)
            criteria['size_filter'] = size_filter
        
        # Hash-Filter
        if self.show_only_hashed.isChecked():
            criteria['has_hash'] = True
        
        if self.show_duplicates.isChecked():
            criteria['show_duplicates'] = True
        
        # Sortierung
        criteria['order_by'] = self.sort_by.currentText()
        criteria['order_direction'] = self.sort_direction.currentText()
        criteria['limit'] = self.result_limit.value()
        
        return criteria

    def start_search(self):
        """Startet die erweiterte Suche"""
        if self.worker and self.worker.isRunning():
            QtWidgets.QMessageBox.information(self, "Suche läuft", "Eine Suche läuft bereits. Bitte warten...")
            return
        
        criteria = self.get_search_criteria()
        if not any(criteria.values()):
            reply = QtWidgets.QMessageBox.question(
                self, "Keine Filter", 
                "Keine Suchfilter gesetzt. Alle Dateien anzeigen? (Kann bei großen Datenbanken dauern)",
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                QtWidgets.QMessageBox.No
            )
            if reply == QtWidgets.QMessageBox.No:
                return
        
        self.status_label.setText("🔍 Suche läuft...")
        self.search_button.setEnabled(False)
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
        
        self.worker = EnhancedSearchWorker(self.db_path, criteria)
        self.worker.finished.connect(self.on_search_finished)
        self.worker.start()

    def on_search_finished(self, results, total_count, query_info):
        """Verarbeitet die Suchergebnisse"""
        QtWidgets.QApplication.restoreOverrideCursor()
        self.search_button.setEnabled(True)
        
        self.table.setRowCount(len(results))
        self.table.setSortingEnabled(False)
        
        for row, result in enumerate(results):
            (file_id, drive_name, directory_path, filename, extension, 
             category, size, hash_val, created_date, modified_date, full_path) = result
            
            # Icon (Spalte 0)
            icon_item = QtWidgets.QTableWidgetItem()
            try:
                file_info = QtCore.QFileInfo(full_path.replace('/', os.sep))
                icon = self.icon_provider.icon(file_info)
                if not icon.isNull():
                    icon_item.setIcon(icon)
            except:
                pass
            self.table.setItem(row, 0, icon_item)
            
            # Daten
            self.table.setItem(row, 1, QtWidgets.QTableWidgetItem(drive_name or ''))
            self.table.setItem(row, 2, QtWidgets.QTableWidgetItem(directory_path or ''))
            self.table.setItem(row, 3, QtWidgets.QTableWidgetItem(filename or ''))
            self.table.setItem(row, 4, QtWidgets.QTableWidgetItem(extension or ''))
            self.table.setItem(row, 5, QtWidgets.QTableWidgetItem((category or '').title()))
            
            # Größe (formatiert)
            if size:
                if size > 1024**3:
                    size_text = f"{size/(1024**3):.2f} GB"
                elif size > 1024**2:
                    size_text = f"{size/(1024**2):.2f} MB"
                elif size > 1024:
                    size_text = f"{size/1024:.2f} KB"
                else:
                    size_text = f"{size} B"
                size_item = NumericTableWidgetItem(size, size_text)
                size_item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
            else:
                size_item = QtWidgets.QTableWidgetItem("")
            self.table.setItem(row, 6, size_item)
            
            # Hash (verkürzt anzeigen)
            hash_text = hash_val[:16] + "..." if hash_val and len(hash_val) > 16 else (hash_val or "")
            hash_item = QtWidgets.QTableWidgetItem(hash_text)
            if hash_val:
                hash_item.setToolTip(hash_val)  # Vollständiger Hash als Tooltip
            self.table.setItem(row, 7, hash_item)
            
            # Änderungsdatum
            date_text = modified_date[:10] if modified_date else ""  # Nur Datum, keine Zeit
            self.table.setItem(row, 8, QtWidgets.QTableWidgetItem(date_text))
        
        self.table.setSortingEnabled(True)
        self.table.resizeColumnsToContents()
        
        # Status aktualisieren
        if len(results) < total_count:
            self.status_label.setText(f"✅ {len(results)} von {total_count} Ergebnissen angezeigt (limitiert)")
        else:
            self.status_label.setText(f"✅ {total_count} Ergebnisse gefunden")

    def show_context_menu(self, position):
        """Zeigt Kontextmenü für die Tabelle"""
        if not self.table.itemAt(position):
            return
            
        menu = QtWidgets.QMenu(self)
        
        open_action = menu.addAction("📂 Öffnen")
        explorer_action = menu.addAction("🗂️ Im Explorer öffnen")
        copy_path_action = menu.addAction("📋 Pfad kopieren")
        menu.addSeparator()
        properties_action = menu.addAction("ℹ️ Eigenschaften")
        
        action = menu.exec_(self.table.viewport().mapToGlobal(position))
        
        if action == open_action:
            self.open_selected()
        elif action == explorer_action:
            self.open_in_explorer()
        elif action == copy_path_action:
            self.copy_paths()
        elif action == properties_action:
            self.show_properties()

    def get_selected_files(self):
        """Gibt die ausgewählten Dateipfade zurück"""
        selected_rows = list(set(index.row() for index in self.table.selectedIndexes()))
        files = []
        
        for row in selected_rows:
            drive_item = self.table.item(row, 1)
            path_item = self.table.item(row, 2)
            filename_item = self.table.item(row, 3)
            extension_item = self.table.item(row, 4)
            
            if all([drive_item, path_item, filename_item]):
                drive = drive_item.text()
                directory = path_item.text()
                filename = filename_item.text()
                extension = extension_item.text() if extension_item else ''
                
                # Vollständigen Pfad rekonstruieren
                full_path = os.path.join(directory, filename + extension)
                full_path = os.path.normpath(full_path.replace('/', os.sep))
                files.append(full_path)
        
        return files

    def open_selected(self):
        """Öffnet die ausgewählte(n) Datei(en)"""
        files = self.get_selected_files()
        if not files:
            QtWidgets.QMessageBox.warning(self, "Keine Auswahl", "Bitte wählen Sie eine Datei aus.")
            return
        
        for file_path in files[:5]:  # Max 5 Dateien öffnen
            if os.path.exists(file_path):
                open_file_with_default_app(file_path)
        
        if len(files) > 5:
            QtWidgets.QMessageBox.information(self, "Info", f"{len(files)} Dateien ausgewählt, nur die ersten 5 wurden geöffnet.")

    def open_in_explorer(self):
        """Öffnet den Ordner der ersten ausgewählten Datei"""
        files = self.get_selected_files()
        if not files:
            QtWidgets.QMessageBox.warning(self, "Keine Auswahl", "Bitte wählen Sie eine Datei aus.")
            return
        
        folder = os.path.dirname(files[0])
        if os.path.exists(folder):
            open_folder_in_explorer(folder)

    def copy_paths(self):
        """Kopiert die Pfade der ausgewählten Dateien in die Zwischenablage"""
        files = self.get_selected_files()
        if not files:
            QtWidgets.QMessageBox.warning(self, "Keine Auswahl", "Bitte wählen Sie mindestens eine Datei aus.")
            return
        
        clipboard = QtWidgets.QApplication.clipboard()
        clipboard.setText('\n'.join(files))
        self.status_label.setText(f"📋 {len(files)} Dateipfad(e) kopiert")

    def export_results(self):
        """Exportiert die aktuellen Suchergebnisse"""
        if self.table.rowCount() == 0:
            QtWidgets.QMessageBox.warning(self, "Keine Daten", "Keine Ergebnisse zum Exportieren vorhanden.")
            return
        
        file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self, "Suchergebnisse exportieren", "", 
            "CSV Dateien (*.csv);;JSON Dateien (*.json)"
        )
        
        if not file_path:
            return
        
        try:
            if file_path.endswith('.csv'):
                self.export_csv(file_path)
            elif file_path.endswith('.json'):
                self.export_json(file_path)
            
            QtWidgets.QMessageBox.information(self, "Export erfolgreich", f"Ergebnisse exportiert nach:\n{file_path}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Export-Fehler", f"Fehler beim Exportieren: {e}")

    def export_csv(self, file_path):
        """Exportiert als CSV"""
        import csv
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Header
            headers = [self.table.horizontalHeaderItem(i).text() for i in range(1, self.table.columnCount())]
            writer.writerow(headers)
            
            # Daten
            for row in range(self.table.rowCount()):
                row_data = []
                for col in range(1, self.table.columnCount()):
                    item = self.table.item(row, col)
                    row_data.append(item.text() if item else '')
                writer.writerow(row_data)

    def export_json(self, file_path):
        """Exportiert als JSON"""
        import json
        data = []
        headers = [self.table.horizontalHeaderItem(i).text() for i in range(1, self.table.columnCount())]
        
        for row in range(self.table.rowCount()):
            row_dict = {}
            for col in range(1, self.table.columnCount()):
                item = self.table.item(row, col)
                row_dict[headers[col-1]] = item.text() if item else ''
            data.append(row_dict)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def show_properties(self):
        """Zeigt Eigenschaften der ersten ausgewählten Datei"""
        files = self.get_selected_files()
        if not files:
            return
        
        file_path = files[0]
        if not os.path.exists(file_path):
            QtWidgets.QMessageBox.warning(self, "Datei nicht gefunden", f"Die Datei existiert nicht mehr:\n{file_path}")
            return
        
        # Eigenschaften sammeln
        stat = os.stat(file_path)
        size = stat.st_size
        created = datetime.datetime.fromtimestamp(stat.st_ctime).strftime('%Y-%m-%d %H:%M:%S')
        modified = datetime.datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        
        # Dialog erstellen
        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle("Dateieigenschaften")
        dialog.setModal(True)
        dialog.resize(400, 300)
        
        layout = QtWidgets.QFormLayout()
        layout.addRow("Datei:", QtWidgets.QLabel(os.path.basename(file_path)))
        layout.addRow("Pfad:", QtWidgets.QLabel(os.path.dirname(file_path)))
        layout.addRow("Größe:", QtWidgets.QLabel(f"{size:,} Bytes"))
        layout.addRow("Erstellt:", QtWidgets.QLabel(created))
        layout.addRow("Geändert:", QtWidgets.QLabel(modified))
        
        close_btn = QtWidgets.QPushButton("Schließen")
        close_btn.clicked.connect(dialog.close)
        layout.addRow("", close_btn)
        
        dialog.setLayout(layout)
        dialog.exec_()

    def closeEvent(self, event):
        if self.worker and self.worker.isRunning():
            self.worker.terminate()
            self.worker.wait()
        if self.conn:
            self.conn.close()
        event.accept()

def main():
    if len(sys.argv) < 2:
        print("Usage: {} <db_path>".format(sys.argv[0]))
        sys.exit(1)
    
    db_path = sys.argv[1]
    app = QtWidgets.QApplication(sys.argv)
    window = EnhancedMainWindow(db_path)
    window.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()