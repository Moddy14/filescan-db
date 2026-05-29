#!/usr/bin/env python3
import sys
import os
import sqlite3
import shutil
from PyQt5 import QtWidgets, QtCore, QtGui, QtSql
import subprocess # Hinzugefügt für open_in_explorer
import math # Hinzugefügt für Größenberechnung
import datetime # Hinzugefügt für Datumskonvertierung
import ctypes # NEU: Für ShellExecuteW unter Windows
from PyQt5.QtCore import QThread, pyqtSignal

# Helper function zum Öffnen von Dateien plattformabhängig
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

# NEU: Helper function zum Öffnen des Ordners im Explorer (ERWEITERT)
def open_folder_in_explorer(folder_path, as_admin=False):
    """Öffnet den gegebenen Ordner im nativen Dateimanager.
    Wenn as_admin=True und unter Windows, wird versucht, Explorer mit Admin-Rechten zu starten.
    """
    try:
        if sys.platform.startswith('win'):
            if as_admin:
                # Aufruf von ShellExecuteW mit dem Verb "runas" zur Erzwingung von Admin-Rechten.
                # Parameter: (hwnd, Operation, Datei, Parameter, Arbeitsverzeichnis, Darstellungsart)
                # hwnd=None: Kein Elternfenster
                # Operation="runas": Fordert Admin-Rechte an
                # Datei="explorer.exe": Das zu startende Programm
                # Parameter=folder_path: Der Ordner, der geöffnet werden soll
                # Arbeitsverzeichnis=None: Nicht benötigt
                # Darstellungsart=1: SW_SHOWNORMAL
                print(f"Versuche Explorer als Admin für '{folder_path}' zu starten...")
                result = ctypes.windll.shell32.ShellExecuteW(None, "runas", "explorer.exe", folder_path, None, 1)
                if int(result) <= 32:
                    # Ein Ergebniswert <= 32 deutet auf einen Fehler hin.
                    # Mögliche Gründe: UAC abgelehnt, Pfad ungültig, etc.
                    error_msg = f"Explorer konnte nicht als Admin gestartet werden (Fehlercode: {result}).\nMöglicherweise wurden Admin-Rechte verweigert."
                    print("FEHLER: " + error_msg)
                    QtWidgets.QMessageBox.warning(None, "Fehler bei Admin-Start", error_msg)
                else:
                    print("Erfolg: Explorer wurde (vermutlich) als Admin gestartet.")
            else:
                # Normaler Start ohne Admin-Rechte
                os.startfile(folder_path)
        elif sys.platform.startswith('darwin'):
            # macOS (Admin-Start nicht auf diese Weise möglich/nötig)
            if as_admin: print("Admin-Start für Finder nicht unterstützt.")
            subprocess.run(['open', folder_path], check=True)
        else:
            # Linux (Admin-Start hängt vom Dateimanager ab, meist komplizierter)
            if as_admin: print("Admin-Start für Linux-Dateimanager nicht standardmäßig unterstützt.")
            subprocess.run(['xdg-open', folder_path], check=True)
    except FileNotFoundError:
         QtWidgets.QMessageBox.critical(None, "Fehler", f"Konnte den Ordner '{folder_path}' nicht finden.")
    except Exception as e:
        # Hier auch Fehler von ShellExecuteW abfangen? Nein, die werden über den result Code signalisiert.
        QtWidgets.QMessageBox.critical(None, "Fehler", f"Ordner konnte nicht im Explorer geöffnet werden: {e}")

# --- NEU: Custom Table Widget Item für numerische Sortierung ---
class NumericTableWidgetItem(QtWidgets.QTableWidgetItem):
    def __init__(self, value, display_text=None):
        # Wenn kein Anzeigetext gegeben, nimm den Wert selbst
        super().__init__(display_text if display_text is not None else str(value))
        self.numeric_value = value

    # Überschreibe den Kleiner-als-Operator für die Sortierung
    def __lt__(self, other):
        try:
            # Versuche, den anderen Wert auch als Zahl zu behandeln
            # Dies funktioniert, wenn der andere auch ein NumericTableWidgetItem ist
            # oder wenn sein text() als Zahl interpretierbar ist (Fallback)
            other_value = float(other.numeric_value if isinstance(other, NumericTableWidgetItem) else other.text())
            return self.numeric_value < other_value
        except (ValueError, TypeError, AttributeError):
            # Fallback: Wenn der andere Wert keine Zahl ist, normale Textsortierung
            return super().__lt__(other)
# --- Ende Custom Item ---

class SearchWorker(QThread):
    finished = pyqtSignal(list, int, bool)  # filtered_results, result_count, warnung_gesetzt
    debug_log = pyqtSignal(str)  # Debug-Nachrichten

    def __init__(self, db_path, filter_args):
        super().__init__()
        self.db_path = db_path
        self.filter_args = filter_args

    def run(self):
        import sqlite3, datetime, os
        self.debug_log.emit("=" * 60)
        self.debug_log.emit(f"🔍 SUCHE GESTARTET - {datetime.datetime.now().strftime('%H:%M:%S')}")
        self.debug_log.emit(f"📂 Datenbank: {self.db_path}")
        db_uri = "file:" + self.db_path.replace("\\", "/") + "?mode=ro"
        conn = sqlite3.connect(db_uri, uri=True, timeout=30.0)
        conn.execute("PRAGMA query_only = ON")
        (
            path_query, name_queries, name_ops, size_op, size_val1, size_val2, size_unit,
            qdate_val1, qdate_val2, date_op, size_units
        ) = self.filter_args
        min_len = 3
        path_filter_active = len(path_query) >= min_len
        active_name_query_indices = [i for i, q in enumerate(name_queries) if len(q) >= min_len]
        name_filter_active = bool(active_name_query_indices)
        size_filter_active = (size_op != "Egal")
        date_filter_active = (date_op != "Egal")
        
        # DEBUG: Eingaben loggen
        self.debug_log.emit(f"\n📋 EINGABEN:")
        self.debug_log.emit(f"   Pfad-Suche: '{path_query}' (aktiv: {path_filter_active}, min_len={min_len})")
        for i, q in enumerate(name_queries):
            op = name_ops[i] if i < len(name_ops) else "-"
            active = i in active_name_query_indices
            self.debug_log.emit(f"   Name[{i}]: '{q}' (aktiv: {active}, Operator: {op})")
        self.debug_log.emit(f"   Größe: {size_op} {size_val1} {size_unit}" + (f" bis {size_val2}" if size_op == "Zwischen" else ""))
        self.debug_log.emit(f"   Datum: {date_op}" + (f" {qdate_val1}" if date_op != "Egal" else ""))
        self.debug_log.emit(f"\n   ⚙️ Filter aktiv: Pfad={path_filter_active}, Name={name_filter_active}, Größe={size_filter_active}, Datum={date_filter_active}")
        # Angepasste SQL für optimierte Datenbankstruktur
        sql_select = """
            SELECT files.id, drives.name, directories.full_path,
                   directories.full_path || '/' || files.filename || COALESCE(extensions.name, '') as file_path,
                   files.size
        """
        sql_count = "SELECT COUNT(*)"
        sql_from_joins = """
            FROM files
            JOIN directories ON files.directory_id = directories.id
            JOIN drives ON directories.drive_id = drives.id
            LEFT JOIN extensions ON files.extension_id = extensions.id
        """
        where_clauses = []
        params = []
        if path_filter_active:
            path_like = '%' + path_query.replace('*', '%') + '%'
            where_clauses.append("""(
                drives.name LIKE ? OR
                directories.full_path LIKE ?
            )""")
            params.extend([path_like, path_like])
        name_filter_sql_parts = []
        active_ops = []
        last_active_index = -1
        for i in active_name_query_indices:
            if last_active_index != -1:
                op_combo_index = last_active_index
                if op_combo_index < len(name_ops):
                    active_ops.append(name_ops[op_combo_index])
            last_active_index = i
        current_op_idx = 0
        for idx, name_query_idx in enumerate(active_name_query_indices):
            name_query = name_queries[name_query_idx]
            name_like = '%' + name_query.replace('*', '%') + '%'
            sql_part = ""
            op_before_term = "AND"
            if idx > 0:
                if current_op_idx < len(active_ops):
                    op_before_term = active_ops[current_op_idx]
                    current_op_idx += 1
            if op_before_term == "ODER":
                sql_part += " OR "
            elif op_before_term == "NICHT":
                # Angepasst für neue Datenbankstruktur - Suche in filename UND extension
                sql_part += " AND (files.filename NOT LIKE ? AND COALESCE(extensions.name, '') NOT LIKE ? AND (files.filename || COALESCE(extensions.name, '')) NOT LIKE ?) "
                params.extend([name_like, name_like, name_like])
                name_filter_sql_parts.append(sql_part)
                continue
            else:
                if idx > 0:
                    sql_part += " AND "
            # Angepasst für neue Datenbankstruktur - Suche in filename UND extension  
            sql_part += " (files.filename LIKE ? OR COALESCE(extensions.name, '') LIKE ? OR (files.filename || COALESCE(extensions.name, '')) LIKE ?) "
            params.extend([name_like, name_like, name_like])
            if op_before_term != "NICHT":
                name_filter_sql_parts.append(sql_part)
        if name_filter_sql_parts:
            full_name_filter_sql = " ".join(name_filter_sql_parts).strip()
            if full_name_filter_sql.startswith("AND "):
                full_name_filter_sql = full_name_filter_sql[4:]
            elif full_name_filter_sql.startswith("OR "):
                full_name_filter_sql = full_name_filter_sql[3:]
            where_clauses.append(f"({full_name_filter_sql})")
        if size_filter_active:
            multiplier = size_units.get(size_unit, 1)
            size_bytes1 = size_val1 * multiplier
            size_bytes2 = size_val2 * multiplier
            op_sql = ""
            if size_op == ">": op_sql = "files.size > ?"
            elif size_op == "<": op_sql = "files.size < ?"
            elif size_op == "=": op_sql = "files.size = ?"
            elif size_op == "Zwischen": op_sql = "files.size BETWEEN ? AND ?"
            if op_sql:
                where_clauses.append(op_sql)
                params.append(size_bytes1)
                if size_op == "Zwischen":
                    if size_bytes2 < size_bytes1: size_bytes1, size_bytes2 = size_bytes2, size_bytes1
                    params.append(size_bytes2)
        sql_where = ""
        if where_clauses:
            sql_where = " WHERE " + " AND ".join(where_clauses)
        
        # DEBUG: SQL und Parameter loggen
        self.debug_log.emit(f"\n📊 SQL WHERE-KLAUSELN ({len(where_clauses)}):")
        for i, clause in enumerate(where_clauses):
            self.debug_log.emit(f"   [{i}] {clause}")
        self.debug_log.emit(f"\n📊 PARAMETER ({len(params)}):")
        for i, p in enumerate(params):
            self.debug_log.emit(f"   [{i}] {repr(p)}")
        
        sql_count_query = sql_count + sql_from_joins + sql_where
        self.debug_log.emit(f"\n🔢 COUNT-QUERY:\n{sql_count_query}")
        
        result_count = -1
        warnung_gesetzt = False
        try:
            cursor = conn.cursor()
            cursor.execute(sql_count_query, tuple(params))
            result_count = cursor.fetchone()[0]
            self.debug_log.emit(f"\n✅ DB-Ergebnis: {result_count} Treffer")
        except Exception as e:
            self.debug_log.emit(f"\n❌ COUNT-QUERY FEHLER: {e}")
            conn.close()
            self.finished.emit([], 0, False)
            return
        if result_count > 1000:
            warnung_gesetzt = True
        db_results = []
        if result_count >= 0:
            sql_main_query = sql_select + sql_from_joins + sql_where
            self.debug_log.emit(f"\n📊 HAUPT-QUERY:\n{sql_main_query}")
            try:
                cursor = conn.cursor()
                cursor.execute(sql_main_query, tuple(params))
                db_results = cursor.fetchall()
                self.debug_log.emit(f"✅ {len(db_results)} Zeilen geladen")
                # DEBUG: Erste 5 Ergebnisse zeigen
                for i, row in enumerate(db_results[:5]):
                    self.debug_log.emit(f"   Zeile {i}: ID={row[0]}, Pfad={row[2]}, Datei={row[3]}")
                if len(db_results) > 5:
                    self.debug_log.emit(f"   ... und {len(db_results)-5} weitere")
            except Exception as e:
                self.debug_log.emit(f"\n❌ HAUPT-QUERY FEHLER: {e}")
                conn.close()
                self.finished.emit([], 0, warnung_gesetzt)
                return
        filtered_results = []
        if date_filter_active and db_results:
            self.debug_log.emit(f"\n📅 DATUMS-FILTER aktiv ({date_op}), prüfe {len(db_results)} Dateien auf Dateisystem...")
            filter_dt1 = datetime.date(qdate_val1.year(), qdate_val1.month(), qdate_val1.day())
            filter_dt2 = datetime.date(qdate_val2.year(), qdate_val2.month(), qdate_val2.day())
            if date_op == "Zwischen" and filter_dt2 < filter_dt1:
                filter_dt1, filter_dt2 = filter_dt2, filter_dt1
            self.debug_log.emit(f"   Datum-Bereich: {filter_dt1} bis {filter_dt2}")
            skipped_not_found = 0
            skipped_no_match = 0
            skipped_error = 0
            for idx, row_data in enumerate(db_results):
                drive_part = str(row_data[1])
                dir_part = str(row_data[2])
                file_path = str(row_data[3])  # Bereits vollständiger Pfad aus JOIN
                # Normalisiere Pfad für Windows-Kompatibilität
                full_path = os.path.normpath(file_path.replace('/', os.sep))
                try:
                    mtime_timestamp = os.path.getmtime(full_path)
                    mtime_date = datetime.date.fromtimestamp(mtime_timestamp)
                    match = False
                    if date_op == "Nach" and mtime_date > filter_dt1: match = True
                    elif date_op == "Vor" and mtime_date < filter_dt1: match = True
                    elif date_op == "Am" and mtime_date == filter_dt1: match = True
                    elif date_op == "Zwischen" and filter_dt1 <= mtime_date <= filter_dt2: match = True
                    if match:
                        filtered_results.append(row_data)
                    else:
                        skipped_no_match += 1
                except FileNotFoundError:
                    skipped_not_found += 1
                    if skipped_not_found <= 3:
                        self.debug_log.emit(f"   ⚠️ Datei nicht gefunden: {full_path}")
                    continue
                except Exception as e:
                    skipped_error += 1
                    if skipped_error <= 3:
                        self.debug_log.emit(f"   ❌ Fehler bei: {full_path} → {e}")
                    continue
            self.debug_log.emit(f"\n   📅 Datums-Filter Ergebnis:")
            self.debug_log.emit(f"   ✅ Treffer: {len(filtered_results)}")
            self.debug_log.emit(f"   ⏭️ Datum passt nicht: {skipped_no_match}")
            self.debug_log.emit(f"   ⚠️ Datei nicht gefunden: {skipped_not_found}")
            self.debug_log.emit(f"   ❌ Fehler: {skipped_error}")
        else:
            filtered_results = db_results
            if not date_filter_active:
                self.debug_log.emit(f"\n📅 Datums-Filter: NICHT aktiv → alle {len(db_results)} DB-Ergebnisse übernommen")
        
        self.debug_log.emit(f"\n🏁 ENDERGEBNIS: {len(filtered_results)} Dateien an GUI übergeben")
        self.debug_log.emit("=" * 60)
        conn.close()
        self.finished.emit(filtered_results, result_count, warnung_gesetzt)

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, db_path):
        super().__init__()
        self.setWindowTitle("Datei Suche - Erweiterte Filter & Icons")
        self.resize(1150, 770) # Höhe leicht erhöht für Statusleiste
        _icon = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'icons', 'search_simple.ico')
        if os.path.exists(_icon):
            self.setWindowIcon(QtGui.QIcon(_icon))
        self.db_path = db_path
        self.conn = None
        self.connect_db()
        self.icon_provider = QtWidgets.QFileIconProvider()
        
        # NEU: Labels für Statusleiste initialisieren
        self.status_total_label = QtWidgets.QLabel("Gefunden: 0")
        self.status_selected_label = QtWidgets.QLabel("Ausgewählt: 0")
        
        self.setup_ui()
        self._apply_stylesheet()
        self.update_action_buttons_state()
        self._update_status_bar() # Initialen Status setzen
    
    # --- NEU: Methode zum Anwenden des Stylesheets ---
    def _apply_stylesheet(self):
        qss = """
            /* Global settings */
            QWidget {
                font-size: 10pt; /* oder 11pt */
                color: #333;
            }

            QMainWindow {
                background-color: #f0f0f0; /* Heller Hintergrund */
            }

            /* Eingabefelder */
            QLineEdit {
                padding: 5px;
                border: 1px solid #ccc;
                border-radius: 3px;
                background-color: white;
            }
            QLineEdit:focus {
                border: 1px solid #5cacee; /* Hellerer Blauton bei Fokus */
            }

            /* Buttons */
            QPushButton {
                padding: 6px 12px;
                border: 1px solid #bbb;
                border-radius: 4px; /* Etwas runder */
                background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                              stop: 0 #fdfdfd, stop: 1 #e9e9e9); /* Leichter Gradient */
                min-width: 70px; /* Mindestbreite angepasst */
            }
            QPushButton:hover {
                background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                              stop: 0 #ffffff, stop: 1 #f0f0f0); /* Heller bei Hover */
                border-color: #999;
            }
            QPushButton:pressed {
                background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                              stop: 0 #e9e9e9, stop: 1 #dcdcdc); /* Dunkler bei Klick */
                border-color: #888;
            }
            QPushButton:disabled {
                 background-color: #e0e0e0; /* Grauer wenn deaktiviert */
                 color: #999;
            }

            /* Tabelle */
            QTableWidget {
                border: 1px solid #ccc;
                gridline-color: #e0e0e0; /* Hellere Linienfarbe */
                background-color: white;
                alternate-background-color: #f7f7f7; /* Sehr dezente Alternierende Zeilenfarbe */
                selection-background-color: #b3d9ff; /* Klarerer Blauton für Auswahl */
                selection-color: #000; /* Schwarze Schrift bei Auswahl */
            }
            QHeaderView::section {
                background-color: #e8e8e8; /* Passend zu Buttons */
                padding: 5px;
                border-top: 0px;
                border-bottom: 1px solid #ccc;
                border-right: 1px solid #ccc;
                border-left: 0px;
                font-weight: bold;
            }
            QHeaderView::section:first {
                border-left: 1px solid #ccc; /* Linker Rand für erste Spalte */
            }
            QTableCornerButton::section { /* Ecke oben links */
                 background-color: #e8e8e8;
                 border: 1px solid #ccc;
             }

            /* Menüs (auch Kontextmenü) */
            QMenu {
                border: 1px solid #bbb;
                background-color: white;
                padding: 2px; /* Kleiner Innenabstand */
            }
            QMenu::item {
                padding: 6px 25px; /* Mehr Padding */
                background-color: transparent;
            }
            QMenu::item:selected {
                background-color: #b3d9ff; /* Gleiche Auswahlfarbe wie Tabelle */
                color: #000;
                border-radius: 2px;
            }
            QMenu::separator {
                height: 1px;
                background-color: #e0e0e0;
                margin-left: 5px;
                margin-right: 5px;
                margin-top: 2px;
                margin-bottom: 2px;
            }
        """
        self.setStyleSheet(qss) # Stylesheet auf das MainWindow anwenden
    # --- Ende Stylesheet-Methode ---
    
    def connect_db(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            # Foreign Keys aktivieren für korrekte CASCADE-Löschungen
            self.conn.execute("PRAGMA foreign_keys = ON")
        except sqlite3.Error as e:
            QtWidgets.QMessageBox.critical(self, "DB Fehler", f"Fehler beim Verbinden zur DB: {e}")
            sys.exit(1)
    
    def setup_ui(self):
        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
        
        # Layouts
        main_layout = QtWidgets.QVBoxLayout()
        top_filter_layout = QtWidgets.QHBoxLayout()
        name_filter_layout = QtWidgets.QHBoxLayout()
        size_date_filter_layout = QtWidgets.QHBoxLayout() # NEUES Layout
        search_button_layout = QtWidgets.QHBoxLayout()
        button_layout = QtWidgets.QHBoxLayout()
        
        # --- Top Filter-Bereich (Hauptsuche Pfad) --- 
        top_filter_layout.addWidget(QtWidgets.QLabel("Suche (Pfad): "))
        self.search_input = QtWidgets.QLineEdit()
        self.search_input.setPlaceholderText("Pfadteil (* als Wildcard)")
        top_filter_layout.addWidget(self.search_input)

        # --- Namensfilter-Bereich --- 
        name_filter_layout.addWidget(QtWidgets.QLabel("Dateiname: "))
        self.name_filter_inputs = []
        self.name_filter_operators = []
        
        operators = ["UND", "ODER", "NICHT"] # Verfügbare Operatoren
        
        for i in range(4): # Vier Eingabefelder
            line_edit = QtWidgets.QLineEdit()
            line_edit.setPlaceholderText(f"Begriff {i+1} (* Wildcard)")
            name_filter_layout.addWidget(line_edit)
            self.name_filter_inputs.append(line_edit)
            
            if i < 3: # Drei Operatoren-Comboboxen
                combo = QtWidgets.QComboBox()
                combo.addItems(operators)
                # Standard: UND für die ersten beiden, ODER für die letzte? Oder alle UND?
                combo.setCurrentIndex(0) # Standard: UND
                name_filter_layout.addWidget(combo)
                self.name_filter_operators.append(combo)
            
        name_filter_layout.addStretch() # Platz am Ende
        # --- Ende Namensfilter-Bereich ---

        # --- NEU: Größen- und Datumsfilter --- 
        # Größenfilter
        size_date_filter_layout.addWidget(QtWidgets.QLabel("Größe:"))
        self.size_op_combo = QtWidgets.QComboBox()
        self.size_op_combo.addItems(["Egal", ">", "<", "=", "Zwischen"])
        self.size_op_combo.currentIndexChanged.connect(self._update_size_filter_state)
        size_date_filter_layout.addWidget(self.size_op_combo)
        
        self.size_input1 = QtWidgets.QDoubleSpinBox() # Double für Kommawerte
        self.size_input1.setRange(0, 999999999) # Großer Bereich
        self.size_input1.setDecimals(2) # 2 Nachkommastellen
        size_date_filter_layout.addWidget(self.size_input1)
        
        self.size_unit_combo = QtWidgets.QComboBox()
        self.size_units = {"Bytes": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3}
        self.size_unit_combo.addItems(self.size_units.keys())
        self.size_unit_combo.setCurrentText("MB") # Standard MB
        size_date_filter_layout.addWidget(self.size_unit_combo)
        
        self.size_label_between = QtWidgets.QLabel(" bis ") # Label für "zwischen"
        size_date_filter_layout.addWidget(self.size_label_between)
        
        self.size_input2 = QtWidgets.QDoubleSpinBox()
        self.size_input2.setRange(0, 999999999)
        self.size_input2.setDecimals(2)
        size_date_filter_layout.addWidget(self.size_input2)
        
        # Datumsfilter
        size_date_filter_layout.addSpacing(25) # Abstand
        size_date_filter_layout.addWidget(QtWidgets.QLabel("Änderungsdatum:"))
        self.date_op_combo = QtWidgets.QComboBox()
        self.date_op_combo.addItems(["Egal", "Nach", "Vor", "Am", "Zwischen"])
        self.date_op_combo.currentIndexChanged.connect(self._update_date_filter_state)
        size_date_filter_layout.addWidget(self.date_op_combo)
        
        self.date_edit1 = QtWidgets.QDateEdit()
        self.date_edit1.setCalendarPopup(True)
        self.date_edit1.setDisplayFormat("yyyy-MM-dd")
        self.date_edit1.setDate(QtCore.QDate.currentDate().addDays(-30)) # Standard: vor 30 Tagen
        size_date_filter_layout.addWidget(self.date_edit1)
        
        self.date_label_between = QtWidgets.QLabel(" bis ")
        size_date_filter_layout.addWidget(self.date_label_between)
        
        self.date_edit2 = QtWidgets.QDateEdit()
        self.date_edit2.setCalendarPopup(True)
        self.date_edit2.setDisplayFormat("yyyy-MM-dd")
        self.date_edit2.setDate(QtCore.QDate.currentDate())
        size_date_filter_layout.addWidget(self.date_edit2)
        
        size_date_filter_layout.addStretch()
        # Initialen Zustand der Filter setzen
        self._update_size_filter_state()
        self._update_date_filter_state()
        # --- Ende Größen- und Datumsfilter ---

        # --- NEU: Suchbutton --- 
        self.search_button = QtWidgets.QPushButton("🔍 Suche starten")
        self.search_button.clicked.connect(self.search_files) # Direkter Aufruf!
        search_button_layout.addStretch()
        search_button_layout.addWidget(self.search_button)
        search_button_layout.addStretch()
        # --- Ende Suchbutton --- 
        
        # Ergebnis Tabelle
        self.table = QtWidgets.QTableWidget()
        self.table.setColumnCount(7) # NEU: Erhöht auf 7 Spalten (Icon + 6 alte)
        # NEU: Header angepasst (erste Spalte leer für Icon)
        self.table.setHorizontalHeaderLabels(["", "ID", "Laufwerk", "Verzeichnis", "Dateiname", "Größe", "Änderungsdatum"])
        self.table.setSelectionBehavior(QtWidgets.QTableWidget.SelectRows)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        self.table.setEditTriggers(QtWidgets.QTableWidget.NoEditTriggers)
        self.table.horizontalHeader().setStretchLastSection(False)
        self.table.verticalHeader().setVisible(False)
        self.table.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)
        self.table.itemSelectionChanged.connect(self.update_action_buttons_state)
        self.table.itemSelectionChanged.connect(self._update_status_bar) # NEU: Status bei Selektion ändern
        
        # NEU: Sortierung aktivieren
        self.table.setSortingEnabled(True)
        # NEU: Breite für Icon-Spalte setzen
        self.table.setColumnWidth(0, 30) # Schmale Spalte für Icons
        # Indizes für andere Spaltenbreiten anpassen (optional)
        # self.table.setColumnWidth(1, 50) # ID
        # self.table.setColumnWidth(2, 80) # Laufwerk
        # self.table.setColumnWidth(3, 300) # Verzeichnis
        # ...
        
        # Aktionsbuttons
        self.open_button = QtWidgets.QPushButton("Öffnen")
        self.open_button.clicked.connect(self.open_file)
        self.delete_button = QtWidgets.QPushButton("Löschen")
        self.delete_button.clicked.connect(self.delete_file)
        self.preview_button = QtWidgets.QPushButton("Vorschau")
        self.preview_button.clicked.connect(self.preview_file)
        self.move_button = QtWidgets.QPushButton("Verschieben")
        self.move_button.clicked.connect(self.move_file)
        self.rename_button = QtWidgets.QPushButton("Umbenennen")
        self.rename_button.clicked.connect(self.rename_file)
        self.share_button = QtWidgets.QPushButton("Teilen")
        self.share_button.clicked.connect(self.show_share_menu)
        
        for btn in [self.open_button, self.delete_button, self.preview_button, self.move_button, self.rename_button, self.share_button]:
            button_layout.addWidget(btn)
        button_layout.addStretch()
        
        # --- DEBUG-Panel ---
        self.debug_group = QtWidgets.QGroupBox("🐛 Debug-Log (SQL-Queries & Filter)")
        self.debug_group.setCheckable(True)
        self.debug_group.setChecked(False)  # Standardmäßig zugeklappt
        debug_layout = QtWidgets.QVBoxLayout()
        self.debug_text = QtWidgets.QPlainTextEdit()
        self.debug_text.setReadOnly(True)
        self.debug_text.setMaximumHeight(250)
        self.debug_text.setFont(QtGui.QFont("Consolas", 9))
        self.debug_text.setStyleSheet("background-color: #1e1e1e; color: #dcdcdc; border: 1px solid #555;")
        debug_btn_layout = QtWidgets.QHBoxLayout()
        debug_clear_btn = QtWidgets.QPushButton("🗑️ Log leeren")
        debug_clear_btn.clicked.connect(self.debug_text.clear)
        debug_copy_btn = QtWidgets.QPushButton("📋 Log kopieren")
        debug_copy_btn.clicked.connect(lambda: QtWidgets.QApplication.clipboard().setText(self.debug_text.toPlainText()))
        debug_btn_layout.addWidget(debug_clear_btn)
        debug_btn_layout.addWidget(debug_copy_btn)
        debug_btn_layout.addStretch()
        debug_layout.addWidget(self.debug_text)
        debug_layout.addLayout(debug_btn_layout)
        self.debug_group.setLayout(debug_layout)
        # --- Ende DEBUG-Panel ---
        
        # Zusammenführung der Layouts
        main_layout.addLayout(top_filter_layout)
        main_layout.addLayout(name_filter_layout)
        main_layout.addLayout(size_date_filter_layout) # NEUES Layout hinzugefügt
        main_layout.addLayout(search_button_layout) # NEUES Layout für Button eingefügt
        main_layout.addWidget(self.table)
        main_layout.addLayout(button_layout)
        main_layout.addWidget(self.debug_group)  # Debug-Panel unten
        central_widget.setLayout(main_layout)
        
        # --- NEU: Statusleiste hinzufügen --- 
        self.statusBar = QtWidgets.QStatusBar()
        self.setStatusBar(self.statusBar)
        # Füge Labels dauerhaft zur Statusleiste hinzu
        self.statusBar.addPermanentWidget(self.status_total_label)
        self.statusBar.addPermanentWidget(QtWidgets.QLabel(" | ")) # Trenner
        self.statusBar.addPermanentWidget(self.status_selected_label)
        # --- Ende Statusleiste ---
    
    # --- NEU: Kontextmenü-Handler (ERWEITERT) ---
    def show_context_menu(self, position):
        """Zeigt das Kontextmenü für die Tabelle an."""
        indexes = self.table.selectedIndexes()
        if not indexes:
            return

        context_menu = QtWidgets.QMenu(self)
        open_action = context_menu.addAction("📂 Öffnen")
        copy_path_action = context_menu.addAction("📋 Pfad kopieren")
        open_folder_action = context_menu.addAction("🧭 Im Explorer öffnen")
        # NEU: Admin Explorer Option (nur Windows)
        open_folder_admin_action = None
        if sys.platform.startswith('win'):
             open_folder_admin_action = context_menu.addAction("🛡️ Im Explorer öffnen (Admin)")
            
        context_menu.addSeparator()
        rename_action = context_menu.addAction("✏️ Umbenennen...")
        move_action = context_menu.addAction("➡️ Verschieben...")
        context_menu.addSeparator()
        delete_action = context_menu.addAction("❌ Löschen...")
        preview_action = context_menu.addAction("👁️ Vorschau")

        action = context_menu.exec_(self.table.viewport().mapToGlobal(position))

        if action == open_action: self.open_file()
        elif action == copy_path_action: self.copy_path()
        elif action == open_folder_action: self.open_in_explorer()
        elif action == open_folder_admin_action: self.open_in_explorer_admin() # NEU
        elif action == rename_action: self.rename_file()
        elif action == move_action: self.move_file()
        elif action == delete_action: self.delete_file()
        elif action == preview_action: self.preview_file()
    # --- Ende Kontextmenü-Handler ---
    
    def search_files(self):
        # --- Suchstatus anzeigen (Start) ---
        self.status_total_label.setText("Suche läuft...")
        self.status_total_label.setStyleSheet("background-color: orange; color: black; font-weight: bold;")
        self.search_button.setEnabled(False)
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)

        # Filterparameter sammeln
        path_query = self.search_input.text().strip()
        name_queries = [inp.text().strip() for inp in self.name_filter_inputs]
        name_ops = [op.currentText() for op in self.name_filter_operators]
        size_op = self.size_op_combo.currentText()
        size_val1 = self.size_input1.value()
        size_val2 = self.size_input2.value()
        size_unit = self.size_unit_combo.currentText()
        qdate_val1 = self.date_edit1.date()
        qdate_val2 = self.date_edit2.date()
        date_op = self.date_op_combo.currentText()
        size_units = self.size_units
        filter_args = (
            path_query, name_queries, name_ops, size_op, size_val1, size_val2, size_unit,
            qdate_val1, qdate_val2, date_op, size_units
        )
        # Worker starten
        self.worker = SearchWorker(self.db_path, filter_args)
        self.worker.finished.connect(self.on_search_finished)
        self.worker.debug_log.connect(self._append_debug)
        self.worker.start()
    
    def _append_debug(self, msg):
        """Debug-Nachricht ans Log anhängen"""
        self.debug_text.appendPlainText(msg)

    def on_search_finished(self, filtered_results, result_count, warnung_gesetzt):
        # Tabelle befüllen (wie bisher, aber ohne Suchlogik)
        self.table.setSortingEnabled(False)
        self.table.setUpdatesEnabled(False)
        self.table.setRowCount(0)
        self.table.setRowCount(len(filtered_results))
        display_unit_name = self.size_unit_combo.currentText()
        display_multiplier = self.size_units.get(display_unit_name, 1)
        import os, datetime
        for row, row_data in enumerate(filtered_results):
            drive_part = str(row_data[1])
            dir_part = str(row_data[2])
            file_path = str(row_data[3])  # Bereits vollständiger Pfad aus JOIN
            file_basename = os.path.basename(file_path)
            # Normalisiere Pfad für Windows-Kompatibilität
            full_path = os.path.normpath(file_path.replace('/', os.sep))
            file_info_for_icon = QtCore.QFileInfo(full_path)
            for col in range(7):
                cell = None
                if col == 0:
                    cell = QtWidgets.QTableWidgetItem()
                    try:
                        icon = self.icon_provider.icon(file_info_for_icon)
                        if not icon.isNull(): cell.setIcon(icon)
                        else: cell.setText("-")
                    except Exception:
                        cell.setText("?")
                elif col == 1:
                    try: cell = NumericTableWidgetItem(int(row_data[0]))
                    except: cell = QtWidgets.QTableWidgetItem("-")
                elif col == 2:
                    cell = QtWidgets.QTableWidgetItem(str(row_data[1]))
                elif col == 3:
                    cell = QtWidgets.QTableWidgetItem(str(row_data[2]))
                elif col == 4:
                    cell = QtWidgets.QTableWidgetItem(file_basename)
                elif col == 5:
                    try:
                        size_bytes = int(row_data[4]) if row_data[4] is not None else 0
                        if display_multiplier > 1:
                            display_item_str = f"{(size_bytes / display_multiplier):.2f} {display_unit_name}"
                        else: display_item_str = f"{size_bytes} Bytes"
                        cell = NumericTableWidgetItem(size_bytes, display_item_str)
                        cell.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                    except: cell = QtWidgets.QTableWidgetItem("-")
                elif col == 6:
                    try:
                        mtime_timestamp = os.path.getmtime(full_path)
                        dt_object = datetime.datetime.fromtimestamp(mtime_timestamp)
                        display_item_str = dt_object.strftime("%Y-%m-%d %H:%M")
                        cell = QtWidgets.QTableWidgetItem(display_item_str)
                        cell.setTextAlignment(QtCore.Qt.AlignCenter)
                    except FileNotFoundError:
                        cell = QtWidgets.QTableWidgetItem("(nicht gefunden)")
                        cell.setForeground(QtGui.QBrush(QtCore.Qt.gray))
                        cell.setTextAlignment(QtCore.Qt.AlignCenter)
                    except Exception:
                        cell = QtWidgets.QTableWidgetItem("(Datum Fehler)")
                        cell.setForeground(QtGui.QBrush(QtCore.Qt.red))
                        cell.setTextAlignment(QtCore.Qt.AlignCenter)
                if cell is None: cell = QtWidgets.QTableWidgetItem("-")
                self.table.setItem(row, col, cell)
        self.table.setUpdatesEnabled(True)
        self.table.setSortingEnabled(True)
        self.table.resizeColumnsToContents()
        self.table.setColumnWidth(0, 30)
        self.update_action_buttons_state()
        # Statuslabel setzen
        if warnung_gesetzt:
            self.status_total_label.setText(f"Warnung: {result_count} potenzielle Treffer gefunden (>1000). Datumsfilter wird jetzt angewendet...")
            self.status_total_label.setStyleSheet("background-color: yellow; color: black; font-weight: bold;")
            self.status_selected_label.setText("Ausgewählt: 0")
        else:
            self.status_total_label.setText(f"Gefunden: {len(filtered_results)}")
            self.status_total_label.setStyleSheet("")
        self._update_status_bar()
        self.search_button.setEnabled(True)
        QtWidgets.QApplication.restoreOverrideCursor()
    
    def get_selected_file_info(self, require_single=True): # require_single Flag hinzugefügt
        """Holt Infos zur ausgewählten Datei(en)."
           Gibt bei require_single=True ein einzelnes dict oder None zurück.
           Gibt bei require_single=False eine Liste von dicts oder eine leere Liste zurück.
        """
        selected_rows = sorted(list(set(index.row() for index in self.table.selectedIndexes()))) # Eindeutige, sortierte Zeilenindizes

        if not selected_rows:
            if require_single:
                QtWidgets.QMessageBox.warning(self, "Warnung", "Bitte wählen Sie mindestens eine Datei aus.")
            return None if require_single else []

        if require_single and len(selected_rows) > 1:
            QtWidgets.QMessageBox.warning(self, "Warnung", "Diese Aktion unterstützt nur die Auswahl einer einzelnen Datei.")
            return None
            
        # Informationen für alle ausgewählten Zeilen sammeln
        all_file_infos = []
        for row in selected_rows:
            # ACHTUNG: Indizes angepasst wegen Icon-Spalte!
            if self.table.item(row, 1) is None:
                continue # ID prüfen (Spalte 1)
            try:
                file_info = {
                    "id": self.table.item(row, 1).text(),        # war 0
                    "drive": self.table.item(row, 2).text(),      # war 1
                    "directory": self.table.item(row, 3).text(),  # war 2
                    "filename": self.table.item(row, 4).text()   # war 3
                }
                # Pfad bauen (Logik bleibt gleich)
                drive_part = file_info["drive"]
                dir_part = file_info["directory"]
                file_basename = file_info["filename"]
                if drive_part.endswith(':') or drive_part.endswith(':\\'):
                    dir_part = dir_part.lstrip('\\/')
                file_info["full_path"] = os.path.normpath(os.path.join(drive_part, dir_part, file_basename))
                file_info["folder_path"] = os.path.dirname(file_info["full_path"])
                all_file_infos.append(file_info)
            except Exception as e:
                print(f"Fehler beim Extrahieren der Datei-Infos für Zeile {row}: {e}")
                pass

        if require_single:
            return all_file_infos[0] if all_file_infos else None
        else:
            return all_file_infos
    
    def _remove_entry_from_db(self, file_id):
        """Entfernt einen einzelnen Eintrag aus der files Tabelle."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM files WHERE id = ?", (file_id,))
            self.conn.commit()
            print(f"DB Eintrag entfernt (ID: {file_id})") # Info für Konsole/Log
            return True
        except sqlite3.Error as e:
            QtWidgets.QMessageBox.critical(self, "DB Fehler", f"Fehler beim Entfernen des DB-Eintrags (ID: {file_id}): {e}")
            return False
    
    def open_file(self):
        info = self.get_selected_file_info(require_single=True)
        if not info:
            return
        if not os.path.exists(info["full_path"]):
            # --- NEU: Direkte Entfernung aus DB --- 
            print(f"Datei nicht gefunden: {info['full_path']}. Entferne DB-Eintrag...")
            if self._remove_entry_from_db(info["id"]):
                 # Optional: Statusmeldung im UI (z.B. Statusleiste)
                 # self.statusBar().showMessage("Veralteter DB-Eintrag entfernt.", 3000)
                 self.search_files() # Ergebnisse aktualisieren
            # --- Ende NEU --- 
            return # Aktion hier beenden, da Datei fehlt
            
        # Ursprüngliche Funktionalität, wenn Datei existiert:
        open_file_with_default_app(info["full_path"])
    
    def delete_file(self):
        selected_files = self.get_selected_file_info(require_single=False) # Mehrfachauswahl erlauben
        if not selected_files:
            return
        count = len(selected_files)
        filenames_preview = "\n".join([f" - {f['filename']}" for f in selected_files[:5]])
        if count > 5:
            filenames_preview += "\n... und weitere"
        reply = QtWidgets.QMessageBox.question(self, "Löschen bestätigen", \
                                               f"Möchten Sie die ausgewählten {count} Datei(en) wirklich unwiderruflich löschen?\n{filenames_preview}",
                                               QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, QtWidgets.QMessageBox.No)
        if reply != QtWidgets.QMessageBox.Yes:
            return
        deleted_count = 0
        db_errors = 0
        fs_errors = 0
        ids_to_delete_db = []
        progress_dialog = QtWidgets.QProgressDialog("Lösche Dateien...", "Abbrechen", 0, count, self)
        progress_dialog.setWindowModality(QtCore.Qt.WindowModal)
        progress_dialog.setWindowTitle("Löschen")
        progress_dialog.setValue(0)
        progress_dialog.show()
        for i, info in enumerate(selected_files):
            progress_dialog.setValue(i)
            progress_dialog.setLabelText(f"Lösche: {info['filename']}...")
            if progress_dialog.wasCanceled():
                break
            try:
                # Physisch löschen (falls vorhanden)
                if os.path.exists(info["full_path"]):
                    os.remove(info["full_path"])
                    ids_to_delete_db.append(info["id"])
                    deleted_count += 1
                else:
                    ids_to_delete_db.append(info["id"])
                    deleted_count += 1 # Zählen wir als gelöscht (aus DB Sicht)
            except Exception as e:
                fs_errors += 1
                print(f"Fehler beim physischen Löschen von {info['full_path']}: {e}")
        progress_dialog.setLabelText("Aktualisiere Datenbank...")
        progress_dialog.setValue(count) # Endwert für DB
        if ids_to_delete_db:
            try:
                cursor = self.conn.cursor()
                placeholders = ', '.join('?' * len(ids_to_delete_db))
                sql = f"DELETE FROM files WHERE id IN ({placeholders})"
                cursor.execute(sql, ids_to_delete_db)
                self.conn.commit()
            except sqlite3.Error as e:
                db_errors += 1
                QtWidgets.QMessageBox.critical(self, "DB Fehler", f"Fehler beim Löschen aus der DB: {e}")
        progress_dialog.close()
        msg = f"{deleted_count} von {count} Datei(en)/Einträge(n) wurden entfernt."
        if fs_errors > 0:
            msg += f"\n{fs_errors} Fehler beim physischen Löschen (Details siehe Konsole)."
        if db_errors > 0:
            msg += f"\n{db_errors} Fehler beim Löschen aus der Datenbank."
        QtWidgets.QMessageBox.information(self, "Löschen abgeschlossen", msg)
        self.search_files()
    
    def preview_file(self):
        info = self.get_selected_file_info(require_single=True)
        if not info:
            return
        if not os.path.exists(info["full_path"]):
            # --- NEU: Direkte Entfernung aus DB (identisch zu open_file) --- 
            print(f"Datei nicht gefunden: {info['full_path']}. Entferne DB-Eintrag...")
            if self._remove_entry_from_db(info["id"]):
                 # Optional: Statusmeldung im UI
                 # self.statusBar().showMessage("Veralteter DB-Eintrag entfernt.", 3000)
                 self.search_files() # Ergebnisse aktualisieren
            # --- Ende NEU --- 
            return # Aktion hier beenden
            
        # Ursprüngliche Funktionalität (Vorschau), wenn Datei existiert:
        ext = os.path.splitext(info["filename"])[1].lower()
        if ext in ['.txt', '.py', '.log', '.md']:
            try:
                with open(info["full_path"], 'r', encoding='utf-8') as f:
                    content = f.read()
                preview_dialog = QtWidgets.QDialog(self)
                preview_dialog.setWindowTitle("Vorschau - " + info["filename"])
                layout = QtWidgets.QVBoxLayout()
                text_edit = QtWidgets.QTextEdit()
                text_edit.setReadOnly(True)
                text_edit.setText(content)
                layout.addWidget(text_edit)
                preview_dialog.setLayout(layout)
                preview_dialog.resize(600,400)
                preview_dialog.exec_()
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Fehler", f"Fehler bei der Vorschau: {e}")
        elif ext in ['.jpg', '.jpeg', '.png', '.bmp', '.gif']:
            try:
                preview_dialog = QtWidgets.QDialog(self)
                preview_dialog.setWindowTitle("Vorschau - " + info["filename"])
                layout = QtWidgets.QVBoxLayout()
                label = QtWidgets.QLabel()
                pixmap = QtGui.QPixmap(info["full_path"])
                label.setPixmap(pixmap.scaled(600,400, QtCore.Qt.KeepAspectRatio))
                layout.addWidget(label)
                preview_dialog.setLayout(layout)
                preview_dialog.exec_()
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Fehler", f"Fehler bei der Bildvorschau: {e}")
        else:
            # Fallback: Datei öffnen
            open_file_with_default_app(info["full_path"])
    
    def move_file(self):
        # TODO: Auf Mehrfachauswahl anpassen?
        info = self.get_selected_file_info(require_single=True) # Aktuell nur für eine Datei
        if not info:
            return

        # --- NEU: Prüfung auf Existenz vor dem Verschieben --- 
        if not os.path.exists(info["full_path"]):
            print(f"Datei für Verschieben nicht gefunden: {info['full_path']}. Entferne DB-Eintrag...")
            if self._remove_entry_from_db(info["id"]):
                 # Optional: Statusmeldung
                 self.search_files() # Ergebnisse aktualisieren
            return # Aktion abbrechen
        # --- Ende NEU --- 

        new_directory = QtWidgets.QFileDialog.getExistingDirectory(self, "Neues Verzeichnis auswählen")
        if not new_directory:
            return
        new_full_path = os.path.join(new_directory, info["filename"])
        try:
            # Physisch verschieben
            shutil.move(info["full_path"], new_full_path)
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Fehler", f"Fehler beim Verschieben: {e}")
            # Hier den DB-Eintrag *nicht* entfernen, da das Verschieben selbst fehlschlug (evtl. Zielproblem)
            return
        # DB aktualisieren (wie zuvor)
        try:
            cursor = self.conn.cursor()
            # Angepasst für optimierte Datenbankstruktur
            cursor.execute("SELECT id FROM directories WHERE drive_id = (SELECT id FROM drives WHERE name = ?) AND full_path = ?", 
                           (info["drive"], new_directory))
            row = cursor.fetchone()
            if row:
                new_directory_id = row[0]
            else:
                # Neues Verzeichnis mit optimierter Funktion erstellen
                try:
                    from models import get_db_instance
                    db_manager = get_db_instance()
                    drive_id = db_manager.get_or_create_drive(info["drive"])
                    new_directory_id = db_manager.get_or_create_directory_optimized(drive_id, new_directory)
                except Exception as e:
                    QtWidgets.QMessageBox.critical(self, "DB Fehler", f"Fehler beim Erstellen des neuen Verzeichnisses: {e}")
                    return
            # DB-Eintrag aktualisieren
            cursor.execute("UPDATE files SET directory_id = ? WHERE id = ?", (new_directory_id, info["id"]))
            self.conn.commit()
            QtWidgets.QMessageBox.information(self, "Erfolg", "Datei wurde verschoben und DB aktualisiert.")
            self.search_files()
        except sqlite3.Error as e:
            QtWidgets.QMessageBox.critical(self, "DB Fehler", f"Fehler beim Aktualisieren der DB nach Verschieben: {e}")
            # Hier könnte man auch versuchen, das Verschieben rückgängig zu machen
    
    def rename_file(self):
        info = self.get_selected_file_info(require_single=True) # Nur für eine Datei
        if not info:
            return

        # --- NEU: Prüfung auf Existenz vor dem Umbenennen --- 
        if not os.path.exists(info["full_path"]):
            print(f"Datei für Umbenennen nicht gefunden: {info['full_path']}. Entferne DB-Eintrag...")
            if self._remove_entry_from_db(info["id"]):
                 # Optional: Statusmeldung
                 self.search_files() # Ergebnisse aktualisieren
            return # Aktion abbrechen
        # --- Ende NEU --- 

        new_name, ok = QtWidgets.QInputDialog.getText(self, "Umbenennen", "Neuen Dateinamen eingeben:", text=info['filename']) # Originalname vorschlagen
        if not ok or not new_name.strip():
            return
        new_name = new_name.strip()
        # Prüfen, ob Name geändert wurde
        if new_name == info['filename']:
            return # Kein Name geändert
            
        new_full_path = os.path.join(os.path.dirname(info["full_path"]), new_name)
        
        # Prüfen, ob Zieldatei schon existiert
        if os.path.exists(new_full_path):
            QtWidgets.QMessageBox.warning(self, "Fehler", f"Eine Datei mit dem Namen '{new_name}' existiert bereits in diesem Ordner.")
            return
            
        try:
            # Physisch umbenennen
            os.rename(info["full_path"], new_full_path)
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Fehler", f"Fehler beim Umbenennen: {e}")
            # Hier den DB-Eintrag *nicht* entfernen, da das Umbenennen selbst fehlschlug
            return
        # DB aktualisieren (wie zuvor)
        try:
            # Angepasst für optimierte Datenbankstruktur - filename und extension trennen
            filename_only, ext = os.path.splitext(new_name)
            ext = ext if ext else None
            
            cursor = self.conn.cursor()
            
            # Extension-ID ermitteln
            if ext:
                cursor.execute("SELECT id FROM extensions WHERE name = ?", (ext,))
                ext_row = cursor.fetchone()
                if ext_row:
                    ext_id = ext_row[0]
                else:
                    # Neue Extension erstellen
                    from models import get_db_instance
                    db_manager = get_db_instance()
                    ext_id = db_manager.get_or_create_extension(ext)
            else:
                cursor.execute("SELECT id FROM extensions WHERE name = '[none]'")
                ext_row = cursor.fetchone()
                ext_id = ext_row[0] if ext_row else None
            
            # Datei-Eintrag aktualisieren
            cursor.execute("UPDATE files SET filename = ?, extension_id = ? WHERE id = ?", 
                          (filename_only, ext_id, info["id"]))
            self.conn.commit()
            QtWidgets.QMessageBox.information(self, "Erfolg", "Datei umbenannt (physisch und in DB aktualisiert).")
            self.search_files()
        except sqlite3.Error as e:
            QtWidgets.QMessageBox.critical(self, "DB Fehler", f"Fehler beim Aktualisieren der DB nach Umbenennen: {e}")
            # Hier könnte man versuchen, das Umbenennen rückgängig zu machen?
    
    # --- NEUE Handler für Kontextmenü ---
    def copy_path(self):
        """Kopiert den vollständigen Pfad der ausgewählten Datei(en) in die Zwischenablage."""
        selected_files = self.get_selected_file_info(require_single=False) # Mehrfachauswahl erlauben
        if not selected_files:
            return
            
        try:
            # Sammle alle Pfade
            paths = [info["full_path"] for info in selected_files]
            # Füge sie mit Zeilenumbruch zusammen
            text_to_copy = "\n".join(paths)
            
            clipboard = QtWidgets.QApplication.clipboard()
            clipboard.setText(text_to_copy)
            # Optional: Kurze Bestätigung
            count = len(paths)
            # ENTFERNT: print(f"{count} Pfad(e) kopiert.") # Debug
            # Hier könnte man eine Statusleiste nutzen (später)
            # self.statusBar().showMessage(f"{count} Pfad(e) kopiert.", 2000) 
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Fehler", f"Fehler beim Kopieren der Pfade: {e}")

    def open_in_explorer(self):
        """Öffnet den Ordner der ausgewählten Datei im Dateimanager."""
        info = self.get_selected_file_info(require_single=True) # Nur für eine Datei
        if not info:
            return
        if not os.path.exists(info["folder_path"]):
            QtWidgets.QMessageBox.critical(self, "Fehler", f"Übergeordneter Ordner '{info['folder_path']}' nicht gefunden.")
            return
        open_folder_in_explorer(info["folder_path"])
    # --- Ende neue Handler ---
    
    # --- NEUE Handler für Admin-Explorer ---
    def open_in_explorer_admin(self):
        """Öffnet den Ordner der ausgewählten Datei im Dateimanager als Admin (nur Windows)."""
        info = self.get_selected_file_info(require_single=True)
        if not info:
            return
        if not os.path.exists(info["folder_path"]):
            # Hier evtl. auch DB bereinigen? Vorerst nur Fehler.
            QtWidgets.QMessageBox.critical(self, "Fehler", f"Übergeordneter Ordner '{info['folder_path']}' nicht gefunden.")
            return
        open_folder_in_explorer(info["folder_path"], as_admin=True)
    # --- Ende neue Handler ---
    
    # --- NEUE METHODEN für Teilen & Button Status ---
    def update_action_buttons_state(self):
        """Aktiviert/Deaktiviert die Aktionsbuttons basierend auf der Tabellenauswahl."""
        selected_rows_count = len(list(set(index.row() for index in self.table.selectedIndexes())))
        
        single_select_buttons = [self.open_button, self.preview_button, 
                                 self.rename_button, self.move_button, self.share_button]
        for button in single_select_buttons:
            button.setEnabled(selected_rows_count == 1)
            
        # Buttons, die MINDESTENS EINE Auswahl benötigen
        multi_select_buttons = [self.delete_button] # Nur der Löschen-Button in der unteren Leiste
        for button in multi_select_buttons:
            button.setEnabled(selected_rows_count >= 1)
            
        # Statusleiste wird separat in _update_status_bar aktualisiert

    def show_share_menu(self):
        """Zeigt das Menü für den Teilen-Button an."""
        info = self.get_selected_file_info(require_single=True)
        if not info:
            return # Sollte durch Button-Deaktivierung nicht passieren, aber sicher ist sicher

        share_menu = QtWidgets.QMenu(self)
        copy_path_action = share_menu.addAction("📋 Pfad kopieren")
        copy_html_action = share_menu.addAction("🔗 Als HTML-Link kopieren")
        # Weitere Aktionen hier hinzufügen (z.B. "Nachricht mit Pfad kopieren")

        # Menü unter dem Button anzeigen
        button_pos = self.share_button.mapToGlobal(QtCore.QPoint(0, self.share_button.height()))
        action = share_menu.exec_(button_pos)

        # Ausgewählte Aktion ausführen
        if action == copy_path_action:
            self.copy_path() # Bestehende Funktion nutzen
        elif action == copy_html_action:
            self.copy_html_link()

    def copy_html_link(self):
        """Kopiert einen HTML-Link (<a> Tag) zur ausgewählten Datei in die Zwischenablage."""
        info = self.get_selected_file_info(require_single=True)
        if not info:
            return
        try:
            # Normiere Pfad und erstelle file:// URL (sollte auch mit Leerzeichen etc. umgehen)
            normalized_path = os.path.normpath(info['full_path'])
            file_url = QtCore.QUrl.fromLocalFile(normalized_path).toString()
            
            # Escape Dateiname für HTML
            escaped_filename = QtGui.QTextDocumentFragment.fromPlainText(info["filename"]).toHtml()
            # Entferne ggf. von toHtml() hinzugefügte Paragraphen-Tags etc.
            # Einfacher Ansatz: direktes Escaping
            escaped_filename_simple = info["filename"].replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            
            html_link = f'<a href="{file_url}">{escaped_filename_simple}</a>'
            
            clipboard = QtWidgets.QApplication.clipboard()
            clipboard.setText(html_link)
            # Optional: Bestätigung
            print(f"HTML-Link kopiert: {html_link}") # Debug
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Fehler", f"Fehler beim Kopieren des HTML-Links: {e}")
    # --- Ende neue Methoden ---
    
    # --- NEUE Methoden zum Steuern der Filter-UI ---
    def _update_size_filter_state(self):
        """Aktiviert/Deaktiviert die Größen-Eingabefelder basierend auf dem Operator."""
        op = self.size_op_combo.currentText()
        is_between = (op == "Zwischen")
        is_active = (op != "Egal")
        
        self.size_input1.setEnabled(is_active)
        self.size_unit_combo.setEnabled(is_active)
        self.size_label_between.setVisible(is_between)
        self.size_input2.setEnabled(is_between)
        self.size_input2.setVisible(is_between)

    def _update_date_filter_state(self):
        """Aktiviert/Deaktiviert die Datums-Eingabefelder basierend auf dem Operator."""
        op = self.date_op_combo.currentText()
        is_between = (op == "Zwischen")
        is_active = (op != "Egal")
        
        self.date_edit1.setEnabled(is_active)
        self.date_label_between.setVisible(is_between)
        self.date_edit2.setEnabled(is_between)
        self.date_edit2.setVisible(is_between)
    
    def closeEvent(self, event):
        if self.conn:
            self.conn.close()
        event.accept()

    # NEU: Methode zum Aktualisieren der Statusleiste
    def _update_status_bar(self, error=False):
        """Aktualisiert die Zähler in der Statusleiste."""
        if error:
            total_count = 0
            selected_count = 0
            self.status_total_label.setText("Fehler!")
            self.status_selected_label.setText("Ausgewählt: 0")
        else:
            total_count = self.table.rowCount()
            selected_rows = list(set(index.row() for index in self.table.selectedIndexes()))
            selected_count = len(selected_rows)
            
            self.status_total_label.setText(f"Gefunden: {total_count}")
            self.status_selected_label.setText(f"Ausgewählt: {selected_count}")
        

def main():
    if len(sys.argv) < 2:
        print("Usage: {} <db_path>".format(sys.argv[0]))
        sys.exit(1)
    db_path = sys.argv[1]
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow(db_path)
    window.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()