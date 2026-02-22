# *** ENTFERNT: Debug-Import-Check ***
# DEBUG_FILE = r"C:\TempServiceTest\watchdog_startup_debug.txt"
# try:
#     with open(DEBUG_FILE, "a") as f: f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Loading watchdog_monitor.py\n")
# except: pass

import os
import time
import sys
import logging
import sqlite3
# *** ENTFERNT: Debug-Import-Check ***
# try:
#     with open(DEBUG_FILE, "a") as f: f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - watchdog_monitor: Imported os, time, sys, sqlite3.\n")
# except: pass

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from drive_alias_detector import normalize_path_with_aliases, get_drive_mapping
from scan_lock import should_fs_write

# Globale Variable für Drive-Mappings (wird einmal geladen)
_drive_mappings = None

def _get_drive_mappings():
    """Hole Drive-Mappings (einmal cachen für Performance)"""
    global _drive_mappings
    if _drive_mappings is None:
        _drive_mappings = get_drive_mapping()
    return _drive_mappings

def _normalize_path_for_watchdog(path):
    """Normalisiert Pfad unter Berücksichtigung von Laufwerk-Aliases"""
    try:
        mappings = _get_drive_mappings()
        normalized_path, is_alias, orig_drive, real_drive = normalize_path_with_aliases(path, mappings)
        if is_alias:
            logger.info(f"[Watchdog Alias] Konvertiert {path} -> {normalized_path}")
        return normalized_path
    except Exception as e:
        logger.warning(f"[Watchdog Alias-Fehler] {path}: {e}")
        return os.path.normpath(path)  # Fallback
# *** ENTFERNT: Debug-Import-Check ***
# try:
#     with open(DEBUG_FILE, "a") as f: f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - watchdog_monitor: Imported watchdog.\n")
# except: pass

# *** ENTFERNT: Debug-Import-Check für logging ***
# import logging
# try:
#     with open(DEBUG_FILE, "a") as f: f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - watchdog_monitor: Imported logging.\n")
# except: pass


# Importiere aus utils und models
try:
    from utils import (calculate_hash, HASHING, DB_PATH, CONFIG, 
                       load_config, logger, LOG_PATH, PROJECT_DIR)
    # *** ENTFERNT: Debug-Import-Check ***
    # with open(DEBUG_FILE, "a") as f: f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - watchdog_monitor: Imported from utils.\n")
except Exception as utils_ex:
    # *** ENTFERNT: Debug-Logging im Fehlerfall ***
    # with open(DEBUG_FILE, "a") as f: f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - watchdog_monitor: EXCEPTION importing from utils: {utils_ex}\n")
    raise # Fehler weiter werfen, damit Hauptskript ihn bemerkt

try:
    from models import get_db_instance, _db_lock
    # *** ENTFERNT: Debug-Import-Check ***
    # with open(DEBUG_FILE, "a") as f: f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - watchdog_monitor: Imported from models.\n")
except Exception as models_ex:
    # *** ENTFERNT: Debug-Logging im Fehlerfall ***
    # with open(DEBUG_FILE, "a") as f: f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - watchdog_monitor: EXCEPTION importing from models: {models_ex}\n")
    raise # Fehler weiter werfen

# Prozessweiter Lock für DB-Reinitialisierung:
# Verhindert, dass mehrere FSHandler-Threads gleichzeitig close() + reinit() aufrufen
# (führt sonst zu "Recursive use of cursors" / "Cannot operate on closed database")
import threading as _threading
_reinit_lock = _threading.Lock()


# --- NEU: Listen für zu ignorierende Pfade und Dateien ---
# Verzeichnisse, die häufig Hintergrundaktivitäten aufweisen
# Wichtig: Pfade müssen normalisiert sein (os.path.normpath, os.path.abspath)
# Verwende Kleinbuchstaben für den Vergleich
IGNORE_DIR_PREFIXES = [
    os.path.normpath(os.environ.get("WINDIR", "C:\\Windows")).lower(),          # C:\Windows
    os.path.normpath(os.environ.get("PROGRAMDATA", "C:\\ProgramData")).lower(), # C:\ProgramData
    # WICHTIG: AppData NICHT komplett ignorieren - enthält wichtige Anwendungsdaten!
    # Stattdessen nur spezifische Unterordner ignorieren:
    os.path.normpath(os.path.join(os.environ.get("LOCALAPPDATA", ""), "Temp")).lower() if os.environ.get("LOCALAPPDATA") else "",  # Nur AppData\Local\Temp
    os.path.normpath(os.path.join(os.environ.get("APPDATA", ""), "Microsoft\\Windows\\Recent")).lower() if os.environ.get("APPDATA") else "",  # Nur Recent Items
    os.path.normpath(os.environ.get("TEMP", "")).lower(),                    # Temp-Ordner
    os.path.normpath(os.environ.get("TMP", "")).lower(),                     # Alternativer Temp-Ordner
    "\\$recycle.bin", # Papierkorb (prüft, ob Pfad *enthält*, da Laufwerksbuchstabe variiert)
    os.path.normpath(os.path.join(PROJECT_DIR, "venv")).lower() # Virtuelle Umgebung im Projekt
]
# Entferne leere Einträge, falls Umgebungsvariablen nicht gesetzt sind
IGNORE_DIR_PREFIXES = [p for p in IGNORE_DIR_PREFIXES if p]

# Dateiendungen, die oft temporär oder System-bezogen sind
IGNORE_EXTENSIONS = [
    ".tmp", ".log", ".etl", ".pf", ".lnk", ".ini", ".bak", ".cache", ".part", ".crdownload", 
    ".db-shm", ".db-wal", ".db-journal"  # Explizit SQLite-Dateien ignorieren
]

# Spezifische Dateien zum Ignorieren (zusätzlich zu LOG_PATH)
IGNORE_FILES = [
    os.path.normpath(DB_PATH).lower(), # Die Datenbankdatei selbst
    os.path.normpath(os.path.join(PROJECT_DIR, "config.json")).lower(), # Die Konfigurationsdatei
    os.path.normpath(DB_PATH + "-shm").lower(),  # SQLite shared memory file
    os.path.normpath(DB_PATH + "-wal").lower(),  # SQLite write-ahead log
    os.path.normpath(DB_PATH + "-journal").lower()  # SQLite journal file
]

# --- Spezifische Pfade, die auf jeden Fall ignoriert werden sollen ---
IGNORE_FILENAMES = [
    "desktop.ini",  # Windows-Desktopkonfiguration
    "thumbs.db",    # Windows-Miniaturansichten
    ".ds_store"     # macOS Verzeichnisattribute
]
# --- Ende Ignorier-Listen ---

class FSHandler(FileSystemEventHandler):
    """Behandelt Dateisystemereignisse und aktualisiert die Datenbank."""
    def __init__(self, path_to_watch):
        super().__init__()
        self.path_to_watch = os.path.normpath(os.path.abspath(path_to_watch))
        
        # WICHTIG: Normalisiere den Pfad für Alias-Laufwerke
        normalized_path = _normalize_path_for_watchdog(self.path_to_watch)
        
        # Bestimme das Laufwerk aus dem NORMALISIERTEN Pfad
        drive_letter, _ = os.path.splitdrive(normalized_path)
        # Standardisiere den Laufwerksnamen (z.B. "C:/")
        self.drive_name = drive_letter.upper() + "/" if drive_letter else "UNKNOWN/"
        
        logger.info(f"[Watchdog Init] Überwache: {self.path_to_watch}")
        logger.info(f"[Watchdog Init] Normalisiert zu: {normalized_path}")
        logger.info(f"[Watchdog Init] Drive: {self.drive_name}")
        
        self.db = None # Wird bei Bedarf initialisiert
        self.drive_id = None # Wird bei Bedarf initialisiert
        self._initialize_db()

    def _initialize_db(self):
        """Initialisiert die DB-Verbindung und holt die drive_id.
        Retry-Loop schützt gegen Race Condition zwischen get_db_instance() und get_or_create_drive().
        """
        last_error = None
        for attempt in range(5):
            try:
                self.db = get_db_instance()
                if not self.db:
                    logger.error("[Watchdog-Fehler] Konnte keine DB-Instanz erhalten.")
                    self.db = None
                    self.drive_id = None
                    return
                self.drive_id = self.db.get_or_create_drive(self.drive_name)
                if self.drive_id is None:
                    logger.error(f"[Watchdog-Fehler] Konnte drive_id für {self.drive_name} nicht ermitteln.")
                return  # Erfolg
            except Exception as e:
                last_error = e
                if "closed database" in str(e).lower() and attempt < 4:
                    time.sleep(0.1 * (attempt + 1))  # Kurz warten, dann erneut versuchen
                    continue
                break

        logger.error(f"[Watchdog-Fehler] Kritischer Fehler bei DB-Initialisierung: {last_error}")
        self.db = None
        self.drive_id = None

    def _reinitialize_db_if_needed(self):
        """Prüft, ob eine DB-Verbindung besteht und versucht ggf. neu zu initialisieren.

        Verwendet _reinit_lock damit niemals zwei FSHandler-Threads gleichzeitig
        close() + reinit() aufrufen → verhindert "Recursive use of cursors" und
        "Cannot operate on closed database".
        """
        # Schnellpfad ohne Lock: Wenn alles ok, sofort zurück
        if self.db is not None and self.drive_id is not None:
            try:
                # conn.execute() ist thread-sicherer als cursor.execute() da kein
                # geteilter Cursor-Zustand verwendet wird (jede Verbindung hat ihren eigenen)
                self.db.conn.execute("SELECT 1")
                return True
            except Exception as e:
                logger.warning(f"[Watchdog] DB-Verbindung unterbrochen: {e}. Versuche Neuinitialisierung...")
                # Fällt durch zum Lock-geschützten Reinitialisierungspfad

        # Lock-geschützter Reinitialisierungspfad: Nur ein Thread auf einmal
        with _reinit_lock:
            # Doppelte Prüfung: Vielleicht hat ein anderer Thread schon reinitialisiert
            if self.db is not None and self.drive_id is not None:
                try:
                    self.db.conn.execute("SELECT 1")
                    return True  # Anderer Thread hat bereits wiederhergestellt
                except Exception:
                    pass  # Immer noch kaputt, wir machen weiter

            if self.db is None or self.drive_id is None:
                logger.info("[Watchdog] DB-Instanz oder drive_id fehlt. Versuche Wiederherstellung...")

            # Verbindung ist unterbrochen - Neuinitialisierung
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    time.sleep(1)  # Kurze Pause
                    if self.db is not None and hasattr(self.db, 'close'):
                        try:
                            self.db.close()  # DBManager.close() → setzt Singleton korrekt zurück
                        except Exception:
                            pass

                    self.db = None
                    self._initialize_db()

                    if self.db is not None and self.drive_id is not None:
                        # Test der neuen Verbindung
                        self.db.conn.execute("SELECT 1")
                        logger.info(f"[Watchdog] DB-Verbindung erfolgreich wiederhergestellt (Versuch {attempt + 1}).")
                        return True

                except Exception as reinit_ex:
                    logger.error(f"[Watchdog] Fehler bei DB-Wiederherstellung (Versuch {attempt + 1}): {reinit_ex}")

            logger.error("[Watchdog] Kritisch: DB-Verbindung konnte nicht wiederhergestellt werden.")
            return False

    # --- NEU: Hilfsfunktion zum Prüfen, ob ein Pfad ignoriert werden soll ---
    def _is_ignored(self, path):
        """Prüft, ob ein gegebener Pfad ignoriert werden soll."""
        try:
            norm_path = os.path.normpath(path).lower()
            
            # 0. Prüfe Dateiname (unabhängig vom Pfad)
            file_name = os.path.basename(norm_path).lower()
            if file_name in IGNORE_FILENAMES:
                return True

            # 1. Prüfe spezifische Dateien (Log, DB, Config)
            if norm_path == LOG_PATH.lower() or norm_path in IGNORE_FILES:
                # logger.debug(f"[Ignoriert] Spezifische Datei: {path}")
                return True

            # 2. Prüfe Verzeichnis-Präfixe
            for prefix in IGNORE_DIR_PREFIXES:
                if norm_path.startswith(prefix):
                    # logger.debug(f"[Ignoriert] Verzeichnis-Präfix '{prefix}': {path}")
                    return True
                # Sonderfall Papierkorb prüfen
                if "\\$recycle.bin" in prefix and "\\$recycle.bin" in norm_path:
                     # logger.debug(f"[Ignoriert] Papierkorb-Pfad: {path}")
                     return True

            # 3. Prüfe Dateiendungen (nur wenn es keine Directory ist)
            # Vorsicht: os.path.isdir kann fehlschlagen, wenn Datei nicht mehr existiert (bei on_deleted)
            # Wir prüfen daher nur die Endung, auch wenn es ein Ordner mit Punkt sein könnte.
            _, ext = os.path.splitext(norm_path)
            if ext and ext.lower() in IGNORE_EXTENSIONS:
                # logger.debug(f"[Ignoriert] Dateiendung '{ext}': {path}")
                return True

        except Exception as e:
            # Bei Fehlern in der Prüfung sicherheitshalber nicht ignorieren und loggen
            logger.warning(f"[Ignore Check Fehler] Fehler bei Prüfung von '{path}': {e}. Pfad wird NICHT ignoriert.")
            return False

        return False # Standard: nicht ignorieren
    # --- Ende Hilfsfunktion ---

    def on_created(self, event):
        """Behandelt das Erstellen von Dateien oder Verzeichnissen."""
        # *** NEU: Prüfung am Anfang ***
        if self._is_ignored(event.src_path):
            return
        if not should_fs_write():
            logger.debug(f"[ScanLock] on_created übersprungen (Scan läuft): {event.src_path}")
            return

        if not self._reinitialize_db_if_needed(): return
        try:
            # WICHTIG: Verwende Alias-bewusste Pfad-Normalisierung
            src_path = _normalize_path_for_watchdog(event.src_path)
            if event.is_directory:
                self._handle_new_directory(src_path)
            else:
                self._insert_or_update_file(src_path)
        except Exception as e:
            logger.error(f"[Watchdog Create-Fehler] {event.src_path}: {e}")

    def on_modified(self, event):
        """Behandelt das Ändern von Dateien."""
        # *** NEU: Prüfung am Anfang ***
        if self._is_ignored(event.src_path):
            return
        if not should_fs_write():
            logger.debug(f"[ScanLock] on_modified übersprungen (Scan läuft): {event.src_path}")
            return

        if not self._reinitialize_db_if_needed(): return
        try:
            # WICHTIG: Verwende Alias-bewusste Pfad-Normalisierung
            src_path = _normalize_path_for_watchdog(event.src_path)
            if not event.is_directory:
                if os.path.exists(src_path):
                    self._insert_or_update_file(src_path)
        except Exception as e:
            logger.error(f"[Watchdog Modify-Fehler] {event.src_path}: {e}")

    def on_moved(self, event):
        """Behandelt das Verschieben/Umbenennen von Dateien oder Verzeichnissen."""
        # *** NEU: Prüfung am Anfang (Quelle UND Ziel) ***
        if self._is_ignored(event.src_path) or self._is_ignored(event.dest_path):
            return
        if not should_fs_write():
            logger.debug(f"[ScanLock] on_moved übersprungen (Scan läuft): {event.src_path}")
            return

        if not self._reinitialize_db_if_needed(): return

        # WICHTIG: Verwende Alias-bewusste Pfad-Normalisierung
        src_path = _normalize_path_for_watchdog(event.src_path)
        dest_path = _normalize_path_for_watchdog(event.dest_path)
        with _db_lock:
            try:
                if event.is_directory:
                    logger.info(f"[Watchdog Move] Verzeichnis verschoben/umbenannt: {src_path} -> {dest_path}. Manuelle Prüfung/Rescan empfohlen.")
                else:
                    # Datei umbenannt/verschoben - für neue DB-Struktur
                    src_filename = os.path.basename(src_path)
                    dest_filename = os.path.basename(dest_path)
                    src_dir = os.path.dirname(src_path)
                    dest_dir = os.path.dirname(dest_path)
                    
                    # --- SMART SPLIT: Multi-Dot-Dateinamen korrekt behandeln ---
                    # os.path.splitext("Second.Try.TEST.xml") → ("Second.Try.TEST", ".xml")
                    # Aber DB könnte "Second.Try" + ".xml" gespeichert haben.
                    # Strategie: Erst os.path.splitext versuchen, dann DB-basierter Fallback.
                    src_filename_only, src_ext = os.path.splitext(src_filename)
                    src_ext = src_ext if src_ext else '[none]'
                    
                    # DEBUG: Alle Rename-Details loggen
                    logger.info(f"[Watchdog Move DEBUG] src_path={src_path}")
                    logger.info(f"[Watchdog Move DEBUG] dest_path={dest_path}")
                    logger.info(f"[Watchdog Move DEBUG] src_dir={src_dir}, src_filename_only={src_filename_only}, src_ext={src_ext}")
                    logger.info(f"[Watchdog Move DEBUG] self.drive_id={self.drive_id}, self.drive_name={self.drive_name}")
                    logger.info(f"[Watchdog Move DEBUG] DB-Query: drive_id={self.drive_id}, full_path={src_dir.replace(chr(92), '/')}")
                    
                    # Finde die alte directory_id
                    self.db.cursor.execute("SELECT id FROM directories WHERE drive_id = ? AND full_path = ?", (self.drive_id, src_dir.replace("\\", "/")))
                    src_dir_row = self.db.cursor.fetchone()
                    if not src_dir_row:
                        # DEBUG: Zeige was in der DB für diesen Pfad existiert (ohne drive_id Filter)
                        self.db.cursor.execute("SELECT id, drive_id, full_path FROM directories WHERE full_path = ?", (src_dir.replace("\\", "/"),))
                        alt_row = self.db.cursor.fetchone()
                        if alt_row:
                            logger.warning(f"[Watchdog Move DEBUG] Pfad existiert aber mit anderer drive_id! DB: drive_id={alt_row[1]}, Watchdog: drive_id={self.drive_id}")
                        else:
                            logger.warning(f"[Watchdog Move DEBUG] Pfad existiert NICHT in DB: {src_dir.replace(chr(92), '/')}")
                        logger.warning(f"[Watchdog Move] Quell-Verzeichnis nicht gefunden: {src_dir}. Lege Ziel als neue Datei an.")
                        self._insert_or_update_file(dest_path)
                        return
                    src_dir_id = src_dir_row[0]
                    logger.info(f"[Watchdog Move DEBUG] src_dir_id={src_dir_id}")
                    
                    # Finde Extension-ID
                    ext_id = self.db.get_or_create_extension(src_ext)
                    logger.info(f"[Watchdog Move DEBUG] ext_id={ext_id} für '{src_ext}'")
                    
                    # --- PHASE 1: Exakter Lookup (os.path.splitext) ---
                    self.db.cursor.execute(
                        "SELECT id FROM files WHERE directory_id = ? AND filename = ? AND extension_id = ?",
                        (src_dir_id, src_filename_only, ext_id)
                    )
                    file_row = self.db.cursor.fetchone()
                    
                    # --- PHASE 2: Smart Fallback für Multi-Dot-Dateinamen ---
                    if not file_row and '.' in src_filename_only:
                        # src_filename_only enthält Punkte → könnte falsch gesplittet sein
                        # Suche in DB: Welche Datei in diesem Ordner hat einen Namen,
                        # der ein Präfix von src_filename ist?
                        # z.B. src_filename="Second.Try.TEST.xml" → DB hat "Second.Try" + ".xml"
                        logger.info(f"[Watchdog Move] Phase 2: Multi-Dot-Fallback für '{src_filename}'")
                        
                        # Hole alle Dateien in diesem Ordner deren Name ein Anfangsstück von src_filename_only ist
                        self.db.cursor.execute(
                            """SELECT f.id, f.filename, f.extension_id, e.name 
                               FROM files f 
                               JOIN extensions e ON f.extension_id = e.id 
                               WHERE f.directory_id = ? 
                               AND ? LIKE f.filename || '%'""",
                            (src_dir_id, src_filename_only)
                        )
                        candidates = self.db.cursor.fetchall()
                        
                        if candidates:
                            # Finde den besten Match: DB-filename + DB-extension muss dem
                            # vollständigen src_filename entsprechen (oder ein gültiger Split sein)
                            best_match = None
                            for cand_id, cand_name, cand_ext_id, cand_ext in candidates:
                                # Prüfe: cand_name + "." + rest + cand_ext == src_filename ?
                                # Also: src_filename muss mit cand_name beginnen und mit cand_ext enden
                                if src_filename.lower().startswith(cand_name.lower()) and src_filename.lower().endswith(cand_ext.lower()):
                                    # Gültiger Kandidat! Bevorzuge längsten DB-filename (genauester Match)
                                    if best_match is None or len(cand_name) > len(best_match[1]):
                                        best_match = (cand_id, cand_name, cand_ext_id, cand_ext)
                            
                            if best_match:
                                file_row = (best_match[0],)
                                # Korrigiere auch ext_id für den gefundenen Eintrag
                                ext_id = best_match[2]
                                logger.info(f"[Watchdog Move] Phase 2 TREFFER: DB-filename='{best_match[1]}', DB-ext='{best_match[3]}', file_id={best_match[0]}")
                            else:
                                logger.info(f"[Watchdog Move] Phase 2: {len(candidates)} Kandidaten, aber kein exakter Match")
                        else:
                            logger.info(f"[Watchdog Move] Phase 2: Keine Kandidaten gefunden")
                    
                    if not file_row:
                        # Weder Phase 1 noch Phase 2 hat die Datei gefunden
                        self.db.cursor.execute(
                            "SELECT id, filename, extension_id FROM files WHERE directory_id = ? AND filename LIKE ?",
                            (src_dir_id, f"%{src_filename_only[:10]}%")
                        )
                        similar = self.db.cursor.fetchall()
                        logger.warning(f"[Watchdog Move DEBUG] Datei nicht gefunden mit: dir_id={src_dir_id}, filename='{src_filename_only}', ext_id={ext_id}")
                        logger.warning(f"[Watchdog Move DEBUG] Ähnliche Dateien in diesem Ordner: {similar[:5]}")
                        logger.warning(f"[Watchdog Move] Datei nicht in DB gefunden: {src_path}. Lege Ziel als neue Datei an.")
                        self._insert_or_update_file(dest_path)
                        return
                    file_id = file_row[0]
                    
                    # Erstelle/hole neues Zielverzeichnis
                    dest_dir_id = self.db.get_or_create_directory_optimized(self.drive_id, dest_dir.replace("\\", "/"))
                    
                    # --- SMART SPLIT für Ziel-Dateiname ---
                    # Gleiche Logik: os.path.splitext für den Dest-Filename,
                    # aber der UNIQUE Index ist auf (directory_id, filename),
                    # also muss filename konsistent sein.
                    dest_filename_only, dest_ext = os.path.splitext(dest_filename)
                    dest_ext = dest_ext if dest_ext else '[none]'
                    dest_ext_id = self.db.get_or_create_extension(dest_ext)
                    
                    # Loesche evtl. existierende Zieldatei (Rename-Pattern: temp -> final)
                    # WICHTIG: extension_id mit prüfen um keine falschen Dateien zu löschen!
                    self.db.cursor.execute(
                        "DELETE FROM files WHERE directory_id = ? AND filename = ? AND extension_id = ? AND id != ?",
                        (dest_dir_id, dest_filename_only, dest_ext_id, file_id)
                    )

                    # Update die Datei
                    self.db.cursor.execute(
                        "UPDATE files SET directory_id = ?, filename = ?, extension_id = ? WHERE id = ?",
                        (dest_dir_id, dest_filename_only, dest_ext_id, file_id)
                    )
                    
                    # Commit the transaction
                    self.db.conn.commit()
                    # REAL-TIME FIX: Force WAL Checkpoint für sofortige Sichtbarkeit
                    self.db.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                    
                    logger.info(f"[Watchdog Move] ✅ Datei verschoben/umbenannt: {src_path} -> {dest_path}")
                    logger.info(f"[Watchdog Move DEBUG] UPDATE: file_id={file_id}, new_dir_id={dest_dir_id}, new_filename='{dest_filename_only}', new_ext_id={dest_ext_id}")

            except sqlite3.Error as e:
                logger.error(f"[Watchdog Move DB-Fehler] Transaktion fehlgeschlagen für {src_path} -> {dest_path}: {e}. Rollback wird durchgeführt.")
                try:
                    self.db.conn.rollback()
                except Exception as rb_ex:
                    logger.error(f"[Watchdog Move DB-Fehler] Kritisch: Rollback fehlgeschlagen! {rb_ex}")
            except Exception as e:
                logger.error(f"[Watchdog Move-Fehler] {src_path} -> {dest_path}: {e}")

    def on_deleted(self, event):
        """Behandelt das Löschen von Dateien oder Verzeichnissen."""
        # *** NEU: Prüfung am Anfang ***
        if self._is_ignored(event.src_path):
            return
        if not should_fs_write():
            logger.debug(f"[ScanLock] on_deleted übersprungen (Scan läuft): {event.src_path}")
            return

        if not self._reinitialize_db_if_needed(): return

        # WICHTIG: Verwende Alias-bewusste Pfad-Normalisierung
        src_path = _normalize_path_for_watchdog(event.src_path)
        
        # *** NEU: Globalen Lock und manuelle Transaktion verwenden ***
        with _db_lock:
            try:
                deleted_rows = 0
                
                if event.is_directory:
                    # Lösche Verzeichnis-Eintrag (CASCADE löst auch Dateien)
                    deleted_rows = self.db.cursor.execute(
                        "DELETE FROM directories WHERE full_path = ? AND drive_id = ?", (src_path.replace("\\", "/"), self.drive_id)
                    ).rowcount
                    if deleted_rows > 0:
                        logger.info(f"[Watchdog Delete] Verzeichnis gelöscht: {src_path} (Kaskade löscht auch Dateien)")
                else:
                    # Lösche Datei-Eintrag - neue Struktur
                    filename = os.path.basename(src_path)
                    dir_path = os.path.dirname(src_path)
                    filename_only, ext = os.path.splitext(filename)
                    ext = ext if ext else '[none]'
                    
                    # SQL mit JOIN für optimierte Struktur
                    delete_sql = """
                        DELETE FROM files 
                        WHERE filename = ? 
                        AND extension_id = (SELECT id FROM extensions WHERE name = ?)
                        AND directory_id = (SELECT id FROM directories WHERE full_path = ? AND drive_id = ?)
                    """
                    deleted_rows = self.db.cursor.execute(
                        delete_sql, (filename_only, ext, dir_path.replace("\\", "/"), self.drive_id)
                    ).rowcount
                    if deleted_rows > 0:
                        logger.info(f"[Watchdog Delete] Datei gelöscht: {src_path}")

                # Nur committen, wenn etwas gelöscht wurde und kein Fehler auftrat
                if deleted_rows > 0:
                    self.db.conn.commit()
                    # REAL-TIME FIX: Force WAL Checkpoint für sofortige Sichtbarkeit
                    self.db.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")


            except sqlite3.Error as e:
                logger.error(f"[Watchdog Delete DB-Fehler] Transaktion fehlgeschlagen für {event.src_path}: {e}. Rollback wird durchgeführt.")
                try:
                    self.db.conn.rollback()
                except Exception as rb_ex:
                    logger.error(f"[Watchdog Delete DB-Fehler] Kritisch: Rollback fehlgeschlagen! {rb_ex}")
            except Exception as e:
                 logger.error(f"[Watchdog Delete-Fehler] {event.src_path}: {e}")


    def _handle_new_directory(self, dir_path):
        """Fügt ein neues Verzeichnis zur Datenbank hinzu."""
        # Keine Notwendigkeit für try/except hier, da in on_created bereits vorhanden
        dir_id = self.db.get_or_create_directory_optimized(self.drive_id, dir_path.replace("\\", "/"))
        if dir_id:
            logger.info(f"[Watchdog Create] Verzeichnis hinzugefügt: {dir_path}")
        else:
            logger.error(f"[Watchdog Create-Fehler] Konnte Verzeichnis nicht hinzufügen: {dir_path}")


    def _insert_or_update_file(self, filepath):
        """Fügt eine neue Datei hinzu oder aktualisiert eine vorhandene."""
        # *** WICHTIG: Auch hier theoretisch möglich, aber da on_created/on_modified schon filtern, nicht nötig ***
        # if os.path.normpath(filepath) == LOG_PATH:
        #     return

        # Keine Notwendigkeit für try/except hier, da in on_created/on_modified bereits vorhanden
        abs_path = os.path.normpath(filepath)
        dir_path = os.path.dirname(abs_path)
        filename = os.path.basename(abs_path)

        # Stelle sicher, dass das Verzeichnis existiert (kann bei schnellen Operationen fehlen)
        dir_id = self.db.get_or_create_directory_optimized(self.drive_id, dir_path.replace("\\", "/"))
        if not dir_id:
             logger.warning(f"[Watchdog Update-Fehler] Konnte Verzeichnis für Datei nicht finden/erstellen: {dir_path}")
             return

        try:
            # Prüfen ob Datei noch existiert und lesbar ist
            if not os.path.isfile(abs_path):
                 logger.warning(f"[Watchdog Update-Info] Datei nicht (mehr) vorhanden oder kein Zugriff: {abs_path}")
                 # Optional: Versuch, die Datei aus der DB zu löschen, falls sie existiert
                 filename_only, ext = os.path.splitext(filename)
                 ext = ext if ext else '[none]'
                 with _db_lock:
                     ext_id = self.db.get_or_create_extension(ext)
                     deleted_rows = self.db.cursor.execute(
                         "DELETE FROM files WHERE directory_id = ? AND filename = ? AND extension_id = ?",
                         (dir_id, filename_only, ext_id)
                     ).rowcount
                     if deleted_rows > 0:
                         self.db.conn.commit()
                         self.db.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                         logger.info(f"[Watchdog Update] Fehlenden Dateieintrag entfernt: {abs_path}")
                 return

            size = os.path.getsize(abs_path)
            hash_val = calculate_hash(abs_path) if HASHING else None
            # Prüfe, ob Hash-Berechnung erfolgreich war (wenn Hashing aktiviert ist)
            if HASHING and hash_val is None:
                logger.warning(f"[Watchdog Update-Warnung] Konnte Hash für Datei nicht berechnen: {abs_path}")
                # Entscheiden: Überspringen oder ohne Hash speichern? -> Aktuell: Ohne Hash speichern
                pass # Speichert None als Hash

            # Verwende optimierte Datei-Einfügung
            with _db_lock:
                file_id = self.db.insert_file_optimized(
                    dir_id, filename, size, hash_val,
                    created_date=None, modified_date=None
                )
                if file_id:
                    self.db.conn.commit()
                    self.db.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                    logger.info(f"[Watchdog Update] Datei hinzugefügt/geändert: {abs_path} (Size: {size}, Hash: {hash_val[:8] if hash_val else 'N/A'})")
                else:
                    logger.warning(f"[Watchdog Update] Datei-Einfügung fehlgeschlagen: {abs_path}")

        except PermissionError:
             logger.error(f"[Watchdog Update-Fehler] Keine Leseberechtigung für: {abs_path}")
        except FileNotFoundError:
             logger.error(f"[Watchdog Update-Fehler] Datei nicht gefunden (trotz vorheriger Prüfung): {abs_path}")
             # Versuch, die Datei aus der DB zu löschen
             filename_only, ext = os.path.splitext(filename)
             ext = ext if ext else '[none]'
             with _db_lock:
                 ext_id = self.db.get_or_create_extension(ext)
                 deleted_rows = self.db.cursor.execute(
                     "DELETE FROM files WHERE directory_id = ? AND filename = ? AND extension_id = ?",
                     (dir_id, filename_only, ext_id)
                 ).rowcount
                 if deleted_rows > 0:
                     self.db.conn.commit()
                     self.db.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                     logger.info(f"[Watchdog Update] Fehlenden Dateieintrag entfernt: {abs_path}")
        except Exception as e:
            logger.error(f"[Watchdog Update-Fehler] Unerwarteter Fehler bei {abs_path}: {e}")
