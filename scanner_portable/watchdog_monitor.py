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


# --- NEU: Listen für zu ignorierende Pfade und Dateien ---
# Verzeichnisse, die häufig Hintergrundaktivitäten aufweisen
# Wichtig: Pfade müssen normalisiert sein (os.path.normpath, os.path.abspath)
# Verwende Kleinbuchstaben für den Vergleich
IGNORE_DIR_PREFIXES = [
    os.path.normpath(os.environ.get("WINDIR", "C:\\Windows")).lower(),          # C:\Windows
    os.path.normpath(os.environ.get("PROGRAMDATA", "C:\\ProgramData")).lower(), # C:\ProgramData
    os.path.normpath(os.environ.get("APPDATA", "")).lower(),                   # C:\Users\<user>\AppData\Roaming
    os.path.normpath(os.environ.get("LOCALAPPDATA", "")).lower(),             # C:\Users\<user>\AppData\Local
    os.path.normpath(os.environ.get("TEMP", "")).lower(),                    # Temp-Ordner
    os.path.normpath(os.environ.get("TMP", "")).lower(),                     # Alternativer Temp-Ordner
    "\\$recycle.bin", # Papierkorb (prüft, ob Pfad *enthält*, da Laufwerksbuchstabe variiert)
    os.path.normpath(os.path.join(PROJECT_DIR, "venv")).lower() # Virtuelle Umgebung im Projekt
]
# Entferne leere Einträge, falls Umgebungsvariablen nicht gesetzt sind
IGNORE_DIR_PREFIXES = [p for p in IGNORE_DIR_PREFIXES if p]

# Dateiendungen, die oft temporär oder System-bezogen sind
IGNORE_EXTENSIONS = [
    ".tmp", ".log", ".etl", ".pf", ".lnk", ".ini", ".bak", ".cache", ".part", ".crdownload"
]

# Spezifische Dateien zum Ignorieren (zusätzlich zu LOG_PATH)
IGNORE_FILES = [
    os.path.normpath(DB_PATH).lower(), # Die Datenbankdatei selbst
    os.path.normpath(os.path.join(PROJECT_DIR, "config.json")).lower() # Die Konfigurationsdatei
]
# --- Ende Ignorier-Listen ---

class FSHandler(FileSystemEventHandler):
    """Behandelt Dateisystemereignisse und aktualisiert die Datenbank."""
    def __init__(self, path_to_watch):
        super().__init__()
        self.path_to_watch = os.path.normpath(os.path.abspath(path_to_watch))
        # Bestimme das Laufwerk aus dem überwachten Pfad
        drive_letter, _ = os.path.splitdrive(self.path_to_watch)
        # Standardisiere den Laufwerksnamen (z.B. "C:/")
        self.drive_name = drive_letter.upper() + "/" if drive_letter else "UNKNOWN/"
        self.db = None # Wird bei Bedarf initialisiert
        self.drive_id = None # Wird bei Bedarf initialisiert
        self._initialize_db()

    def _initialize_db(self):
        """Initialisiert die DB-Verbindung und holt die drive_id."""
        try:
            self.db = get_db_instance() # Verwendet Standardpfad aus utils
            if self.db:
                self.drive_id = self.db.get_or_create_drive(self.drive_name)
                if self.drive_id is None:
                    logger.error(f"[Watchdog-Fehler] Konnte drive_id für {self.drive_name} nicht ermitteln.")
            else:
                 logger.error("[Watchdog-Fehler] Konnte keine DB-Instanz erhalten.")
        except Exception as e:
            logger.error(f"[Watchdog-Fehler] Kritischer Fehler bei DB-Initialisierung: {e}")
            # In diesem Fall kann der Handler nicht richtig arbeiten
            self.db = None
            self.drive_id = None

    def _reinitialize_db_if_needed(self):
        """Prüft, ob eine DB-Verbindung besteht und versucht ggf. neu zu initialisieren."""
        if self.db is None or self.drive_id is None:
            logger.info("[Watchdog] Versuche DB-Verbindung wiederherzustellen...")
            self._initialize_db()
        return self.db is not None and self.drive_id is not None

    # --- NEU: Hilfsfunktion zum Prüfen, ob ein Pfad ignoriert werden soll ---
    def _is_ignored(self, path):
        """Prüft, ob ein gegebener Pfad ignoriert werden soll."""
        try:
            norm_path = os.path.normpath(path).lower()

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
            if ext and ext in IGNORE_EXTENSIONS:
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

        if not self._reinitialize_db_if_needed(): return
        try:
            src_path = os.path.normpath(event.src_path)
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

        if not self._reinitialize_db_if_needed(): return
        try:
            src_path = os.path.normpath(event.src_path)
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

        if not self._reinitialize_db_if_needed(): return

        src_path = os.path.normpath(event.src_path)
        dest_path = os.path.normpath(event.dest_path)
        with _db_lock:
            try:
                if event.is_directory:
                    logger.info(f"[Watchdog Move] Verzeichnis verschoben/umbenannt: {src_path} -> {dest_path}. Manuelle Prüfung/Rescan empfohlen.")
                else:
                    # Datei umbenannt/verschoben
                    
                    # 1. Update the file path (identifying the original file)
                    update_path_sql = "UPDATE files SET file_path = ? WHERE file_path = ? AND directory_id IN (SELECT id FROM directories WHERE drive_id = ?)"
                    self.db.cursor.execute(update_path_sql, (dest_path, src_path, self.drive_id))
                    
                    # 2. Get/Create the new directory ID
                    new_dir_path = os.path.dirname(dest_path)
                    new_dir_id = self.db.get_or_create_directory(self.drive_id, new_dir_path)

                    # 3. Update the directory ID for the moved file (now identified by its new path)
                    if new_dir_id:
                        # *** KORRIGIERT: WHERE-Klausel verwendet jetzt dest_path, keine drive_id mehr ***
                        update_dir_sql = "UPDATE files SET directory_id = ? WHERE file_path = ?"
                        self.db.cursor.execute(update_dir_sql, (new_dir_id, dest_path))
                    else:
                         # Log a warning if the new directory couldn't be determined
                         logger.warning(f"[Watchdog Move] Konnte neue directory_id für Zieldatei {dest_path} nicht ermitteln. directory_id wurde nicht aktualisiert.")

                    # Commit the transaction covering both updates
                    self.db.conn.commit()
                    logger.info(f"[Watchdog Move] Datei verschoben/umbenannt: {src_path} -> {dest_path}")

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

        if not self._reinitialize_db_if_needed(): return

        # *** NEU: Globalen Lock und manuelle Transaktion verwenden ***
        with _db_lock:
            try:
                deleted_rows = 0
                if event.is_directory:
                    # Lösche Verzeichnis-Eintrag (Trigger loggt in deleted_directories)
                    deleted_rows = self.db.cursor.execute(
                        "DELETE FROM directories WHERE path = ? AND drive_id = ?", (event.src_path, self.drive_id)
                    ).rowcount
                    if deleted_rows > 0:
                        logger.info(f"[Watchdog Delete] Verzeichnis gelöscht: {event.src_path} (und {deleted_rows-1} Einträge via Cascade vermutet)")
                else:
                    # Lösche Datei-Eintrag (Trigger loggt in deleted_files)
                    deleted_rows = self.db.cursor.execute(
                        "DELETE FROM files WHERE file_path = ? AND directory_id IN (SELECT id FROM directories WHERE drive_id = ?)",
                        (event.src_path, self.drive_id)
                    ).rowcount
                    if deleted_rows > 0:
                        logger.info(f"[Watchdog Delete] Datei gelöscht: {event.src_path}")

                # Nur committen, wenn etwas gelöscht wurde und kein Fehler auftrat
                if deleted_rows > 0:
                    self.db.conn.commit()

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
        dir_id = self.db.get_or_create_directory(self.drive_id, dir_path)
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

        # Stelle sicher, dass das Verzeichnis existiert (kann bei schnellen Operationen fehlen)
        dir_id = self.db.get_or_create_directory(self.drive_id, dir_path)
        if not dir_id:
             logger.warning(f"[Watchdog Update-Fehler] Konnte Verzeichnis für Datei nicht finden/erstellen: {dir_path}")
             return

        try:
            # Prüfen ob Datei noch existiert und lesbar ist
            if not os.path.isfile(abs_path):
                 logger.warning(f"[Watchdog Update-Info] Datei nicht (mehr) vorhanden oder kein Zugriff: {abs_path}")
                 # Optional: Versuch, die Datei aus der DB zu löschen, falls sie existiert
                 deleted_rows = self.db.cursor.execute(
                     "DELETE FROM files WHERE file_path = ? AND directory_id = ?", (abs_path, dir_id)
                 ).rowcount
                 if deleted_rows > 0:
                     self.db.conn.commit()
                     logger.info(f"[Watchdog Update] Fehlenden Dateieintrag entfernt: {abs_path}")
                 return

            size = os.path.getsize(abs_path)
            hash_val = calculate_hash(abs_path) if HASHING else None
            # Prüfe, ob Hash-Berechnung erfolgreich war (wenn Hashing aktiviert ist)
            if HASHING and hash_val is None:
                logger.warning(f"[Watchdog Update-Warnung] Konnte Hash für Datei nicht berechnen: {abs_path}")
                # Entscheiden: Überspringen oder ohne Hash speichern? -> Aktuell: Ohne Hash speichern
                pass # Speichert None als Hash

            # Verwende Batch-Insert (auch wenn nur eine Datei) für Konsistenz
            self.db.batch_insert_files([(dir_id, abs_path, size, hash_val)])
            # Loggen nur, wenn Operation erfolgreich war (batch_insert_files gibt nichts zurück)
            # Könnte man verbessern, indem batch_insert_files Erfolg/Misserfolg zurückgibt
            logger.info(f"[Watchdog Update] Datei hinzugefügt/geändert: {abs_path} (Size: {size}, Hash: {hash_val[:8] if hash_val else 'N/A'})")

        except PermissionError:
             logger.error(f"[Watchdog Update-Fehler] Keine Leseberechtigung für: {abs_path}")
        except FileNotFoundError:
             logger.error(f"[Watchdog Update-Fehler] Datei nicht gefunden (trotz vorheriger Prüfung): {abs_path}")
             # Versuch, die Datei aus der DB zu löschen
             deleted_rows = self.db.cursor.execute(
                 "DELETE FROM files WHERE file_path = ? AND directory_id = ?", (abs_path, dir_id)
             ).rowcount
             if deleted_rows > 0:
                 self.db.conn.commit()
                 logger.info(f"[Watchdog Update] Fehlenden Dateieintrag entfernt: {abs_path}")
        except Exception as e:
            logger.error(f"[Watchdog Update-Fehler] Unerwarteter Fehler bei {abs_path}: {e}")
