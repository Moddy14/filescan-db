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
import threading
# *** ENTFERNT: Debug-Import-Check ***
# try:
#     with open(DEBUG_FILE, "a") as f: f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - watchdog_monitor: Imported os, time, sys, sqlite3.\n")
# except: pass

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from drive_alias_detector import normalize_path_with_aliases, get_drive_mapping

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
    from models import get_db_instance, _db_lock, split_name_ext
    # *** ENTFERNT: Debug-Import-Check ***
    # with open(DEBUG_FILE, "a") as f: f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - watchdog_monitor: Imported from models.\n")
except Exception as models_ex:
    # *** ENTFERNT: Debug-Logging im Fehlerfall ***
    # with open(DEBUG_FILE, "a") as f: f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - watchdog_monitor: EXCEPTION importing from models: {models_ex}\n")
    raise # Fehler weiter werfen

try:
    from scan_lock import should_fs_write
except Exception:
    # Fallback: wenn scan_lock nicht verfügbar ist, immer schreiben erlauben
    def should_fs_write(timeout: float = 0.0) -> bool:
        return True


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
    os.path.normpath(os.path.join(os.environ.get("LOCALAPPDATA", ""), "Qsirch", "es_data")).lower() if os.environ.get("LOCALAPPDATA") else "",  # Qsirch Index-Churn
    os.path.normpath(os.path.join(os.environ.get("APPDATA", ""), "Microsoft\\Windows\\Recent")).lower() if os.environ.get("APPDATA") else "",  # Nur Recent Items
    os.path.normpath(os.environ.get("TEMP", "")).lower(),                    # Temp-Ordner
    os.path.normpath(os.environ.get("TMP", "")).lower(),                     # Alternativer Temp-Ordner
    os.path.normpath(os.path.join(os.environ.get("PROGRAMFILES", "C:\\Program Files"), "Norton", "Suite", "defs")).lower(),  # Norton Def-Updates
    "\\$recycle.bin", # Papierkorb (prüft, ob Pfad *enthält*, da Laufwerksbuchstabe variiert)
    os.path.normpath(os.path.join(PROJECT_DIR, "venv")).lower() # Virtuelle Umgebung im Projekt
]
# Entferne leere Einträge, falls Umgebungsvariablen nicht gesetzt sind
IGNORE_DIR_PREFIXES = [p for p in IGNORE_DIR_PREFIXES if p]

# Dateiendungen, die oft temporär oder System-bezogen sind
IGNORE_EXTENSIONS = [
    ".tmp", ".log", ".etl", ".pf", ".lnk", ".ini", ".bak", ".cache", ".part", ".crdownload", ".$$$",
    ".db-shm", ".db-wal", ".db-journal", ".pyc"  # Explizit SQLite-/Bytecode-Dateien ignorieren
]

# Spezifische Dateien zum Ignorieren (zusätzlich zu LOG_PATH)
IGNORE_FILES = [
    os.path.normpath(DB_PATH).lower(), # Die Datenbankdatei selbst
    os.path.normpath(os.path.join(PROJECT_DIR, "config.json")).lower(), # Die Konfigurationsdatei
    os.path.normpath(DB_PATH + "-shm").lower(),  # SQLite shared memory file
    os.path.normpath(DB_PATH + "-wal").lower(),  # SQLite write-ahead log
    os.path.normpath(DB_PATH + "-journal").lower()  # SQLite journal file
]

# Laufzeit-/Cache-Churn, der als "Datei-Lärm" für DateiDB keinen Mehrwert bringt.
# Wichtig: bewusst als substring-Muster, damit es unabhängig vom Service-User-Profil
# (LocalSystem vs. Moddy) trotzdem greift.
IGNORE_PATH_CONTAINS = [
    "\\appdata\\local\\temp\\codex-index-",
    "\\appdata\\local\\temp\\",
    "\\users\\moddy\\.codex\\runtimes\\",
    "\\users\\moddy\\.codex\\logs_2.sqlite",
    "\\users\\moddy\\.codex\\version.json",
    "\\appdata\\local\\microsoft\\windows\\powershell\\startupprofiledata-noninteractive",
    "\\appdata\\local\\packages\\5319275a.whatsappdesktop_",
    "\\appdata\\roaming\\telegram desktop\\tdata\\",
    "\\appdata\\roaming\\telegram desktop\\telegram.exe",
    "\\appdata\\local\\microsoft\\edge\\user data\\default\\platform notifications\\",
    "\\appdata\\local\\microsoft\\edge\\user data\\crashpad\\",
    "\\appdata\\local\\nvidia corporation\\nvidia overlay\\cefcache\\",
    "\\appdata\\local\\microsoft\\edge\\user data\\default\\indexeddb\\",
    "\\appdata\\local\\microsoft\\edge\\user data\\default\\local extension settings\\",
    "\\appdata\\local\\microsoft\\edge\\user data\\default\\code cache\\",
    "\\appdata\\local\\microsoft\\edge\\user data\\default\\entityextraction\\",
    "\\program files (x86)\\microsoft\\edge\\application\\",
    "\\appdata\\local\\microsoft\\identitycache\\",
    "\\appdata\\local\\microsoft\\onedrive\\logs\\",
    "\\appdata\\local\\packages\\microsoft.windows.contentdeliverymanager_",
    "\\program files\\common files\\acronis\\agent\\bin\\",
    # Zusätzlicher Runtime-Lärm (nach Live-Restart 2026-05-08)
    "\\appdata\\local\\packages\\openai.codex_",
    "\\appdata\\local\\packages\\openai.chatgpt-desktop_",
    "\\appdata\\roaming\\claude\\sentry\\",
    "\\appdata\\roaming\\claude\\network\\",
    "\\appdata\\roaming\\claude\\indexeddb\\",
    "\\appdata\\roaming\\claude\\webstorage\\",
    "\\appdata\\roaming\\claude\\config.json.tmp-",
    "\\appdata\\roaming\\termius\\webstorage\\",
    "\\appdata\\roaming\\microsoft\\windows\\powershell\\psreadline\\consolehost_history.txt",
    "\\appdata\\local\\microsoft\\edge\\user data\\default\\network\\",
    "\\appdata\\local\\microsoft\\edge\\user data\\default\\dnr extension rules\\",
    "\\appdata\\local\\microsoft\\edge\\user data\\safe browsing\\",
    "\\appdata\\local\\microsoft\\edge\\user data\\default\\webstorage\\",
    "\\appdata\\local\\microsoft\\edge\\user data\\default\\preferences",
    "\\appdata\\local\\microsoft\\edge\\user data\\local state",
    "\\appdata\\local\\docker\\wsl\\disk\\docker_data.vhdx",
    "\\users\\moddy\\.docker\\.tmp-",
    "\\appdata\\roaming\\docker\\.tmp-settings-store.json",
    "\\.docker\\buildx\\.tmp-current",
    "\\wsl\\ubuntu\\ext4.vhdx",
    "\\ac\\temp\\casesensitivetest",
    "\\appdata\\roaming\\code\\user\\globalstorage\\state.vscdb-journal",
    "\\appdata\\roaming\\code\\user\\globalstorage\\state.vscdb",
    "\\appdata\\roaming\\code\\user\\workspacestorage\\",
    "\\program files\\common files\\norton\\icarus\\",
    "\\program files\\syncovery\\",
    "\\program files\\windowsapps\\microsoft.languageexperiencepackde-de_",
    "\\system volume information\\masterfilestatus.db",
    "\\.git\\objects\\",
    "\\.git\\index.lock",
    "\\appdata\\roaming\\microsoft\\windows\\recent\\customdestinations\\",
    "\\dateidb\\scanner_portable\\watchdog_startup_report.txt",
    "\\dateidb\\scanner_portable\\.scheduled_last_runs.json",
    "\\projekte\\virtuellepythons\\clipboardmanager\\dist\\clipboard.db",
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
        self._last_wal_checkpoint_ts = 0.0
        self._wal_checkpoint_interval_s = float(CONFIG.get('watchdog_wal_checkpoint_interval_s', 60))

        # Event-Puffer gegen Event-Loss waehrend eines Bulk-Scans (Scan-Lock):
        # Events werden nicht verworfen, sondern gesammelt und beim naechsten
        # Schreibfenster nachgearbeitet.
        self._pending = []
        self._pending_lock = threading.Lock()
        self._pending_max = int(CONFIG.get('watchdog_pending_max', 20000))
        self._draining = False

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
        # Erste Prüfung: Sind DB und drive_id vorhanden?
        if self.db is None or self.drive_id is None:
            logger.info("[Watchdog] DB-Instanz oder drive_id fehlt. Versuche Wiederherstellung...")
            self._initialize_db()
            return self.db is not None and self.drive_id is not None
        
        # Zweite Prüfung: Ist die DB-Verbindung noch aktiv?
        try:
            self.db.cursor.execute("SELECT 1")
            result = self.db.cursor.fetchone()
            if result and result[0] == 1:
                return True
        except Exception as e:
            logger.warning(f"[Watchdog] DB-Verbindung unterbrochen: {e}. Versuche Neuinitialisierung...")
            
        # Verbindung ist unterbrochen - Neuinitialisierung
        max_retries = 3
        for attempt in range(max_retries):
            try:
                time.sleep(1)  # Kurze Pause
                if hasattr(self.db, 'conn'):
                    try:
                        self.db.conn.close()
                    except:
                        pass
                
                self.db = None
                self._initialize_db()
                
                if self.db is not None and self.drive_id is not None:
                    # Test der neuen Verbindung
                    self.db.cursor.execute("SELECT 1")
                    result = self.db.cursor.fetchone()
                    if result and result[0] == 1:
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

            # 3. Prüfe substring-Muster (profilunabhängige Lärm-Pfade)
            for needle in IGNORE_PATH_CONTAINS:
                if needle and needle in norm_path:
                    return True

            # 4. Prüfe Dateiendungen (nur wenn es keine Directory ist)
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

    def _wait_for_write_window(self, src_path: str) -> bool:
        """Stoppt Event-Verarbeitung kurz, wenn gerade ein Bulk-Scan läuft."""
        if should_fs_write(timeout=0.25):
            return True
        logger.debug(f"[Watchdog Skip] Scan-Lock aktiv, Event wird gepuffert: {src_path}")
        return False

    def _gate(self, kind, event):
        """Entscheidet, ob ein Event jetzt verarbeitet wird.

        Bei aktivem Scan-Lock wird das Event GEPUFFERT (nicht verworfen) und
        False zurückgegeben. Andernfalls werden zuvor aufgelaufene Events
        nachgearbeitet und True zurückgegeben.
        """
        if not self._wait_for_write_window(event.src_path):
            self._buffer_pending(kind, event)
            return False
        self._drain_pending()
        return True

    def _buffer_pending(self, kind, event):
        """Puffert ein Event, das während eines Scan-Locks aufgetreten ist."""
        with self._pending_lock:
            if len(self._pending) >= self._pending_max:
                logger.warning(
                    f"[Watchdog Pending] Puffer voll ({self._pending_max}); Event "
                    f"verworfen: {getattr(event, 'src_path', '?')}. Nächster "
                    "Scan/Integrity-Check stellt Konsistenz wieder her."
                )
                return
            self._pending.append((kind, event))

    def _drain_pending(self):
        """Arbeitet zuvor gepufferte Events ab, sobald wieder geschrieben werden darf."""
        if self._draining:
            return
        with self._pending_lock:
            if not self._pending:
                return
            batch = self._pending
            self._pending = []
        self._draining = True
        try:
            handlers = {
                "created": self.on_created,
                "modified": self.on_modified,
                "moved": self.on_moved,
                "deleted": self.on_deleted,
            }
            logger.info(f"[Watchdog Pending] Arbeite {len(batch)} aufgelaufene Events nach.")
            for kind, event in batch:
                handler = handlers.get(kind)
                if handler is None:
                    continue
                try:
                    handler(event)
                except Exception as e:
                    logger.error(
                        f"[Watchdog Pending] Fehler beim Nacharbeiten von {kind} "
                        f"{getattr(event, 'src_path', '?')}: {e}"
                    )
        finally:
            self._draining = False

    @staticmethod
    def _is_db_lock_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        return (
            "database table is locked" in msg
            or "database is locked" in msg
            or "database schema is locked" in msg
            or "database busy" in msg
            or "cannot operate on a closed database" in msg
            or "recursive use of cursors not allowed" in msg
        )

    def _checkpoint_if_due(self, reason: str):
        """Drosselt WAL-Checkpointing, um Lock-Stürme durch Event-Bursts zu vermeiden."""
        now = time.time()
        if now - self._last_wal_checkpoint_ts < self._wal_checkpoint_interval_s:
            return
        try:
            self.db.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            self._last_wal_checkpoint_ts = now
            logger.debug(f"[Watchdog Checkpoint] PASSIVE checkpoint nach {reason}")
        except Exception as e:
            logger.warning(f"[Watchdog Checkpoint] Fehler bei PASSIVE checkpoint nach {reason}: {e}")

    def on_created(self, event):
        """Behandelt das Erstellen von Dateien oder Verzeichnissen."""
        if self._is_ignored(event.src_path):
            return
        if not self._gate("created", event):
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
        if self._is_ignored(event.src_path):
            return
        if not self._gate("modified", event):
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
        if self._is_ignored(event.src_path) or self._is_ignored(event.dest_path):
            return
        if not self._gate("moved", event):
            return

        if not self._reinitialize_db_if_needed(): return

        # WICHTIG: Verwende Alias-bewusste Pfad-Normalisierung
        src_path = _normalize_path_for_watchdog(event.src_path)
        dest_path = _normalize_path_for_watchdog(event.dest_path)

        max_retries = 5
        for attempt in range(1, max_retries + 1):
            with _db_lock:
                try:
                    if event.is_directory:
                        moved = self._handle_directory_move(src_path, dest_path)
                        logger.info(f"[Watchdog Move] Verzeichnis verschoben/umbenannt: {src_path} -> {dest_path} ({moved} Eintraege aktualisiert)")
                        return

                    # Datei umbenannt/verschoben - für neue DB-Struktur
                    src_filename = os.path.basename(src_path)
                    dest_filename = os.path.basename(dest_path)
                    src_dir = os.path.dirname(src_path)
                    dest_dir = os.path.dirname(dest_path)

                    # Finde die Datei in der optimierten Struktur
                    # (split_name_ext: identische Endungs-Logik wie der Scanner,
                    #  sonst verfehlt der Lookup gehaertete filename/extension)
                    src_filename_only, src_ext = split_name_ext(src_filename)

                    # Finde die alte directory_id
                    self.db.cursor.execute("SELECT id FROM directories WHERE drive_id = ? AND full_path = ?", (self.drive_id, src_dir.replace("\\", "/")))
                    src_dir_row = self.db.cursor.fetchone()
                    if not src_dir_row:
                        logger.warning(f"[Watchdog Move] Quell-Verzeichnis nicht gefunden: {src_dir} -> fallback insert für Ziel")
                        # Fallback: Zielzustand trotzdem indexieren (wichtig bei unvollständiger DB)
                        self._insert_or_update_file(dest_path)
                        return
                    src_dir_id = src_dir_row[0]

                    # Finde Extension-ID
                    ext_id = self.db.get_or_create_extension(src_ext)

                    # Finde die Datei
                    self.db.cursor.execute(
                        "SELECT id FROM files WHERE directory_id = ? AND filename = ? AND extension_id = ?",
                        (src_dir_id, src_filename_only, ext_id)
                    )
                    file_row = self.db.cursor.fetchone()
                    if not file_row:
                        logger.warning(f"[Watchdog Move] Datei nicht in DB gefunden: {src_path} -> fallback insert für Ziel")
                        # Fallback: Rename/Move soll trotzdem in DB landen
                        self._insert_or_update_file(dest_path)
                        return
                    file_id = file_row[0]

                    # Erstelle/hole neues Zielverzeichnis
                    dest_dir_id = self.db.get_or_create_directory_optimized(self.drive_id, dest_dir.replace("\\", "/"))

                    # Parse neuen Dateinamen
                    dest_filename_only, dest_ext = split_name_ext(dest_filename)
                    dest_ext_id = self.db.get_or_create_extension(dest_ext)

                    # Ziel kann bei temp->final Moves bereits als separates Event existieren.
                    # Dann Quellzeile entfernen statt UNIQUE-Fehler zu erzeugen.
                    self.db.cursor.execute(
                        "SELECT id FROM files WHERE directory_id = ? AND filename = ? AND extension_id = ?",
                        (dest_dir_id, dest_filename_only, dest_ext_id)
                    )
                    dest_row = self.db.cursor.fetchone()
                    if dest_row and dest_row[0] != file_id:
                        self.db.cursor.execute("DELETE FROM files WHERE id = ?", (file_id,))
                        self.db.conn.commit()
                        self._checkpoint_if_due("MoveDedupe")
                        logger.info(f"[Watchdog Move] Ziel existierte bereits, Quell-Eintrag entfernt: {src_path} -> {dest_path}")
                        return

                    # Update die Datei
                    self.db.cursor.execute(
                        "UPDATE files SET directory_id = ?, filename = ?, extension_id = ? WHERE id = ?",
                        (dest_dir_id, dest_filename_only, dest_ext_id, file_id)
                    )

                    self.db.conn.commit()
                    self._checkpoint_if_due("Move")
                    logger.info(f"[Watchdog Move] Datei verschoben/umbenannt: {src_path} -> {dest_path}")
                    return

                except sqlite3.Error as e:
                    try:
                        self.db.conn.rollback()
                    except Exception as rb_ex:
                        logger.error(f"[Watchdog Move DB-Fehler] Kritisch: Rollback fehlgeschlagen! {rb_ex}")

                    if self._is_db_lock_error(e) and attempt < max_retries:
                        wait_s = min(0.2 * (2 ** (attempt - 1)), 2.0)
                        logger.warning(f"[Watchdog Move Lock] DB gesperrt bei {src_path}. Retry {attempt}/{max_retries} in {wait_s:.2f}s")
                        time.sleep(wait_s)
                        continue

                    logger.error(f"[Watchdog Move DB-Fehler] Transaktion fehlgeschlagen für {src_path} -> {dest_path}: {e}.")
                    return
                except Exception as e:
                    logger.error(f"[Watchdog Move-Fehler] {src_path} -> {dest_path}: {e}")
                    return

    def on_deleted(self, event):
        """Behandelt das Löschen von Dateien oder Verzeichnissen."""
        if self._is_ignored(event.src_path):
            return
        if not self._gate("deleted", event):
            return

        if not self._reinitialize_db_if_needed(): return

        # WICHTIG: Verwende Alias-bewusste Pfad-Normalisierung
        src_path = _normalize_path_for_watchdog(event.src_path)

        max_retries = 5
        for attempt in range(1, max_retries + 1):
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
                        filename_only, ext = split_name_ext(filename)

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
                        self._checkpoint_if_due("Delete")
                    return

                except sqlite3.Error as e:
                    try:
                        self.db.conn.rollback()
                    except Exception as rb_ex:
                        logger.error(f"[Watchdog Delete DB-Fehler] Kritisch: Rollback fehlgeschlagen! {rb_ex}")

                    if self._is_db_lock_error(e) and attempt < max_retries:
                        wait_s = min(0.2 * (2 ** (attempt - 1)), 2.0)
                        logger.warning(f"[Watchdog Delete Lock] DB gesperrt bei {event.src_path}. Retry {attempt}/{max_retries} in {wait_s:.2f}s")
                        time.sleep(wait_s)
                        continue

                    logger.error(f"[Watchdog Delete DB-Fehler] Transaktion fehlgeschlagen für {event.src_path}: {e}.")
                    return
                except Exception as e:
                    logger.error(f"[Watchdog Delete-Fehler] {event.src_path}: {e}")
                    return


    def _handle_new_directory(self, dir_path):
        """Fügt ein neues Verzeichnis zur Datenbank hinzu."""
        # Keine Notwendigkeit für try/except hier, da in on_created bereits vorhanden
        dir_id = self.db.get_or_create_directory_optimized(self.drive_id, dir_path.replace("\\", "/"))
        if dir_id:
            logger.info(f"[Watchdog Create] Verzeichnis hinzugefügt: {dir_path}")
        else:
            logger.error(f"[Watchdog Create-Fehler] Konnte Verzeichnis nicht hinzufügen: {dir_path}")

    def _handle_directory_move(self, src_dir, dest_dir):
        """Aktualisiert ein verschobenes/umbenanntes Verzeichnis und ALLE
        Nachkommen in der DB (full_path, directory_name, parent_id, depth_level).

        Die zugehörigen Dateien bleiben automatisch korrekt verknüpft, da sich
        nur die full_path-Werte der Verzeichnisse ändern, nicht deren IDs.

        Muss unter gehaltenem _db_lock aufgerufen werden (siehe on_moved).
        Gibt die Anzahl der aktualisierten Verzeichnis-Einträge zurück.
        """
        src_fp = os.path.normpath(src_dir).replace("\\", "/")
        dest_fp = os.path.normpath(dest_dir).replace("\\", "/")
        if src_fp == dest_fp:
            return 0

        # Verschobenes Verzeichnis selbst + alle Nachkommen einsammeln
        self.db.cursor.execute(
            "SELECT id, full_path FROM directories "
            "WHERE drive_id = ? AND (full_path = ? OR full_path LIKE ?)",
            (self.drive_id, src_fp, src_fp + "/%"),
        )
        affected = self.db.cursor.fetchall()
        if not affected:
            return 0

        drive_root = (self.drive_name or "").rstrip("/")

        def _depth(fp):
            if drive_root and fp.startswith(drive_root):
                rel = fp[len(drive_root):].strip("/")
            else:
                rel = fp.strip("/")
            return len([p for p in rel.split("/") if p]) if rel else 0

        # Neuer Parent für das verschobene Top-Verzeichnis
        dest_parent = os.path.dirname(dest_fp)
        if dest_parent and dest_parent != drive_root and dest_parent + "/" != self.drive_name:
            new_parent_id = self.db.get_or_create_directory_optimized(self.drive_id, dest_parent)
        else:
            new_parent_id = None

        for dir_id, fp in affected:
            new_fp = dest_fp + fp[len(src_fp):]
            new_name = os.path.basename(new_fp)
            new_depth = _depth(new_fp)
            if fp == src_fp:
                self.db.cursor.execute(
                    "UPDATE directories SET full_path = ?, directory_name = ?, "
                    "parent_id = ?, depth_level = ? WHERE id = ?",
                    (new_fp, new_name, new_parent_id, new_depth, dir_id),
                )
            else:
                self.db.cursor.execute(
                    "UPDATE directories SET full_path = ?, depth_level = ? WHERE id = ?",
                    (new_fp, new_depth, dir_id),
                )

        self.db.conn.commit()
        self._checkpoint_if_due("DirMove")
        return len(affected)


    def _insert_or_update_file(self, filepath):
        """Fügt eine neue Datei hinzu oder aktualisiert eine vorhandene."""
        abs_path = os.path.normpath(filepath)
        dir_path = os.path.dirname(abs_path)
        filename = os.path.basename(abs_path)

        max_retries = 5
        for attempt in range(1, max_retries + 1):
            try:
                with _db_lock:
                    # Stelle sicher, dass das Verzeichnis existiert (kann bei schnellen Operationen fehlen)
                    dir_id = self.db.get_or_create_directory_optimized(self.drive_id, dir_path.replace("\\", "/"))
                    if not dir_id:
                        logger.warning(f"[Watchdog Update-Fehler] Konnte Verzeichnis für Datei nicht finden/erstellen: {dir_path}")
                        return

                    # Prüfen ob Datei noch existiert und lesbar ist
                    if not os.path.isfile(abs_path):
                        logger.warning(f"[Watchdog Update-Info] Datei nicht (mehr) vorhanden oder kein Zugriff: {abs_path}")
                        filename_only, ext = split_name_ext(filename)
                        ext_id = self.db.get_or_create_extension(ext)
                        deleted_rows = self.db.cursor.execute(
                            "DELETE FROM files WHERE directory_id = ? AND filename = ? AND extension_id = ?",
                            (dir_id, filename_only, ext_id)
                        ).rowcount
                        if deleted_rows > 0:
                            self.db.conn.commit()
                            self._checkpoint_if_due("UpdateMissingFile")
                            logger.info(f"[Watchdog Update] Fehlenden Dateieintrag entfernt: {abs_path}")
                        return

                    size = os.path.getsize(abs_path)
                    hash_val = calculate_hash(abs_path) if HASHING else None
                    if HASHING and hash_val is None:
                        logger.warning(f"[Watchdog Update-Warnung] Konnte Hash für Datei nicht berechnen: {abs_path}")

                    # Verwende optimierte Datei-Einfügung
                    file_id = self.db.insert_file_optimized(
                        dir_id, filename, size, hash_val,
                        created_date=None, modified_date=None
                    )

                    # insert_file_optimized kann bei UPDATE/IGNORE einen falsy Rückgabewert liefern,
                    # obwohl der Datensatz korrekt vorhanden/aktualisiert ist.
                    # Daher Existenz im Zielslot prüfen, bevor wir einen Fehler loggen.
                    op_ok = bool(file_id)
                    if not op_ok:
                        filename_only, ext = split_name_ext(filename)
                        ext_id = self.db.get_or_create_extension(ext)
                        self.db.cursor.execute(
                            "SELECT id FROM files WHERE directory_id = ? AND filename = ? AND extension_id = ?",
                            (dir_id, filename_only, ext_id)
                        )
                        op_ok = self.db.cursor.fetchone() is not None

                    if op_ok:
                        self.db.conn.commit()
                        self._checkpoint_if_due("UpdateInsert")
                        logger.info(f"[Watchdog Update] Datei hinzugefügt/geändert: {abs_path} (Size: {size}, Hash: {hash_val[:8] if hash_val else 'N/A'})")
                    else:
                        logger.warning(f"[Watchdog Update] Datei-Einfügung fehlgeschlagen: {abs_path}")
                    return

            except PermissionError:
                logger.error(f"[Watchdog Update-Fehler] Keine Leseberechtigung für: {abs_path}")
                return
            except FileNotFoundError:
                # Race Condition: Datei zwischen Event und Zugriff verschwunden
                with _db_lock:
                    try:
                        filename_only, ext = split_name_ext(filename)
                        dir_id = self.db.get_or_create_directory_optimized(self.drive_id, dir_path.replace("\\", "/"))
                        if dir_id:
                            ext_id = self.db.get_or_create_extension(ext)
                            deleted_rows = self.db.cursor.execute(
                                "DELETE FROM files WHERE directory_id = ? AND filename = ? AND extension_id = ?",
                                (dir_id, filename_only, ext_id)
                            ).rowcount
                            if deleted_rows > 0:
                                self.db.conn.commit()
                                self._checkpoint_if_due("UpdateRaceDelete")
                                logger.info(f"[Watchdog Update] Fehlenden Dateieintrag entfernt: {abs_path}")
                    except Exception:
                        pass
                return
            except sqlite3.Error as e:
                try:
                    with _db_lock:
                        self.db.conn.rollback()
                except Exception:
                    pass

                if self._is_db_lock_error(e) and attempt < max_retries:
                    wait_s = min(0.2 * (2 ** (attempt - 1)), 2.0)
                    logger.warning(f"[Watchdog Update Lock] DB gesperrt bei {abs_path}. Retry {attempt}/{max_retries} in {wait_s:.2f}s")
                    time.sleep(wait_s)
                    continue

                logger.error(f"[Watchdog Update-Fehler] SQL-Fehler bei {abs_path}: {e}")
                return
            except Exception as e:
                logger.error(f"[Watchdog Update-Fehler] Unerwarteter Fehler bei {abs_path}: {e}")
                return
