# -*- coding: utf-8 -*-
import os
import re
import sqlite3
import threading
from datetime import datetime
import logging
from collections import defaultdict

# Importiere den globalen Logger aus utils
from utils import logger, DB_PATH, CONFIG

_db_lock = threading.RLock()
_db_instance = None
_db_path = None

# --- Endungs-Plausibilitaet (Root-Cause-Fix gegen extensions-Bloat) ---------
# os.path.splitext nimmt blind alles nach dem letzten Punkt als "Endung". Bei
# Dateinamen mit mehreren Punkten (Versionen, Datumsangaben, Hashes, Assembly-
# Strings) entstand so Muell wie '.0', '.0-STABLE', '.luzia-UBUNTU_2,ST' oder
# 67-Zeichen-SHA-Hashes – pro eindeutigem String eine permanente extensions-Zeile.
# R3-Regel: Punkt + alnum-Startzeichen + bis zu 15 weitere [alnum/_/-], und es
# muss mindestens ein Buchstabe vorkommen (verwirft rein-numerische wie '.0').
NONE_EXT = '[none]'
_PLAUSIBLE_EXT_RE = re.compile(r"^\.[A-Za-z0-9][A-Za-z0-9_-]{0,15}$")

# scan_lock ist ein Mutex + kurze Verlaufsanzeige, KEIN Audit-Trail. acquire
# beschneidet die inaktive Historie automatisch auf die letzten N Eintraege,
# damit nie ein externer Cleanup noetig ist (Root-Cause-Fix gegen scan_lock-Bloat).
SCAN_LOCK_HISTORY_KEEP = 20


def _is_plausible_ext(ext):
    """True, wenn ``ext`` wie eine echte Datei-Endung aussieht (R3-Regel).

    Case-insensitiv (das Muster akzeptiert beide Schreibweisen). Rein-numerische
    "Endungen" gelten immer als Muell.
    """
    if not ext or ext == NONE_EXT:
        return False
    if not _PLAUSIBLE_EXT_RE.match(ext):
        return False
    return any(ch.isalpha() for ch in ext)


def split_name_ext(basename):
    """Trennt einen Basename in (filename, ext) mit Plausibilitaets-Pruefung.

    Nur eine plausible Endung (siehe :func:`_is_plausible_ext`) wird abgetrennt;
    sonst bleibt der VOLLE Name im filename und ext == '[none]'. Gespeichert wird
    die Originalschreibweise. Es gilt stets die Konkatenations-Invariante::

        filename + (ext, falls echt) == basename

    Diese Invariante ist Voraussetzung fuer die Pfad-Rekonstruktion in
    integrity_checker.py / exporter.py (full_path/filename/e.name).
    """
    stem, ext = os.path.splitext(basename)
    if _is_plausible_ext(ext):
        return stem, ext
    return basename, NONE_EXT

class FileCache:
    """Leichtgewichtiger In-Memory Cache für existierende Dateien"""
    
    def __init__(self):
        self.cache = {}  # (directory_id, filename, extension_id) -> True
        self.lock = threading.RLock()
        self.enabled = True
        self.max_entries = 100000  # Max 100k Einträge im Cache

    def check(self, directory_id, filename, extension_id=None):
        """Prüft ob Datei im Cache ist (True = existiert, None = unbekannt).

        Der Schluessel enthaelt extension_id, damit gleichnamige Dateien mit
        verschiedener Endung (logo.png / logo.svg) NICHT als dieselbe Datei gelten.
        """
        if not self.enabled:
            return None
        with self.lock:
            key = (directory_id, filename, extension_id)
            if key in self.cache:
                return True
            return None  # Unbekannt, nicht False

    def add(self, directory_id, filename, extension_id=None):
        """Fügt Datei zum Cache hinzu"""
        if not self.enabled:
            return
        with self.lock:
            # Einfache LRU: Lösche älteste wenn zu voll
            if len(self.cache) >= self.max_entries:
                # Lösche erste 10% der Einträge
                to_remove = list(self.cache.keys())[:self.max_entries // 10]
                for key in to_remove:
                    del self.cache[key]
            self.cache[(directory_id, filename, extension_id)] = True

    def remove(self, directory_id, filename, extension_id=None):
        """Entfernt Datei aus Cache"""
        with self.lock:
            self.cache.pop((directory_id, filename, extension_id), None)
    
    def clear(self):
        """Leert den Cache"""
        with self.lock:
            self.cache.clear()

class DBManager:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path, check_same_thread=False, timeout=120.0)
        try:
            self.conn.execute("PRAGMA journal_mode=WAL;")
            logger.info("[DB] WAL Journal-Modus erfolgreich aktiviert.")
        except sqlite3.Error as e:
            logger.warning(f"[DB Warnung] Konnte WAL Journal-Modus nicht aktivieren: {e}. Verwende Standard-Journal.")
        self.conn.execute("PRAGMA busy_timeout = 60000;")
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.cursor = self.conn.cursor()
        
        # Verifiziere, dass Foreign Keys wirklich aktiviert sind
        self.cursor.execute("PRAGMA foreign_keys")
        fk_status = self.cursor.fetchone()[0]
        if fk_status == 1:
            logger.info("[DB] Foreign Keys erfolgreich aktiviert")
        else:
            logger.error("[DB] WARNUNG: Foreign Keys konnten NICHT aktiviert werden! CASCADE DELETE funktioniert nicht!")
            # Versuche es nochmal
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.cursor.execute("PRAGMA foreign_keys")
            fk_status = self.cursor.fetchone()[0]
            if fk_status == 1:
                logger.info("[DB] Foreign Keys im zweiten Versuch aktiviert")
            else:
                logger.error("[DB] KRITISCH: Foreign Keys bleiben deaktiviert!")
        
        self.path = db_path

        # In-Memory File Cache für Performance
        self.file_cache = FileCache()

        self.ensure_schema()

    def with_lock(func):
        def wrapper(self, *args, **kwargs):
            with _db_lock:
                return func(self, *args, **kwargs)
        return wrapper

    @with_lock
    def ensure_schema(self):
        """Erstellt das optimierte Datenbankschema mit normalisierten Tabellen."""
        
        # MIGRATION: Deaktiviert - Tabellen werden NICHT mehr gelöscht
        # Dies war der kritische Bug, der alle Daten löschte!
        # try:
        #     old_tables = ['files', 'directories', 'deleted_files', 'deleted_directories']
        #     for table in old_tables:
        #         self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
        #     logger.info("[DB] Alte Tabellen für Migration gelöscht")
        # except Exception as e:
        #     logger.warning(f"[DB] Warnung beim Löschen alter Tabellen: {e}")
        
        # 1. Tabelle für Laufwerke (unverändert)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS drives (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL  -- C:/, D:/, etc.
            )
        """)
        
        # 2. Tabelle für Extensions (NEU)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS extensions (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,  -- .txt, .pdf, .jpg, etc.
                category TEXT,              -- 'document', 'image', 'executable', etc.
                is_binary BOOLEAN DEFAULT 0,
                mime_type TEXT
            )
        """)
        
        # 3. Tabelle für Verzeichnisse (erweitert)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS directories (
                id INTEGER PRIMARY KEY,
                drive_id INTEGER NOT NULL,
                parent_id INTEGER,          -- Hierarchie-Support
                directory_name TEXT NOT NULL, -- Nur der Name, nicht vollständiger Pfad
                full_path TEXT NOT NULL,    -- Cache für Performance
                depth_level INTEGER DEFAULT 0, -- Verzeichnistiefe
                FOREIGN KEY (drive_id) REFERENCES drives (id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES directories (id) ON DELETE CASCADE,
                UNIQUE (drive_id, full_path)
            )
        """)
        
        # 4. Tabelle für Dateien (komplett überarbeitet)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY,
                directory_id INTEGER NOT NULL,
                filename TEXT NOT NULL,        -- Dateiname OHNE Extension
                extension_id INTEGER,          -- Referenz zu extensions
                size INTEGER,
                hash TEXT,
                created_date TEXT,            -- Erstellungsdatum
                modified_date TEXT,           -- Änderungsdatum
                attributes INTEGER DEFAULT 0, -- Dateiattribute
                FOREIGN KEY (directory_id) REFERENCES directories (id) ON DELETE CASCADE,
                FOREIGN KEY (extension_id) REFERENCES extensions (id)
            )
        """)
        
        # 5. Standard Extensions einfügen
        self._populate_standard_extensions()
        
        # 6. Performance-Indizes (nach Tabellenerstellung)
        try:
            # MIGRATION: Der UNIQUE-Index war frueher nur (directory_id, filename)
            # OHNE extension_id. Da filename der Name OHNE Endung ist, kollidierten
            # gleichnamige Dateien mit verschiedener Endung (logo.png vs logo.svg) und
            # eine Datei ging per INSERT OR IGNORE verloren. Wir ersetzen den alten
            # Index idempotent durch die 3-spaltige Variante.
            self.cursor.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' "
                "AND name='idx_files_directory_filename'"
            )
            existing = self.cursor.fetchone()
            if existing and existing[0] and 'extension_id' not in existing[0]:
                logger.info("[DB] Migriere UNIQUE-Index -> (directory_id, filename, extension_id)")
                self.cursor.execute("DROP INDEX idx_files_directory_filename")

            indices = [
                "CREATE INDEX IF NOT EXISTS idx_directories_drive_path ON directories (drive_id, full_path)",
                "CREATE INDEX IF NOT EXISTS idx_directories_parent ON directories (parent_id)",
                "CREATE INDEX IF NOT EXISTS idx_files_filename ON files (filename)",
                "CREATE INDEX IF NOT EXISTS idx_files_extension ON files (extension_id)",
                "CREATE INDEX IF NOT EXISTS idx_files_directory ON files (directory_id)",
                "CREATE INDEX IF NOT EXISTS idx_files_size ON files (size)",
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_extensions_name ON extensions (name)",
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_files_directory_filename ON files (directory_id, filename, extension_id)",
                "CREATE INDEX IF NOT EXISTS idx_files_hash ON files (hash)",
                "CREATE INDEX IF NOT EXISTS idx_extensions_category ON extensions (category)",
                "CREATE INDEX IF NOT EXISTS idx_files_name_ext_size ON files (filename, extension_id, size)"
            ]

            for idx_sql in indices:
                self.cursor.execute(idx_sql)
        except sqlite3.Error as e:
            logger.warning(f"[DB] Warnung beim Erstellen von Indizes: {e}")
        
        # 6. Scan-Progress (unverändert)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS scan_progress (
                id INTEGER PRIMARY KEY,
                drive_id INTEGER UNIQUE NOT NULL,
                last_path TEXT,
                timestamp TEXT,
                FOREIGN KEY (drive_id) REFERENCES drives (id) ON DELETE CASCADE 
            )
        """)

        # 7. Scan-Lock (unverändert)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS scan_lock (
                id INTEGER PRIMARY KEY,
                scan_type TEXT NOT NULL,
                start_time TEXT NOT NULL,
                pid INTEGER NOT NULL,
                hostname TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1
            )
        """)
        
        # 8. Export-Log (unverändert)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS export_log (
                id INTEGER PRIMARY KEY,
                export_type TEXT,
                export_time TEXT,
                file_path TEXT
            )
        """)
        
        # 9. Deleted-Tables mit neuer Struktur
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS deleted_directories (
                id INTEGER PRIMARY KEY,
                drive_id INTEGER,
                full_path TEXT NOT NULL,
                deleted_date TEXT NOT NULL
            )
        """)
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS deleted_files (
                id INTEGER PRIMARY KEY,
                directory_id INTEGER,
                filename TEXT NOT NULL,
                extension_id INTEGER,
                deleted_date TEXT NOT NULL
            )
        """)
        
        # 10. Kompatibilitäts-View für legacy code
        self.cursor.execute("""
            CREATE VIEW IF NOT EXISTS files_legacy AS
            SELECT 
                f.id,
                f.directory_id,
                d.full_path || '/' || f.filename || COALESCE(e.name, '') as file_path,
                f.size,
                f.hash
            FROM files f
            JOIN directories d ON f.directory_id = d.id
            LEFT JOIN extensions e ON f.extension_id = e.id
        """)
        
        self.conn.commit()
        logger.info("[DB] Optimiertes Datenbankschema erstellt/aktualisiert.")

    @with_lock
    def get_or_create_drive(self, name):
        self.cursor.execute("SELECT id FROM drives WHERE name = ?", (name,))
        row = self.cursor.fetchone()
        if row:
            return row[0]
        self.cursor.execute("INSERT INTO drives (name) VALUES (?)", (name,))
        logger.info(f"[DB Commit] Committing new drive: {name}")
        self.conn.commit()
        return self.cursor.lastrowid
    
    def _populate_standard_extensions(self):
        """Fügt Standard-Extensions mit Kategorien ein."""
        standard_extensions = [
            # Documents
            ('.pdf', 'document', 1, 'application/pdf'),
            ('.doc', 'document', 1, 'application/msword'),
            ('.docx', 'document', 1, 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'),
            ('.txt', 'document', 0, 'text/plain'),
            ('.rtf', 'document', 0, 'application/rtf'),
            ('.odt', 'document', 1, 'application/vnd.oasis.opendocument.text'),
            ('.xls', 'document', 1, 'application/vnd.ms-excel'),
            ('.xlsx', 'document', 1, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
            ('.ppt', 'document', 1, 'application/vnd.ms-powerpoint'),
            ('.pptx', 'document', 1, 'application/vnd.openxmlformats-officedocument.presentationml.presentation'),
            
            # Images
            ('.jpg', 'image', 1, 'image/jpeg'),
            ('.jpeg', 'image', 1, 'image/jpeg'),
            ('.png', 'image', 1, 'image/png'),
            ('.gif', 'image', 1, 'image/gif'),
            ('.bmp', 'image', 1, 'image/bmp'),
            ('.tiff', 'image', 1, 'image/tiff'),
            ('.svg', 'image', 0, 'image/svg+xml'),
            ('.ico', 'image', 1, 'image/x-icon'),
            ('.webp', 'image', 1, 'image/webp'),
            
            # Video
            ('.mp4', 'video', 1, 'video/mp4'),
            ('.avi', 'video', 1, 'video/x-msvideo'),
            ('.mkv', 'video', 1, 'video/x-matroska'),
            ('.mov', 'video', 1, 'video/quicktime'),
            ('.wmv', 'video', 1, 'video/x-ms-wmv'),
            ('.flv', 'video', 1, 'video/x-flv'),
            ('.webm', 'video', 1, 'video/webm'),
            
            # Audio
            ('.mp3', 'audio', 1, 'audio/mpeg'),
            ('.wav', 'audio', 1, 'audio/wav'),
            ('.flac', 'audio', 1, 'audio/flac'),
            ('.aac', 'audio', 1, 'audio/aac'),
            ('.ogg', 'audio', 1, 'audio/ogg'),
            ('.wma', 'audio', 1, 'audio/x-ms-wma'),
            
            # Archives
            ('.zip', 'archive', 1, 'application/zip'),
            ('.rar', 'archive', 1, 'application/vnd.rar'),
            ('.7z', 'archive', 1, 'application/x-7z-compressed'),
            ('.tar', 'archive', 1, 'application/x-tar'),
            ('.gz', 'archive', 1, 'application/gzip'),
            
            # Executables
            ('.exe', 'executable', 1, 'application/x-msdownload'),
            ('.dll', 'executable', 1, 'application/x-msdownload'),
            ('.sys', 'executable', 1, 'application/x-msdownload'),
            ('.msi', 'executable', 1, 'application/x-msi'),
            ('.bat', 'executable', 0, 'application/x-bat'),
            ('.cmd', 'executable', 0, 'application/x-bat'),
            
            # Code
            ('.py', 'code', 0, 'text/x-python'),
            ('.js', 'code', 0, 'text/javascript'),
            ('.html', 'code', 0, 'text/html'),
            ('.css', 'code', 0, 'text/css'),
            ('.cpp', 'code', 0, 'text/x-c++src'),
            ('.java', 'code', 0, 'text/x-java-source'),
            ('.php', 'code', 0, 'text/x-php'),
            ('.sql', 'code', 0, 'text/x-sql'),
            ('.xml', 'code', 0, 'text/xml'),
            ('.json', 'code', 0, 'application/json'),
            
            # Other common
            ('[none]', 'other', 0, 'application/octet-stream'),  # Dateien ohne Extension
        ]
        
        self.cursor.executemany(
            "INSERT OR IGNORE INTO extensions (name, category, is_binary, mime_type) VALUES (?, ?, ?, ?)",
            standard_extensions
        )

    @with_lock
    def get_or_create_extension(self, ext_name):
        """Holt oder erstellt eine Extension-ID.

        Backstop gegen Bloat: unplausible Endungen (splitext-Muell) werden NICHT
        als neue Zeile angelegt, sondern auf '[none]' abgebildet. So kann auch ein
        Aufrufer, der split_name_ext nicht nutzt, keine Muell-Extension erzeugen.
        """
        if not ext_name:
            ext_name = NONE_EXT
        elif ext_name != NONE_EXT and not _is_plausible_ext(ext_name):
            ext_name = NONE_EXT

        self.cursor.execute("SELECT id FROM extensions WHERE name = ?", (ext_name,))
        row = self.cursor.fetchone()
        if row:
            return row[0]
        
        # Neue Extension erstellen - Kategorie automatisch bestimmen
        category = self._determine_extension_category(ext_name)
        is_binary = 1 if category in ['executable', 'image', 'video', 'audio', 'archive'] else 0
        
        self.cursor.execute(
            "INSERT INTO extensions (name, category, is_binary) VALUES (?, ?, ?)",
            (ext_name, category, is_binary)
        )
        self.conn.commit()
        return self.cursor.lastrowid
    
    def _determine_extension_category(self, ext):
        """Bestimmt automatisch die Kategorie einer unbekannten Extension."""
        ext_lower = ext.lower()
        
        if ext_lower in ['.doc', '.docx', '.pdf', '.txt', '.rtf', '.odt', '.xls', '.xlsx', '.ppt', '.pptx']:
            return 'document'
        elif ext_lower in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.svg', '.ico', '.webp']:
            return 'image'
        elif ext_lower in ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm']:
            return 'video'
        elif ext_lower in ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma']:
            return 'audio'
        elif ext_lower in ['.zip', '.rar', '.7z', '.tar', '.gz', '.bz2']:
            return 'archive'
        elif ext_lower in ['.exe', '.dll', '.sys', '.msi', '.bat', '.cmd', '.com']:
            return 'executable'
        elif ext_lower in ['.py', '.js', '.html', '.css', '.cpp', '.java', '.php', '.sql', '.xml', '.json']:
            return 'code'
        else:
            return 'other'

    @with_lock
    def get_or_create_directory_optimized(self, drive_id, full_path):
        """Optimierte Directory-Erstellung mit Hierarchie-Support."""
        # Normalisiere Pfad ZUERST (Windows-kompatibel)
        full_path = os.path.normpath(full_path).replace('\\', '/')
        
        # Prüfe ob bereits existiert (mit normalisiertem Pfad!)
        self.cursor.execute("SELECT id FROM directories WHERE drive_id = ? AND full_path = ?", (drive_id, full_path))
        row = self.cursor.fetchone()
        if row:
            return row[0]
        
        drive_name = self.get_drive_name(drive_id)
        
        # Root-Verzeichnis behandlung
        if full_path == drive_name or full_path == drive_name.rstrip('/'):
            directory_name = ""
            parent_id = None
            depth_level = 0
        else:
            directory_name = os.path.basename(full_path)
            parent_path = os.path.dirname(full_path).replace('\\', '/')
            
            # Berechne depth level
            relative_path = full_path[len(drive_name.rstrip('/')):].strip('/')
            depth_level = len([p for p in relative_path.split('/') if p]) if relative_path else 0
            
            # Parent-ID ermitteln - aber nur wenn parent_path nicht das Root ist
            if parent_path == drive_name.rstrip('/') or parent_path + '/' == drive_name:
                parent_id = None
            else:
                # Rekursionsschutz: nur wenn parent_path != current path
                if parent_path != full_path:
                    parent_id = self.get_or_create_directory_optimized(drive_id, parent_path)
                else:
                    parent_id = None
        
        # Neues Verzeichnis einfügen (mit Fehlerbehandlung für Race Conditions)
        try:
            self.cursor.execute(
                "INSERT INTO directories (drive_id, parent_id, directory_name, full_path, depth_level) VALUES (?, ?, ?, ?, ?)",
                (drive_id, parent_id, directory_name, full_path, depth_level)
            )
            return self.cursor.lastrowid
        except sqlite3.IntegrityError:
            # Race Condition: Ein anderer Thread hat das Verzeichnis bereits erstellt
            # Versuche es nochmal zu finden
            self.cursor.execute("SELECT id FROM directories WHERE drive_id = ? AND full_path = ?", (drive_id, full_path))
            row = self.cursor.fetchone()
            if row:
                return row[0]
            else:
                # Sollte nicht passieren, aber zur Sicherheit
                raise
    
    def get_drive_name(self, drive_id):
        """Hilfsfunktion um Drive-Namen zu holen."""
        self.cursor.execute("SELECT name FROM drives WHERE id = ?", (drive_id,))
        row = self.cursor.fetchone()
        return row[0] if row else None

    @with_lock
    def insert_file_optimized(self, directory_id, full_filename, size, hash_val, created_date=None, modified_date=None):
        """Optimierte Datei-Einfügung mit Cache und UNIQUE INDEX Kompatibilität."""
        # Filename und Extension trennen (mit Plausibilitaets-Pruefung gegen Muell-Endungen)
        filename, ext = split_name_ext(full_filename)

        # Extension-ID ermitteln
        extension_id = self.get_or_create_extension(ext)

        # PERFORMANCE: Prüfe Cache zuerst (Schluessel inkl. extension_id, damit
        # gleichnamige Dateien mit verschiedener Endung nicht kollidieren)
        in_cache = self.file_cache.check(directory_id, filename, extension_id)
        
        if in_cache is True:
            # Cache sagt: Datei existiert -> UPDATE. Falls die Zeile wider
            # Erwarten fehlt (Cache-Inkonsistenz), auf INSERT zurückfallen,
            # damit die Datei nicht verloren geht.
            self.cursor.execute("""
                UPDATE files
                SET size = ?, hash = ?, modified_date = COALESCE(?, datetime('now'))
                WHERE directory_id = ? AND filename = ? AND extension_id IS ?
            """, (size, hash_val, modified_date, directory_id, filename, extension_id))
            if self.cursor.rowcount == 0:
                self.cursor.execute("""
                    INSERT OR IGNORE INTO files
                    (directory_id, filename, extension_id, size, hash, created_date, modified_date)
                    VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, datetime('now')))
                """, (directory_id, filename, extension_id, size, hash_val, created_date, modified_date))
            self.file_cache.add(directory_id, filename, extension_id)
            return self.cursor.lastrowid

        else:
            # Cache unbekannt (None) = DB-Check.
            # (FileCache.check liefert nur True oder None, nie False – ein
            #  "definitiv nicht im Cache"-Zweig wäre toter Code.)
            # Versuche erst zu aktualisieren (wenn Datei existiert)
            self.cursor.execute("""
                UPDATE files
                SET size = ?, hash = ?, modified_date = COALESCE(?, datetime('now'))
                WHERE directory_id = ? AND filename = ? AND extension_id IS ?
            """, (size, hash_val, modified_date, directory_id, filename, extension_id))

            if self.cursor.rowcount > 0:
                # UPDATE erfolgreich = Datei existierte
                self.file_cache.add(directory_id, filename, extension_id)
            else:
                # Keine Zeile aktualisiert = INSERT nötig
                self.cursor.execute("""
                    INSERT OR IGNORE INTO files
                    (directory_id, filename, extension_id, size, hash, created_date, modified_date)
                    VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, datetime('now')))
                """, (directory_id, filename, extension_id, size, hash_val, created_date, modified_date))
                if self.cursor.rowcount > 0:
                    self.file_cache.add(directory_id, filename, extension_id)

            return self.cursor.lastrowid

    @with_lock
    def get_or_create_directory(self, drive_id, path):
        """Legacy-Kompatibilitätsfunktion - verwendet optimierte Version."""
        return self.get_or_create_directory_optimized(drive_id, path)

    @with_lock
    def batch_insert_files(self, file_tuples):
        """Optimierte Batch-Insertion für neue Datenbankstruktur.
        file_tuples: [(dir_id, full_filename, size, hash_val), ...]
        """
        try:
            # Konvertiere zu optimierter Struktur
            optimized_tuples = []
            for dir_id, full_filename, size, hash_val in file_tuples:
                # Parse filename und extension (mit Plausibilitaets-Pruefung)
                basename = os.path.basename(full_filename) if '/' in full_filename or '\\' in full_filename else full_filename
                filename, ext = split_name_ext(basename)

                # Extension-ID ermitteln (Bulk-Optimierung möglich)
                extension_id = self.get_or_create_extension(ext)

                optimized_tuples.append((dir_id, filename, extension_id, size, hash_val))
            
            # Batch-Insert in optimierte Tabelle mit Cache-Unterstützung
            updates = []
            inserts = []
            
            for dir_id, filename, extension_id, size, hash_val in optimized_tuples:
                # Prüfe Cache für bessere Performance (Schluessel inkl. extension_id,
                # sonst kollidieren gleichnamige Dateien mit verschiedener Endung)
                in_cache = self.file_cache.check(dir_id, filename, extension_id)

                if in_cache is True:
                    # Definitiv existiert = UPDATE
                    updates.append((size, hash_val, dir_id, filename, extension_id))
                else:
                    # Cache unbekannt (None) = einzeln per DB-Check entscheiden.
                    # (FileCache.check liefert nie False.)
                    self.cursor.execute("""
                        SELECT 1 FROM files
                        WHERE directory_id = ? AND filename = ? AND extension_id IS ?
                        LIMIT 1
                    """, (dir_id, filename, extension_id))

                    if self.cursor.fetchone():
                        updates.append((size, hash_val, dir_id, filename, extension_id))
                        self.file_cache.add(dir_id, filename, extension_id)
                    else:
                        inserts.append((dir_id, filename, extension_id, size, hash_val))
                        self.file_cache.add(dir_id, filename, extension_id)

            # Batch-UPDATE
            if updates:
                self.cursor.executemany("""
                    UPDATE files
                    SET size = ?, hash = ?, modified_date = datetime('now')
                    WHERE directory_id = ? AND filename = ? AND extension_id IS ?
                """, updates)
            
            # Batch-INSERT
            if inserts:
                self.cursor.executemany("""
                    INSERT OR IGNORE INTO files 
                    (directory_id, filename, extension_id, size, hash, modified_date) 
                    VALUES (?, ?, ?, ?, ?, datetime('now'))
                """, inserts)
            
        except sqlite3.Error as e:
            num_tuples = len(file_tuples) if file_tuples else 0
            first_tuple_example = file_tuples[0] if file_tuples else "N/A"
            logger.error(f"[DB Fehler] Fehler bei batch_insert_files (optimized) mit {num_tuples} Tupeln. Erstes Tupel: {first_tuple_example}. Fehler: {e}")
        except Exception as e:
             logger.error(f"[DB Fehler] Unerwarteter Fehler bei batch_insert_files (optimized): {e}")

    @with_lock
    def get_last_scan_path(self, drive_id):
        self.cursor.execute("SELECT last_path FROM scan_progress WHERE drive_id = ?", (drive_id,))
        row = self.cursor.fetchone()
        return row[0] if row else None

    @with_lock
    def update_scan_progress(self, drive_id, path):
        timestamp = datetime.now().isoformat()
        self.cursor.execute("SELECT id FROM scan_progress WHERE drive_id = ?", (drive_id,))
        row = self.cursor.fetchone()
        if row:
            self.cursor.execute(
                "UPDATE scan_progress SET last_path = ?, timestamp = ? WHERE drive_id = ?",
                (path, timestamp, drive_id)
            )
        else:
            self.cursor.execute(
                "INSERT INTO scan_progress (drive_id, last_path, timestamp) VALUES (?, ?, ?)",
                (drive_id, path, timestamp)
            )
        logger.info(f"[DB Commit] Committing scan progress update: drive_id={drive_id}, last_path={path}")
        self.conn.commit()

    @with_lock
    def cleanup_removed_dirs(self, drive_id, scanned_paths_set):
        """Entfernt Verzeichnisse dieses Laufwerks, die weder im frisch
        gescannten Set enthalten sind NOCH auf dem Dateisystem existieren.

        Hinweis: Spalte ist im aktuellen Schema 'full_path' (nicht 'path').
        Die zu löschenden IDs werden zuerst vollständig ermittelt und erst
        danach gelöscht, um Iteration und Mutation desselben Cursors zu trennen.
        """
        self.cursor.execute(
            "SELECT id, full_path FROM directories WHERE drive_id = ?", (drive_id,)
        )
        stale_ids = [
            dir_id
            for dir_id, full_path in self.cursor.fetchall()
            if full_path not in scanned_paths_set and not os.path.exists(full_path)
        ]
        for dir_id in stale_ids:
            self.cursor.execute("DELETE FROM directories WHERE id = ?", (dir_id,))

    @with_lock
    def cleanup_removed_files(self, scanned_file_paths_set):
        """Entfernt Dateien, die weder im frisch gescannten Set enthalten sind
        NOCH auf dem Dateisystem existieren.

        Der vollständige Pfad existiert im aktuellen Schema nicht mehr als
        Spalte, sondern wird aus directories.full_path + files.filename +
        Extension rekonstruiert. Die Platzhalter-Extension '[none]' (Dateien
        ohne Endung) wird dabei NICHT an den Pfad angehängt.
        """
        self.cursor.execute(
            """
            SELECT f.id,
                   d.full_path || '/' || f.filename ||
                   CASE
                       WHEN e.name IS NULL OR e.name = '[none]' THEN ''
                       ELSE e.name
                   END AS file_path
            FROM files f
            JOIN directories d ON f.directory_id = d.id
            LEFT JOIN extensions e ON f.extension_id = e.id
            """
        )
        stale_ids = [
            file_id
            for file_id, file_path in self.cursor.fetchall()
            if file_path not in scanned_file_paths_set and not os.path.exists(file_path)
        ]
        for file_id in stale_ids:
            self.cursor.execute("DELETE FROM files WHERE id = ?", (file_id,))

    @with_lock
    def clear_drive_data(self, drive_id):
        """Löscht alle Daten eines spezifischen Laufwerks (für --restart).
        
        WICHTIG: Löscht NUR die Daten des angegebenen Laufwerks!
        Andere Laufwerke bleiben unberührt.
        
        Args:
            drive_id: Die ID des Laufwerks, dessen Daten gelöscht werden sollen
        """
        try:
            # Zähle vorher die Daten für Logging
            self.cursor.execute("SELECT COUNT(*) FROM directories WHERE drive_id = ?", (drive_id,))
            dir_count = self.cursor.fetchone()[0]
            
            self.cursor.execute("""
                SELECT COUNT(*) FROM files f 
                JOIN directories d ON f.directory_id = d.id 
                WHERE d.drive_id = ?
            """, (drive_id,))
            file_count = self.cursor.fetchone()[0]
            
            # Lösche alle Verzeichnisse des Laufwerks (CASCADE löscht automatisch alle Dateien)
            self.cursor.execute("DELETE FROM directories WHERE drive_id = ?", (drive_id,))
            
            # Lösche auch den Scan-Fortschritt für dieses Laufwerk
            self.cursor.execute("DELETE FROM scan_progress WHERE drive_id = ?", (drive_id,))
            
            self.conn.commit()
            logger.info(f"[DB] Daten für Laufwerk ID {drive_id} gelöscht: {dir_count} Verzeichnisse, {file_count} Dateien")
            
            return True
        except Exception as e:
            logger.error(f"[DB] Fehler beim Löschen der Laufwerksdaten: {e}")
            self.conn.rollback()
            return False

    @with_lock
    def close(self):
        logger.info("[DB Commit] Committing final changes on DB close.")
        self.conn.commit()
        self.conn.close()
        # Singleton zurücksetzen, damit get_db_instance() eine neue Verbindung erstellt
        global _db_instance
        _db_instance = None

    @with_lock
    def acquire_scan_lock(self, scan_type="manual"):
        """Versucht, einen Scan-Lock zu erwerben.
        
        Args:
            scan_type: Der Typ des Scans ("manual", "scheduled", usw.)
            
        Returns:
            int or None: Die ID des erworbenen Locks oder None, wenn ein anderer Scan läuft
        """
        # Prüfe, ob bereits ein aktiver Scan läuft
        self.cursor.execute("SELECT id, scan_type, start_time, pid, hostname FROM scan_lock WHERE is_active=1")
        active_scan = self.cursor.fetchone()
        
        if active_scan:
            # Prüfe, ob der Prozess noch läuft (falls PID auf diesem Host)
            lock_id, lock_type, lock_time, lock_pid, lock_hostname = active_scan
            import socket, os
            current_hostname = socket.gethostname()
            
            # Hostname-Vergleich robust (Groß/Kleinschreibung)
            same_host = str(lock_hostname or "").strip().lower() == str(current_hostname or "").strip().lower()

            if same_host:
                import psutil
                try:
                    # Wenn PID nicht mehr existiert, ist der Scan vermutlich abgestürzt
                    if not psutil.pid_exists(lock_pid):
                        logger.warning(f"[DB] Verwaister Scan-Lock gefunden (PID {lock_pid} existiert nicht mehr). Setze Lock zurück.")
                        self.release_scan_lock(lock_id)
                        active_scan = None
                    else:
                        # PID-Reuse absichern: nur echte Scan-Prozesse akzeptieren
                        p = psutil.Process(lock_pid)
                        cmdline = " ".join(p.cmdline()).lower()
                        scan_markers = [
                            "scanner_core.py",
                            "scan_all_drives.py",
                            "integrity_checker.py",
                            "scheduled_scanner.py",
                            "watchdog_service.py",
                        ]
                        if not any(m in cmdline for m in scan_markers):
                            logger.warning(f"[DB] Verwaister Scan-Lock gefunden (PID {lock_pid} gehört nicht zu Scan-Prozess). Setze Lock zurück.")
                            self.release_scan_lock(lock_id)
                            active_scan = None
                except:
                    # Falls PID nicht prüfbar: alte Locks trotzdem heilen
                    try:
                        import datetime as _dt
                        lock_dt = _dt.datetime.fromisoformat(str(lock_time))
                        age = (_dt.datetime.now() - lock_dt).total_seconds()
                        if age > 6 * 3600:
                            logger.warning(f"[DB] PID {lock_pid} nicht prüfbar, aber Lock ist alt ({age/3600:.1f}h). Setze Lock zurück.")
                            self.release_scan_lock(lock_id)
                            active_scan = None
                        else:
                            logger.warning(f"[DB] Konnte PID {lock_pid} nicht prüfen. Nehme an, der Scan läuft noch.")
                    except Exception:
                        logger.warning(f"[DB] Konnte PID {lock_pid} nicht prüfen. Nehme an, der Scan läuft noch.")
            else:
                # Fallback: sehr alte aktive Locks als verwaist behandeln (z.B. Hostname-Formatwechsel)
                try:
                    import datetime as _dt
                    lock_dt = _dt.datetime.fromisoformat(str(lock_time))
                    age = (_dt.datetime.now() - lock_dt).total_seconds()
                    if age > 6 * 3600:
                        logger.warning(f"[DB] Sehr alter aktiver Scan-Lock gefunden ({age/3600:.1f}h, Host {lock_hostname}). Setze Lock zurück.")
                        self.release_scan_lock(lock_id)
                        active_scan = None
                except Exception:
                    pass
            
            # Wenn immer noch ein aktiver Scan läuft, kein Lock erwerben
            if active_scan:
                logger.warning(f"[DB] Kann Scan-Lock nicht erwerben, aktiver Scan im Gange: {lock_type} (PID: {lock_pid}@{lock_hostname}, Start: {lock_time})")
                return None
        
        # Kein aktiver Scan, wir können einen Lock erwerben.
        # Vorher: inaktive Historie selbst beschneiden (kein externer Cleanup noetig).
        self.cleanup_scan_lock_history(keep_recent=SCAN_LOCK_HISTORY_KEEP)

        import socket, os, datetime
        current_time = datetime.datetime.now().isoformat()
        current_pid = os.getpid()
        current_hostname = socket.gethostname()

        self.cursor.execute(
            "INSERT INTO scan_lock (scan_type, start_time, pid, hostname, is_active) VALUES (?, ?, ?, ?, 1)",
            (scan_type, current_time, current_pid, current_hostname)
        )
        lock_id = self.cursor.lastrowid
        self.conn.commit()
        
        logger.info(f"[DB] Scan-Lock erworben: ID {lock_id}, Typ {scan_type}, PID {current_pid}@{current_hostname}")
        return lock_id
    
    @with_lock
    def release_scan_lock(self, lock_id=None):
        """Gibt einen Scan-Lock frei.
        
        Args:
            lock_id: Optional die ID des zu beendenden Locks, sonst wird der aktive Lock freigegeben
        
        Returns:
            bool: True wenn erfolgreich, False sonst
        """
        if lock_id is None:
            # Finde den aktiven Lock
            self.cursor.execute("SELECT id FROM scan_lock WHERE is_active=1")
            row = self.cursor.fetchone()
            if not row:
                logger.warning("[DB] Kein aktiver Scan-Lock zum Freigeben gefunden.")
                return False
            lock_id = row[0]
        
        try:
            self.cursor.execute("UPDATE scan_lock SET is_active=0 WHERE id=?", (lock_id,))
            self.conn.commit()
            logger.info(f"[DB] Scan-Lock {lock_id} freigegeben.")
            return True
        except Exception as e:
            logger.error(f"[DB] Fehler beim Freigeben des Scan-Locks {lock_id}: {e}")
            return False
    
    @with_lock
    def is_scan_running(self):
        """Prüft, ob aktuell ein Scan läuft.
        
        Returns:
            bool: True wenn ein Scan aktiv ist, False sonst
        """
        try:
            self.cursor.execute("SELECT COUNT(*) FROM scan_lock WHERE is_active=1")
            row = self.cursor.fetchone()
            if row is None:
                return False
            count = row[0]
            if count is None:
                return False
            return count > 0
        except Exception:
            return False

    @with_lock
    def cleanup_scan_lock_history(self, keep_recent=20):
        """Entfernt alte, INAKTIVE scan_lock-Einträge (Audit-Bloat).

        Behält alle aktiven Locks sowie die ``keep_recent`` neuesten inaktiven
        Einträge. Gibt die Anzahl gelöschter Einträge zurück.
        """
        self.cursor.execute(
            """
            DELETE FROM scan_lock
            WHERE is_active = 0
              AND id NOT IN (
                  SELECT id FROM scan_lock WHERE is_active = 0
                  ORDER BY id DESC LIMIT ?
              )
            """,
            (keep_recent,),
        )
        removed = self.cursor.rowcount
        self.conn.commit()
        return removed

    @with_lock
    def cleanup_orphan_extensions(self):
        """Entfernt verwaiste, NICHT-standardisierte Extension-Einträge.

        Löscht Extensions, die von KEINER Datei genutzt werden UND keinen
        mime_type haben (= automatisch angelegte „Müll"-Extensions, die nach
        dem Löschen ihrer Dateien zurückbleiben, da es kein CASCADE auf
        extensions gibt). Standard-Extensions (mit mime_type) und aktuell
        genutzte bleiben unangetastet. Gibt die Anzahl gelöschter Einträge zurück.
        """
        self.cursor.execute(
            """
            DELETE FROM extensions
            WHERE mime_type IS NULL
              AND id NOT IN (
                  SELECT DISTINCT extension_id FROM files
                  WHERE extension_id IS NOT NULL
              )
            """
        )
        removed = self.cursor.rowcount
        self.conn.commit()
        return removed

    def read_query(self, sql, params=()):
        """Führt eine reine Lese-Abfrage aus und gibt fetchall() zurück.

        Für Datei-DBs wird eine SEPARATE read-only Connection verwendet, damit
        Lesezugriffe (GUI, Reports) nicht vom Schreib-Lock der Haupt-Connection
        blockiert werden – SQLite WAL erlaubt einen Writer und beliebig viele
        Reader gleichzeitig. Für ':memory:' (Tests) wird die Haupt-Connection
        mit eigenem Cursor genutzt, da es dort keinen separaten Pfad gibt.
        """
        if self.path == ":memory:":
            with _db_lock:
                cur = self.conn.cursor()
                try:
                    cur.execute(sql, params)
                    return cur.fetchall()
                finally:
                    cur.close()

        conn = sqlite3.connect(
            "file:" + self.path.replace("\\", "/") + "?mode=ro",
            uri=True,
            timeout=30.0,
        )
        try:
            conn.execute("PRAGMA query_only = ON")
            cur = conn.cursor()
            cur.execute(sql, params)
            return cur.fetchall()
        finally:
            conn.close()

def get_db_instance(path=None):
    """Gibt eine globale, thread-sichere Singleton-Instanz des DBManagers zurück."""
    global _db_instance, _db_path
    with _db_lock: # Schützt den Zugriff auf globale Variablen
        # Bestimme den zu verwendenden Pfad: Übergebener Pfad hat Vorrang, sonst Standard aus utils
        db_path_to_use = path or DB_PATH

        # Sicherheitscheck: Instanz vorhanden aber Verbindung bereits geschlossen?
        if _db_instance is not None:
            try:
                _db_instance.conn.execute("SELECT 1")
            except Exception:
                logger.warning("[DB] Bestehende Instanz hat geschlossene Verbindung – wird neu erstellt.")
                _db_instance = None

        if _db_instance is None:
            # Erster Aufruf oder nach Schließen/Änderung
            logger.info(f"[DB] Erstelle neue DB-Instanz für: {db_path_to_use}")
            # Prüfe, ob ein gültiger Pfad ermittelt wurde
            if not db_path_to_use:
                 logger.critical("[DB Fehler] Kritisch: Kein DB-Pfad verfügbar (weder übergeben noch Standard).")
                 raise ValueError("DB-Pfad konnte nicht ermittelt werden.")
            # Erstelle die Instanz mit dem ermittelten Pfad
            _db_instance = DBManager(db_path_to_use)
            _db_path = db_path_to_use
        elif db_path_to_use != _db_path:
            # Pfad hat sich geändert, alte Instanz schließen, neue erstellen
            logger.info(f"[DB] Pfad geändert von {_db_path} zu {db_path_to_use}. Erstelle neue Instanz.")
            if not db_path_to_use: # Auch hier prüfen
                 logger.critical("[DB Fehler] Kritisch: Versuch, auf einen leeren DB-Pfad zu wechseln.")
                 raise ValueError("Neuer DB-Pfad darf nicht leer sein.")
            if _db_instance: # Nur schließen, wenn Instanz existierte
                 _db_instance.close()
            _db_instance = DBManager(db_path_to_use)
            _db_path = db_path_to_use
        # else: Instanz existiert und Pfad ist gleich, nichts zu tun

        # Gib die (möglicherweise neu erstellte) Instanz zurück
        return _db_instance

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)
