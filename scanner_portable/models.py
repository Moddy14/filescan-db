import os
import sqlite3
import threading
from datetime import datetime
import logging

# Importiere den globalen Logger aus utils
from utils import logger, DB_PATH, CONFIG

_db_lock = threading.RLock()
_db_instance = None
_db_path = None

class DBManager:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.cursor = self.conn.cursor()
        self.path = db_path
        self.lock = threading.Lock()
        self.connect()
        self.ensure_schema()

    def connect(self):
        # ... (unverändert)
        pass # Hinzugefügt, um Einrückungsfehler zu beheben

    def with_lock(func):
        def wrapper(self, *args, **kwargs):
            with _db_lock:
                return func(self, *args, **kwargs)
        return wrapper

    @with_lock
    def ensure_schema(self):
        # ... (unverändert)
        pass # Hinzugefügt, um Einrückungsfehler zu beheben

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

    @with_lock
    def get_or_create_directory(self, drive_id, path):
        self.cursor.execute("SELECT id FROM directories WHERE drive_id = ? AND path = ?", (drive_id, path))
        row = self.cursor.fetchone()
        if row:
            return row[0]
        else:
            self.cursor.execute("INSERT INTO directories (drive_id, path) VALUES (?, ?)", (drive_id, path))
            return self.cursor.lastrowid

    @with_lock
    def batch_insert_files(self, file_tuples):
        # file_tuples ist eine Liste von Tupeln: [(dir_id, file_path, size, hash_val), ...]
        try:
            # Verwende INSERT OR REPLACE, um vorhandene Dateien zu aktualisieren
            # SQL-Statement hat nur eine Gruppe von Platzhaltern
            sql = "INSERT OR REPLACE INTO files (directory_id, file_path, size, hash) VALUES (?, ?, ?, ?)"
            # executemany erwartet SQL und eine Liste von Tupeln
            self.cursor.executemany(sql, file_tuples)
            # Kein Commit hier, da äußere Transaktion in scanner_core.py
        except sqlite3.Error as e:
            # Gib mehr Kontext im Fehlerfall aus
            num_tuples = len(file_tuples) if file_tuples else 0
            first_tuple_example = file_tuples[0] if file_tuples else "N/A"
            logger.error(f"[DB Fehler] Fehler bei batch_insert_files (executemany) mit {num_tuples} Tupeln. Erstes Tupel: {first_tuple_example}. Fehler: {e}")
        except Exception as e: # Fange auch andere Fehler ab
             logger.error(f"[DB Fehler] Unerwarteter Fehler bei batch_insert_files (executemany): {e}")

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
        self.cursor.execute("SELECT id, path FROM directories WHERE drive_id = ?", (drive_id,))
        for dir_id, path in self.cursor.fetchall():
            if path not in scanned_paths_set and not os.path.exists(path):
                self.cursor.execute("DELETE FROM directories WHERE id = ?", (dir_id,))

    @with_lock
    def cleanup_removed_files(self, scanned_file_paths_set):
        self.cursor.execute("SELECT id, file_path FROM files")
        for file_id, path in self.cursor.fetchall():
            if path not in scanned_file_paths_set and not os.path.exists(path):
                self.cursor.execute("DELETE FROM files WHERE id = ?", (file_id,))

    @with_lock
    def close(self):
        logger.info("[DB Commit] Committing final changes on DB close.")
        self.conn.commit()
        self.conn.close()

def get_db_instance(path=None):
    """Gibt eine globale, thread-sichere Singleton-Instanz des DBManagers zurück."""
    global _db_instance, _db_path
    with _db_lock: # Schützt den Zugriff auf globale Variablen
        # Bestimme den zu verwendenden Pfad: Übergebener Pfad hat Vorrang, sonst Standard aus utils
        db_path_to_use = path or DB_PATH

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
