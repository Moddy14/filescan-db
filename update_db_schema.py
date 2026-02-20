import os
import sys
import sqlite3
import shutil
import time
import logging

# Importiere zentrale Funktionen und Konstanten
try:
    from utils import logger, DB_PATH, setup_logging, PROJECT_DIR
except ImportError:
    print("FEHLER: utils.py nicht gefunden. Stelle sicher, dass das Skript im Hauptverzeichnis des Projekts liegt.")
    # Fallback für Logging, wenn utils nicht importiert werden kann
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    # Versuche, DB_PATH trotzdem zu definieren (Standardpfad)
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Dateien.db")

# SQL-Befehle zum Aktualisieren des Schemas
SQL_COMMANDS = """
-- 0. Fremdschlüsselprüfung vorübergehend deaktivieren
PRAGMA foreign_keys = OFF;

-- 1. Transaktion starten
BEGIN TRANSACTION;

-- ----------------------------------
-- Tabelle 'directories' anpassen
-- ----------------------------------

-- 1.1 Neue Tabelle erstellen (mit CASCADE und UNIQUE)
CREATE TABLE directories_new (
    id INTEGER PRIMARY KEY,
    drive_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    FOREIGN KEY (drive_id) REFERENCES drives(id) ON DELETE CASCADE,
    UNIQUE (drive_id, path)
);

-- 1.2 Daten kopieren (mit Deduplizierung: Wähle MIN(id) für jede drive_id/path Kombination)
INSERT INTO directories_new (id, drive_id, path)
SELECT MIN(id), drive_id, path 
FROM directories 
GROUP BY drive_id, path;

-- 1.3 Alte Tabelle löschen
DROP TABLE directories;

-- 1.4 Neue Tabelle umbenennen
ALTER TABLE directories_new RENAME TO directories;

-- 1.5 Index neu erstellen (Name aus deiner Analyse)
CREATE INDEX idx_directories_drive_path ON directories(drive_id, path);

-- 1.6 Trigger neu erstellen (Definition aus deiner Analyse)
CREATE TRIGGER trg_after_delete_directories
AFTER DELETE ON directories
BEGIN
    INSERT INTO deleted_directories (drive_id, path, deleted_date)
    VALUES (OLD.drive_id, OLD.path, datetime('now'));
END;

-- ----------------------------------
-- Tabelle 'files' anpassen
-- ----------------------------------

-- 2.1 Neue Tabelle erstellen (mit CASCADE und UNIQUE)
CREATE TABLE files_new (
    id INTEGER PRIMARY KEY,
    directory_id INTEGER NOT NULL,
    file_path TEXT UNIQUE NOT NULL,
    size INTEGER,
    hash TEXT,
    FOREIGN KEY (directory_id) REFERENCES directories(id) ON DELETE CASCADE
);

-- 2.2 Daten kopieren
-- Wichtiger Hinweis: Wenn Duplikate in 'directories' existierten, könnten einige
-- Einträge in 'files' jetzt auf eine 'directory_id' verweisen, die wir 
-- beim Deduplizieren *nicht* übernommen haben (weil wir MIN(id) gewählt haben).
-- Wir müssen sicherstellen, dass wir nur Dateien kopieren, deren directory_id
-- noch in der *neuen* directories-Tabelle existiert.
INSERT INTO files_new (id, directory_id, file_path, size, hash)
SELECT f.id, f.directory_id, f.file_path, f.size, f.hash 
FROM files f
JOIN directories d ON f.directory_id = d.id; -- Join mit der *neuen* directories Tabelle!

-- 2.3 Alte Tabelle löschen
DROP TABLE files;

-- 2.4 Neue Tabelle umbenennen
ALTER TABLE files_new RENAME TO files;

-- 2.5 Indizes neu erstellen (Namen aus deiner Analyse)
-- Der UNIQUE Index für file_path wird automatisch erstellt
CREATE INDEX idx_files_directory_id ON files(directory_id);

-- 2.6 Trigger neu erstellen (Definition aus deiner Analyse)
CREATE TRIGGER trg_after_delete_files
AFTER DELETE ON files
BEGIN
    INSERT INTO deleted_files (file_path, deleted_date)
    VALUES (OLD.file_path, datetime('now'));
END;

-- ----------------------------------
-- Tabelle 'scan_progress' anpassen
-- ----------------------------------

-- 3.1 Neue Tabelle erstellen (mit CASCADE und UNIQUE)
CREATE TABLE scan_progress_new (
    id INTEGER PRIMARY KEY,
    drive_id INTEGER UNIQUE NOT NULL,
    last_path TEXT,
    timestamp TEXT,
    FOREIGN KEY (drive_id) REFERENCES drives(id) ON DELETE CASCADE
);

-- 3.2 Daten kopieren
INSERT INTO scan_progress_new (id, drive_id, last_path, timestamp)
SELECT id, drive_id, last_path, timestamp FROM scan_progress;

-- 3.3 Alte Tabelle löschen
DROP TABLE scan_progress;

-- 3.4 Neue Tabelle umbenennen
ALTER TABLE scan_progress_new RENAME TO scan_progress;

-- 3.5 Index neu erstellen (Name aus deiner Analyse)
-- Da drive_id jetzt UNIQUE ist, ist der Index 'idx_scan_progress_drive_id' nicht mehr nötig bzw. wird automatisch erstellt.

-- ----------------------------------
-- Abschluss
-- ----------------------------------

-- 4. Transaktion abschließen
COMMIT;

-- 5. Fremdschlüsselprüfung wieder aktivieren
PRAGMA foreign_keys = ON;
"""

def update_schema():
    """Führt die Schema-Aktualisierung durch."""
    
    if not os.path.exists(DB_PATH):
        logger.error(f"Datenbankdatei nicht gefunden: {DB_PATH}")
        return False

    # --- Backup erstellen --- 
    backup_path = DB_PATH + ".bak"
    try:
        logger.info(f"Erstelle Backup der Datenbank nach: {backup_path}")
        shutil.copy2(DB_PATH, backup_path)
        logger.info("Backup erfolgreich erstellt.")
    except Exception as e:
        logger.error(f"Fehler beim Erstellen des Backups: {e}")
        logger.error("Schema-Update wird NICHT durchgeführt. Bitte Backup manuell erstellen.")
        return False

    # --- Bestätigung einholen --- 
    print("\nWARNUNG: Dieses Skript wird das Schema der Datenbank ändern.")
    print(f"Ein Backup wurde nach '{backup_path}' erstellt.")
    confirm = input("Möchten Sie fortfahren? (ja/nein): ")
    if confirm.lower() != 'ja':
        logger.info("Schema-Update abgebrochen.")
        return False

    # --- Schema aktualisieren --- 
    conn = None
    try:
        logger.info(f"Verbinde mit Datenbank: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        logger.info("Führe Schema-Update-Befehle aus...")
        cursor.executescript(SQL_COMMANDS)
        
        logger.info("Schema-Update erfolgreich abgeschlossen.")
        logger.info("Überprüfe Datenbank-Integrität...")
        cursor.execute("PRAGMA integrity_check;")
        result = cursor.fetchone()
        if result[0] == "ok":
             logger.info("Datenbank-Integritätsprüfung erfolgreich.")
        else:
             logger.warning(f"Datenbank-Integritätsprüfung meldet: {result[0]}")
        
        conn.close()
        return True
        
    except sqlite3.Error as e:
        logger.error(f"SQLite Fehler während des Schema-Updates: {e}")
        if conn: 
            try:
                logger.info("Versuche Rollback...")
                conn.rollback()
            except Exception as rb_ex:
                 logger.error(f"Fehler beim Rollback: {rb_ex}")
        return False
    except Exception as e:
        logger.error(f"Allgemeiner Fehler während des Schema-Updates: {e}")
        return False
    finally:
        if conn:
            conn.close()
            logger.info("Datenbankverbindung geschlossen.")

if __name__ == "__main__":
    # Versuche, Logging über utils zu initialisieren
    try:
        logger = setup_logging(log_filename="schema_update.log", level_str="INFO")
    except NameError:
        # Fallback wurde bereits oben initialisiert
        pass 
        
    logger.info("===== Starte Datenbank Schema Update Skript ====")
    if update_schema():
        logger.info("===== Schema Update erfolgreich beendet ====")
    else:
        logger.error("===== Schema Update fehlgeschlagen ====")
        logger.error("Bitte überprüfe die Logs und stelle ggf. das Backup wieder her.")
        sys.exit(1) 