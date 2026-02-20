import os
import sys
import sqlite3
import time
import logging

# Importiere zentrale Funktionen und Konstanten
from utils import logger, DB_PATH, CONFIG, PROJECT_DIR, calculate_hash, HASHING
from models import get_db_instance

def check_integrity(db, check_base_path=None):
    """Prüft die Integrität der Datenbankeinträge gegen das Dateisystem.

    Args:
        db: Die DBManager-Instanz.
        check_base_path: Optionaler Basispfad. Wenn angegeben, werden nur
                         Einträge innerhalb dieses Pfades geprüft.
    """
    start_time = time.time()
    if check_base_path:
        check_base_path = os.path.normpath(check_base_path)
        logger.info(f"[Integrität] Starte Integritätsprüfung für Pfad: {check_base_path}...")
    else:
        logger.info("[Integrität] Starte globale Integritätsprüfung...")

    cursor = db.cursor
    missing_dirs = 0
    missing_files = 0
    updated_files = 0
    checked_dirs = 0
    checked_files = 0

    try:
        # --- Verzeichnisse prüfen --- 
        logger.info("[Integrität] Prüfe Verzeichnisse...")
        dir_query = "SELECT id, full_path FROM directories"
        dir_params = []
        if check_base_path:
            # Prüfe nur Verzeichnisse, die mit dem Basispfad beginnen oder der Basispfad selbst sind
            dir_query += " WHERE full_path LIKE ? OR full_path = ?"
            dir_params.extend([check_base_path + os.sep + '%', check_base_path])

        cursor.execute(dir_query, dir_params)
        # Kopiere die Ergebnisse, um Cursor-Probleme während des Löschens zu vermeiden
        dirs_to_check = cursor.fetchall()

        dirs_to_delete = []
        for dir_id, full_path in dirs_to_check:
            checked_dirs += 1
            # Prüfe Existenz
            if not os.path.isdir(full_path):
                dirs_to_delete.append((dir_id,))
                logger.error(f"[FEHLT] Verzeichnis fehlt: {full_path}")
                missing_dirs += 1

        if dirs_to_delete:
            logger.info(f"[Integrität] Entferne {len(dirs_to_delete)} fehlende Verzeichnisse...")
            # Lösche fehlende Verzeichnisse (Trigger loggt in deleted_directories)
            # ON DELETE CASCADE sollte abhängige Dateien löschen
            cursor.executemany("DELETE FROM directories WHERE id = ?", dirs_to_delete)
            db.conn.commit()

        # --- Dateien prüfen --- 
        logger.info("[Integrität] Prüfe Dateien...")
        # Erweiterte Query für optimierte Datenbankstruktur - rekonstruiert vollständigen Pfad
        file_query = """
            SELECT f.id, 
                   d.full_path || '/' || f.filename || COALESCE(e.name, '') as file_path,
                   f.size, f.hash 
            FROM files f
            JOIN directories d ON f.directory_id = d.id
            LEFT JOIN extensions e ON f.extension_id = e.id
        """
        file_params = []

        if check_base_path:
            # Filter nach Verzeichnispfad
            file_query += " WHERE d.full_path LIKE ? OR d.full_path = ?"
            file_params.extend([check_base_path + os.sep + '%', check_base_path])

        cursor.execute(file_query, file_params)
        # Verarbeite Dateien in Chunks, um Speicher zu sparen
        chunk_size = 500
        files_to_delete = []
        files_to_update = []

        while True:
            files_chunk = cursor.fetchmany(chunk_size)
            if not files_chunk:
                break

            for file_id, file_path, size_old, hash_old in files_chunk:
                checked_files += 1
                # Normalisiere Pfad für Windows-Kompatibilität
                file_path = os.path.normpath(file_path.replace('/', os.sep))
                
                # Prüfe Existenz
                if not os.path.isfile(file_path):
                    files_to_delete.append((file_id,))
                    logger.error(f"[FEHLT] Datei fehlt: {file_path}")
                    missing_files += 1
                else:
                    try:
                        size_new = os.path.getsize(file_path)
                        hash_new = calculate_hash(file_path) if HASHING else None # Hash nur neu berechnen, wenn Hashing aktiv

                        needs_update = False
                        if size_new != size_old:
                            needs_update = True
                        # Prüfe Hash nur, wenn Hashing aktiv ist UND der alte Hash nicht None war (oder der neue nicht None ist)
                        elif HASHING and hash_new is not None and hash_new != hash_old:
                            needs_update = True
                        # Optional: Fall behandeln, wo alter Hash None war, neuer aber berechnet wurde?
                        # elif HASHING and hash_new is not None and hash_old is None:
                        #    needs_update = True

                        if needs_update:
                            files_to_update.append((size_new, hash_new, file_id))
                            logger.info(f"[GEÄNDERT] Datei geändert: {file_path} (Size: {size_old}->{size_new}, Hash: {(hash_old or 'N/A')[:8]}->{(hash_new or 'N/A')[:8]})")
                            updated_files += 1

                    except PermissionError:
                        logger.error(f"[Integrität Fehler] Keine Berechtigung für Datei: {file_path}")
                    except FileNotFoundError:
                        # Sollte durch isfile() abgedeckt sein, aber zur Sicherheit
                        files_to_delete.append((file_id,))
                        logger.error(f"[FEHLT] Datei fehlt (trotz isfile): {file_path}")
                        missing_files += 1
                    except Exception as e:
                        logger.error(f"[Integrität Fehler] Unerwarteter Fehler bei Prüfung von {file_path}: {e}")

            # Lösche fehlende Dateien im Chunk (Trigger loggt)
            if files_to_delete:
                 logger.info(f"[Integrität] Entferne {len(files_to_delete)} fehlende Dateien...")
                 cursor.executemany("DELETE FROM files WHERE id = ?", files_to_delete)
                 files_to_delete.clear()

            # Aktualisiere geänderte Dateien im Chunk
            if files_to_update:
                logger.info(f"[Integrität] Aktualisiere {len(files_to_update)} geänderte Dateien...")
                cursor.executemany("UPDATE files SET size = ?, hash = ? WHERE id = ?", files_to_update)
                files_to_update.clear()

            # Commit nach jedem Chunk, um die Last zu verteilen
            db.conn.commit()
            logger.info(f"[Integrität] {checked_files} Dateien geprüft...")


        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"[OK] Integritätsprüfung abgeschlossen nach {duration:.2f} Sek.")
        logger.info(f"   Geprüft: {checked_dirs} Verzeichnisse, {checked_files} Dateien.")
        logger.info(f"   Resultat: {missing_dirs} fehlende Verz., {missing_files} fehlende Dateien, {updated_files} geänderte Dateien.")
        
        # Erweiterte Statistiken für optimierte Datenbank
        try:
            # Extension-Statistiken
            cursor.execute("""
                SELECT e.name, e.category, COUNT(f.id) as count 
                FROM extensions e 
                JOIN files f ON e.id = f.extension_id 
                GROUP BY e.id 
                HAVING count > 0
                ORDER BY count DESC 
                LIMIT 5
            """)
            ext_stats = cursor.fetchall()
            if ext_stats:
                logger.info("[Integrität] Top Extensions in Datenbank:")
                for ext, category, count in ext_stats:
                    logger.info(f"   {ext:12} ({category:10}): {count:>8,} Dateien")
        except Exception as e:
            logger.warning(f"[Integrität] Konnte erweiterte Statistiken nicht erstellen: {e}")

    except Exception as e:
        logger.critical(f"[Integrität Fehler] Kritischer Fehler während der Prüfung: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # DB nicht hier schließen, da es die Singleton-Instanz ist
        pass

def main():
    """Hauptfunktion: Ermittelt optionalen Pfad und startet die Prüfung."""
    check_path = None
    # Prüfe, ob ein Pfad als Argument übergeben wurde
    if len(sys.argv) == 2:
        path_arg = sys.argv[1]
        # Erlaube auch die Prüfung für eine nicht (mehr) existierende Basis
        check_path = os.path.normpath(os.path.abspath(path_arg))
        logger.info(f"[Integrität] Verwende Pfadfilter aus Argument: {check_path}")
    elif len(sys.argv) > 2:
        print("Verwendung: python integrity_checker.py [optional: <Pfad>]")
        sys.exit(1)
    else:
        logger.info("[Integrität] Kein Pfadfilter angegeben, prüfe gesamte Datenbank.")

    try:
        db = get_db_instance() # Holt die globale Instanz
        if not db:
             logger.critical("[Integrität Fehler] Konnte DB-Instanz nicht erhalten.")
             sys.exit(1)

        check_integrity(db, check_path)
        logger.info("[Integrität] Programm beendet.")
        sys.exit(0)

    except Exception as e:
        logger.critical(f"[Integrität Fehler] Unerwarteter Fehler im Hauptablauf: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
