# -*- coding: utf-8 -*-
import os
import sys
import json
import time
import logging

# Importiere zentrale Funktionen und Konstanten
from utils import logger, DB_PATH, CONFIG, PROJECT_DIR, calculate_hash, HASHING
from models import get_db_instance


def _emit(line):
    """Schreibt eine Zeile auf stdout und flusht sofort (fuer QProcess)."""
    print(line, flush=True)


def check_integrity(db, check_base_path=None):
    """Prueft die Integritaet der Datenbankeintraege gegen das Dateisystem.

    Args:
        db: Die DBManager-Instanz.
        check_base_path: Optionaler Basispfad. Wenn angegeben, werden nur
                         Eintraege innerhalb dieses Pfades geprueft.
    """
    start_time = time.time()
    if check_base_path:
        check_base_path = os.path.normpath(check_base_path)
        logger.info(f"[Integritaet] Starte Integritaetspruefung fuer Pfad: {check_base_path}...")
    else:
        logger.info("[Integritaet] Starte globale Integritaetspruefung...")

    # Eigener Cursor — nicht db.cursor teilen
    cursor = db.conn.cursor()
    cursor.execute("PRAGMA busy_timeout = 60000")
    cursor.execute("PRAGMA journal_mode = WAL")

    missing_dirs = 0
    missing_files = 0
    updated_files = 0
    checked_dirs = 0
    checked_files = 0

    try:
        # --- Gesamtzahlen ermitteln (fuer Fortschritt) ---
        count_dir_query = "SELECT COUNT(*) FROM directories"
        count_file_query = """
            SELECT COUNT(*) FROM files f
            JOIN directories d ON f.directory_id = d.id
        """
        count_params = []

        if check_base_path:
            path_filter = " WHERE d.full_path LIKE ? OR d.full_path = ?"
            dir_path_filter = " WHERE full_path LIKE ? OR full_path = ?"
            count_params = [check_base_path + os.sep + '%', check_base_path]
            count_dir_query += dir_path_filter
            count_file_query += path_filter

        cursor.execute(count_dir_query, count_params)
        total_dirs = cursor.fetchone()[0]

        cursor.execute(count_file_query, count_params)
        total_files = cursor.fetchone()[0]

        logger.info(f"[Integritaet] Zu pruefen: {total_dirs} Verzeichnisse, {total_files} Dateien")

        # --- Verzeichnisse pruefen ---
        _emit("@@PHASE:dirs")
        logger.info("[Integritaet] Pruefe Verzeichnisse...")
        dir_query = "SELECT id, full_path FROM directories"
        dir_params = []
        if check_base_path:
            dir_query += " WHERE full_path LIKE ? OR full_path = ?"
            dir_params.extend([check_base_path + os.sep + '%', check_base_path])

        cursor.execute(dir_query, dir_params)
        dirs_to_check = cursor.fetchall()

        dirs_to_delete = []
        for dir_id, full_path in dirs_to_check:
            checked_dirs += 1
            if checked_dirs % 100 == 0 or checked_dirs == total_dirs:
                _emit(f"@@PROGRESS:{checked_dirs}:{total_dirs}")
            # Pruefe Existenz
            if not os.path.isdir(full_path):
                dirs_to_delete.append((dir_id,))
                logger.error(f"[FEHLT] Verzeichnis fehlt: {full_path}")
                missing_dirs += 1

        if dirs_to_delete:
            logger.info(f"[Integritaet] Entferne {len(dirs_to_delete)} fehlende Verzeichnisse...")
            cursor.executemany("DELETE FROM directories WHERE id = ?", dirs_to_delete)
            db.conn.commit()

        # Abschluss-Fortschritt fuer Verzeichnisse
        _emit(f"@@PROGRESS:{checked_dirs}:{total_dirs}")

        # --- Dateien pruefen ---
        _emit("@@PHASE:files")
        logger.info("[Integritaet] Pruefe Dateien...")
        # FIX: CASE WHEN statt COALESCE — '[none]' Extension wird korrekt als '' behandelt
        file_query = """
            SELECT f.id,
                   d.full_path || '/' || f.filename || CASE WHEN e.name IS NULL OR e.name = '[none]' THEN '' ELSE e.name END as file_path,
                   f.size, f.hash
            FROM files f
            JOIN directories d ON f.directory_id = d.id
            LEFT JOIN extensions e ON f.extension_id = e.id
        """
        file_params = []

        if check_base_path:
            file_query += " WHERE d.full_path LIKE ? OR d.full_path = ?"
            file_params.extend([check_base_path + os.sep + '%', check_base_path])

        cursor.execute(file_query, file_params)
        chunk_size = 500
        files_to_delete = []
        files_to_update = []

        while True:
            files_chunk = cursor.fetchmany(chunk_size)
            if not files_chunk:
                break

            for file_id, file_path, size_old, hash_old in files_chunk:
                checked_files += 1
                if checked_files % 200 == 0 or checked_files == total_files:
                    _emit(f"@@PROGRESS:{checked_files}:{total_files}")

                # Normalisiere Pfad fuer Windows-Kompatibilitaet
                file_path = os.path.normpath(file_path.replace('/', os.sep))

                # Pruefe Existenz
                if not os.path.isfile(file_path):
                    files_to_delete.append((file_id,))
                    logger.error(f"[FEHLT] Datei fehlt: {file_path}")
                    missing_files += 1
                else:
                    try:
                        size_new = os.path.getsize(file_path)
                        hash_new = calculate_hash(file_path) if HASHING else None

                        needs_update = False
                        if size_new != size_old:
                            needs_update = True
                        elif HASHING and hash_new is not None and hash_new != hash_old:
                            needs_update = True

                        if needs_update:
                            files_to_update.append((size_new, hash_new, file_id))
                            logger.info(f"[GEAENDERT] Datei geaendert: {file_path} (Size: {size_old}->{size_new}, Hash: {(hash_old or 'N/A')[:8]}->{(hash_new or 'N/A')[:8]})")
                            updated_files += 1

                    except PermissionError:
                        logger.error(f"[Integritaet Fehler] Keine Berechtigung fuer Datei: {file_path}")
                    except FileNotFoundError:
                        files_to_delete.append((file_id,))
                        logger.error(f"[FEHLT] Datei fehlt (trotz isfile): {file_path}")
                        missing_files += 1
                    except Exception as e:
                        logger.error(f"[Integritaet Fehler] Unerwarteter Fehler bei Pruefung von {file_path}: {e}")

            # Loesche fehlende Dateien im Chunk
            if files_to_delete:
                logger.info(f"[Integritaet] Entferne {len(files_to_delete)} fehlende Dateien...")
                cursor.executemany("DELETE FROM files WHERE id = ?", files_to_delete)
                files_to_delete.clear()

            # Aktualisiere geaenderte Dateien im Chunk
            if files_to_update:
                logger.info(f"[Integritaet] Aktualisiere {len(files_to_update)} geaenderte Dateien...")
                cursor.executemany("UPDATE files SET size = ?, hash = ? WHERE id = ?", files_to_update)
                files_to_update.clear()

            # Commit nach jedem Chunk
            db.conn.commit()
            logger.info(f"[Integritaet] {checked_files} Dateien geprueft...")

        # Abschluss-Fortschritt fuer Dateien
        _emit(f"@@PROGRESS:{checked_files}:{total_files}")

        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"[OK] Integritaetspruefung abgeschlossen nach {duration:.2f} Sek.")
        logger.info(f"   Geprueft: {checked_dirs} Verzeichnisse, {checked_files} Dateien.")
        logger.info(f"   Resultat: {missing_dirs} fehlende Verz., {missing_files} fehlende Dateien, {updated_files} geaenderte Dateien.")

        # Strukturiertes Ergebnis fuer GUI
        result = {
            "checked_dirs": checked_dirs,
            "missing_dirs": missing_dirs,
            "checked_files": checked_files,
            "missing_files": missing_files,
            "updated_files": updated_files,
            "duration": round(duration, 2)
        }
        _emit(f"@@RESULT:{json.dumps(result)}")

        # Erweiterte Statistiken
        try:
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
                logger.info("[Integritaet] Top Extensions in Datenbank:")
                for ext, category, count in ext_stats:
                    logger.info(f"   {ext:12} ({category:10}): {count:>8,} Dateien")
        except Exception as e:
            logger.warning(f"[Integritaet] Konnte erweiterte Statistiken nicht erstellen: {e}")

    except Exception as e:
        logger.critical(f"[Integritaet Fehler] Kritischer Fehler waehrend der Pruefung: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        cursor.close()


def main():
    """Hauptfunktion: Ermittelt optionalen Pfad und startet die Pruefung."""
    check_path = None
    if len(sys.argv) == 2:
        path_arg = sys.argv[1]
        check_path = os.path.normpath(os.path.abspath(path_arg))
        logger.info(f"[Integritaet] Verwende Pfadfilter aus Argument: {check_path}")
    elif len(sys.argv) > 2:
        print("Verwendung: python integrity_checker.py [optional: <Pfad>]")
        sys.exit(1)
    else:
        logger.info("[Integritaet] Kein Pfadfilter angegeben, pruefe gesamte Datenbank.")

    try:
        db = get_db_instance()
        if not db:
            logger.critical("[Integritaet Fehler] Konnte DB-Instanz nicht erhalten.")
            sys.exit(1)

        check_integrity(db, check_path)
        logger.info("[Integritaet] Programm beendet.")
        sys.exit(0)

    except Exception as e:
        logger.critical(f"[Integritaet Fehler] Unerwarteter Fehler im Hauptablauf: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
