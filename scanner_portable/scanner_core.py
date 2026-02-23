import os
import sys
import time # Für mögliche Pausen
import sqlite3
from datetime import datetime
import argparse # Importieren
import logging # Hinzufügen

# Importiere zentrale Funktionen und Konstanten
from utils import calculate_hash, HASHING, CONFIG, DB_PATH, load_config, logger # logger importieren
from models import get_db_instance

# --- Entferne alte, lokale Funktionen --- 
# def load_config():
#     ...
# def save_config(base_path):
#     ...
# def connect_to_db():
#     ...
# def initialize_db(): # Wird durch DBManager.ensure_schema() ersetzt
#     ...
# def scan_and_insert_files(base_path): # Wird durch run_scan ersetzt
#    ...
# -------------------------------------

hash_dirs = [] # Wird in main geladen

def run_scan(base_path, force_restart=False):
    logger.debug(f"[Core Scan DEBUG] Entering run_scan for {base_path}, force_restart={force_restart}") # Geändert auf logger.debug
    db = None
    try:
        db = get_db_instance()
        logger.debug(f"[Core Scan DEBUG] DB instance obtained: {db}") # Geändert auf logger.debug
    except Exception as db_ex:
        logger.error(f"[Core Scan FEHLER] Failed to get DB instance: {db_ex}") # Geändert auf logger.error
        import traceback
        logger.error(traceback.format_exc()) # Geändert auf logger.error
        return False # Frühzeitiger Ausstieg bei DB-Fehler

    drive_id = None
    drive_name_for_db = os.path.splitdrive(base_path)[0] + "/" # Standardisiere auf "X:/"
    try:
        logger.debug(f"[Core Scan DEBUG] Attempting to get/create drive for '{drive_name_for_db}' (from base_path '{base_path}')") # Geändert auf logger.debug
        drive_id = db.get_or_create_drive(drive_name_for_db)
        logger.debug(f"[Core Scan DEBUG] Drive ID obtained/created: {drive_id}") # Geändert auf logger.debug
    except Exception as drive_ex:
        logger.error(f"[Core Scan FEHLER] Failed during get_or_create_drive for {drive_name_for_db}: {drive_ex}") # Geändert auf logger.error
        import traceback
        logger.error(traceback.format_exc()) # Geändert auf logger.error
        return False # Frühzeitiger Ausstieg bei Laufwerksfehler

    # Überprüfe drive_id explizit (könnte 0 oder None sein)
    if not drive_id: # Falls 0 oder None zurückgegeben wird
        logger.error(f"[Core Scan FEHLER] Konnte Laufwerk-ID nicht erstellen/abrufen für: {drive_name_for_db} (Returned: {drive_id}). Scan wird abgebrochen.") # Geändert auf logger.error
        return False

    # Ab hier sollten die normalen Logs erscheinen, wenn alles gut ging
    start_time = time.time()
    logger.info(f"[Core Scan] Starte Scan für: {base_path} (Global Hashing: {global_hashing}, Specific Hash Dirs: {hash_dirs})") # Geändert auf logger.info
    logger.info(f"[Core Scan] Verwende Laufwerk: {drive_name_for_db} (ID: {drive_id})") # Geändert auf logger.info

    # --- Logik zur Wiederaufnahme / Neustart ---
    resuming = False
    resume_dir = None
    if not force_restart: # Nur nach resume_dir suchen, wenn kein Neustart erzwungen wird
        resume_dir = db.get_last_scan_path(drive_id)
        if resume_dir:
            resume_dir = os.path.normpath(resume_dir) # Normalisieren für konsistente Vergleiche
            logger.info(f"[Core Scan] Setze Scan fort ab Verzeichnis: {resume_dir}") # Geändert auf logger.info
            resuming = True
        else:
            logger.info("[Core Scan] Starte neuen Scan (kein gültiger Fortsetzungspunkt gefunden).") # Geändert auf logger.info
            # resuming bleibt False
            resume_dir = None # Sicherstellen, dass resume_dir None ist
            # resuming = False # <-- redundant, da schon initialisiert
    else:
        logger.info("[Core Scan] Neustart erzwungen (--restart Flag). Starte Scan von vorne.") # Geändert auf logger.info
        # resuming bleibt False
        # resume_dir bleibt None
    # ----------------------------------------

    # Initialisiere Zähler und Batch-Listen außerhalb der Schleife
    file_count = 0
    dir_count = 0
    batch_size = 100 # Anzahl Dateien pro Batch-Insert
    files_batch = []
    scanned_dirs_set = set() # Zum Speichern aller gefundenen Verzeichnispfade für Cleanup
    scanned_files_in_dir_set = set() # Zum Speichern der Dateien im aktuellen Verzeichnis für Cleanup

    db.conn.execute("BEGIN") # Transaktion starten
    transaction_active = True
    try:
        # Durchlaufe das Verzeichnis
        for root, dirs, files in os.walk(base_path, topdown=True):
            current_dir = os.path.normpath(root)
            process_this_dir_and_files = True # Standardmäßig alles verarbeiten

            # ---- Logik zur Wiederaufnahme v3 ----
            if resuming and resume_dir:
                # Fall 1: Wir sind strikt VOR dem Fortsetzungspunkt
                if current_dir < resume_dir:
                    # Prüfen, ob der resume_dir überhaupt unterhalb des current_dir liegen KANN.
                    # Wenn nicht (z.B. current="N:\A", resume="N:\B"), dann können wir
                    # current_dir und dessen Unterverzeichnisse komplett überspringen.
                    if not resume_dir.startswith(current_dir.rstrip(os.sep) + os.sep):
                         logger.info(f"[Core Scan Resuming] Überspringe Baum: {current_dir} (liegt komplett vor {resume_dir})") # Geändert auf logger.info
                         dirs[:] = [] # Nicht in Unterverzeichnisse von current_dir gehen
                         process_this_dir_and_files = False # Auch keine Dateien/DB-Ops für current_dir
                    # Wenn doch (current="N:", resume="N:\B"), DANN müssen wir in die Unterverzeichnisse schauen,
                    # dürfen aber current_dir selbst noch nicht verarbeiten.
                    else:
                         logger.info(f"[Core Scan Resuming] Überspringe Verarbeitung von {current_dir} (Vorfahre von {resume_dir}), steige aber ab.") # Geändert auf logger.info
                         process_this_dir_and_files = False # Keine Dateien/DB-Ops für current_dir
                         # Optional: Performance-Optimierung -> dirs filtern, nur relevanten Pfad behalten
                         # dirs[:] = [d for d in dirs if resume_dir.startswith(os.path.normpath(os.path.join(current_dir, d)).rstrip(os.sep) + os.sep) or os.path.normpath(os.path.join(current_dir, d)) >= resume_dir]
                         # Erstmal ohne Optimierung, um Korrektheit sicherzustellen.

                # Fall 2: Wir haben den Fortsetzungspunkt erreicht oder sind dahinter
                elif current_dir >= resume_dir:
                    logger.info(f"[Core Scan] Fortsetzungspunkt erreicht/überschritten bei {current_dir}. Setze Scan normal fort.") # Geändert auf logger.info
                    resuming = False
                    resume_dir = None
                    # process_this_dir_and_files bleibt True

                # Fall 3: resume_dir wurde währenddessen None (sollte nicht passieren)
                elif not resume_dir:
                    logger.warning("[Core Scan Warnung] Fortsetzungspunkt ungültig geworden, setze normal fort.") # Geändert auf logger.warning
                    resuming = False
                    # process_this_dir_and_files bleibt True
            # ------------------------------------

            # Nur verarbeiten, wenn nicht wegen Wiederaufnahme übersprungen
            if process_this_dir_and_files:
                # --- Verzeichnis-Verarbeitung ---
                scanned_dirs_set.add(current_dir)
                dir_count += 1
                # Verzeichnis in DB eintragen/holen
                dir_id = db.get_or_create_directory(drive_id, current_dir)

                if not dir_id:
                    logger.warning(f"[Core Scan Warnung] Konnte Verzeichnis nicht verarbeiten, überspringe: {current_dir}") # Geändert auf logger.warning
                    dirs[:] = [] # Nicht weiter in dieses fehlerhafte Verzeichnis absteigen
                    continue # Zum nächsten Eintrag in os.walk

                # --- Datei-Verarbeitung ---
                scanned_files_in_dir_set.clear()
                files_batch.clear()

                for file in files:
                    full_path = os.path.normpath(os.path.join(current_dir, file))
                    scanned_files_in_dir_set.add(full_path)
                    try:
                        # Prüfe Zugriffsrechte und ob es eine Datei ist
                        if not os.access(full_path, os.R_OK) or not os.path.isfile(full_path):
                           continue

                        size = os.path.getsize(full_path)
                        
                        # ---- Neue Hashing-Logik ----
                        should_hash = False
                        if global_hashing:
                            should_hash = True
                        else:
                            for hash_dir_path in hash_dirs:
                                if current_dir.startswith(hash_dir_path):
                                    should_hash = True
                                    break
                        
                        hash_val = None
                        if should_hash:
                            hash_val = calculate_hash(full_path)
                            if hash_val is None:
                                 logger.warning(f"[Core Scan Warnung] Konnte Hash nicht berechnen für: {full_path}") # Geändert auf logger.warning
                        # ----------------------------

                        files_batch.append((dir_id, full_path, size, hash_val))
                        file_count += 1 # Zähler hier erhöhen

                    except PermissionError:
                        logger.error(f"[Core Scan Fehler] Keine Berechtigung für: {full_path}") # Geändert auf logger.error
                    except FileNotFoundError:
                        logger.error(f"[Core Scan Fehler] Datei nicht gefunden (sollte nicht passieren): {full_path}") # Geändert auf logger.error
                    except OSError as e:
                        logger.error(f"[Core Scan Fehler] OS-Fehler bei {full_path}: {e}") # Geändert auf logger.error
                    except Exception as e:
                        logger.error(f"[Core Scan Fehler] Unerwarteter Fehler bei {full_path}: {e}") # Geändert auf logger.error

                # Verarbeite den Batch für das aktuelle Verzeichnis
                if files_batch:
                    db.batch_insert_files(files_batch)

                # Fortschritt loggen (jetzt alle 1000 Verzeichnisse)
                if dir_count % 1000 == 0:
                    logger.info(f"[Core Scan] Fortschritt: {dir_count} Verzeichnisse und {file_count} Dateien gescannt...")

                # --- Update Scan Progress regelmäßig (jetzt alle 1000 Verzeichnisse) --- 
                if dir_count % 1000 == 0:
                     try:
                         db.update_scan_progress(drive_id, current_dir)
                     except Exception as e:
                         logger.warning(f"[Core Scan Warnung] Fehler beim Speichern des Fortschritts: {e}") # Geändert auf logger.warning
            # else: # Debugging, falls gewünscht
            #    logger.debug(f"[Core Scan Resuming] Verarbeitung übersprungen für {current_dir}") # Geändert auf logger.debug

        # Nach dem gesamten Walk (nur wenn keine Exception auftrat):
        logger.info("[Core Scan] os.walk beendet. Bereite Commit der Haupt-Transaktion vor...") # Geändert auf logger.info
        logger.info(f"[DB Commit] Committing main transaction (scanner_core.py)") # Geändert auf logger.info
        db.conn.commit() # Alle Änderungen speichern
        transaction_active = False
        # Log über gefundene Dateien/Verzeichnisse NACH erfolgreichem Commit
        logger.info(f"[Core Scan] {dir_count} Verzeichnisse und {file_count} Dateien verarbeitet und committet.") # Geändert auf logger.info

        # Bereinige veraltete Verzeichnisse für das gescannte Laufwerk
        # db.cleanup_removed_dirs(drive_id, scanned_dirs_set)
        # TODO: Entscheiden, ob Cleanup hier oder im Integrity Check erfolgen soll.
        # Aktuell: Cleanup nur im Integrity Check.

        # Scan-Fortschritt löschen (Signal für Abschluss)
        try:
            logger.debug(f"[Core Scan DEBUG] Versuche Abschluss-Fortschritt zu speichern (last_path=None) für drive_id {drive_id}") # Geändert auf logger.debug
            db.update_scan_progress(drive_id, None) # Ruft Methode in models.py auf, die selbst committet
            # Das folgende Commit ist technisch doppelt, da update_scan_progress schon committet.
            # Entfernen wir es hier, um Log-Konsistenz zu wahren.
            # write_log(f"[DB Commit] Committing final progress update (scanner_core.py, last_path=None)") # Log vor Commit (Entfernt)
            # db.conn.commit() # Explizites Commit für den Abschluss-Fortschritt (Entfernt)
            logger.info(f"[Core Scan] Abschluss-Fortschritt für Laufwerk ID {drive_id} erfolgreich gespeichert (Scan beendet).") # Geändert auf logger.info
        except Exception as final_update_error:
             logger.error(f"[Core Scan FEHLER] Kritischer Fehler beim Speichern des Abschluss-Fortschritts: {final_update_error}") # Geändert auf logger.error
             import traceback
             logger.error(traceback.format_exc()) # Geändert auf logger.error
             # Hier signalisieren wir jetzt einen Fehler, auch wenn der Scan selbst ok war.
             return False

    except Exception as e: # Haupt-Scan-Fehler
        logger.error(f"[Core Scan Fehler] Kritischer Fehler während des Scans für {base_path}: {e}") # Geändert auf logger.error
        # Optional: Stacktrace loggen
        import traceback
        logger.error(traceback.format_exc()) # Geändert auf logger.error
        if transaction_active:
            logger.warning("[Core Scan] Rollback Transaktion aufgrund eines Fehlers.") # Geändert auf logger.warning
            db.conn.rollback() # Änderungen verwerfen
        return False # Fehler signalisieren
    # 'finally' wird hier nicht benötigt, da commit/rollback im try/except behandelt wird.

    # Erfolgreicher Abschluss (nur wenn kein Fehler beim letzten Commit auftrat)
    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"[Core Scan] Scan für {base_path} abgeschlossen. Dauer: {duration:.2f} Sekunden.") # Geändert auf logger.info
    return True # Nur wenn alles bis hierhin geklappt hat

def main():
    """Hauptfunktion: Verarbeitet Argumente und startet den Scan."""
    # Lade Konfiguration früh, um Logging-Level etc. zu haben (obwohl Logging hier schon aktiv ist)
    # global CONFIG # Wird jetzt in main() geladen und an run_scan übergeben (implizit über globale Variablen)
    # CONFIG = load_config()
    # setup_logging(CONFIG.get('log_level', 'INFO')) # Logging wird schon vorher initialisiert

    # --- Argument Parser Setup ---
    parser = argparse.ArgumentParser(description="Dateiscanner Core-Modul.")
    parser.add_argument("path", help="Der zu scannende Basispfad (Laufwerk oder Verzeichnis).")
    parser.add_argument("--restart", action="store_true",
                        help="Erzwingt einen Neustart des Scans von Anfang an, ignoriert gespeicherten Fortschritt.")
    args = parser.parse_args()
    # ---------------------------

    scan_path = None

    # 1. Prüfe Kommandozeilenargument 'path'
    path_arg = args.path
    # if len(sys.argv) > 1:
    #     path_arg = sys.argv[1]
    if os.path.isdir(path_arg):
        scan_path = os.path.normpath(path_arg)
        logger.info(f"[Core Scan] Verwende Pfad aus Kommandozeilenargument: {scan_path}") # Geändert auf logger.info
    else:
        logger.error(f"[Core Scan Fehler] Ungültiger Pfad als Kommandozeilenargument übergeben: {path_arg}") # Geändert auf logger.error
        sys.exit(1)

    # 2. Wenn kein gültiges Argument, prüfe Konfiguration - ENTFÄLLT, Pfad ist jetzt Pflichtargument
    # if not scan_path:
    #     config_path = CONFIG.get('base_path')
    #     if config_path and os.path.isdir(config_path):
    #         scan_path = os.path.normpath(config_path)
    #         write_log(f"[Core Scan] Verwende Pfad aus Konfigurationsdatei: {scan_path}")
    #     else:
    #         write_log("[Core Scan Fehler] Kein gültiger Scan-Pfad angegeben (weder als Argument noch in config.json).")
    #         print("Fehler: Bitte gib einen gültigen Pfad als Argument an oder setze 'base_path' in config.json.")
    #         sys.exit(1)

    # Lade globale Konfiguration (wird für Hashing in run_scan benötigt)
    global CONFIG, global_hashing, hash_dirs
    CONFIG = load_config() # Lade die aktuelle Konfiguration
    global_hashing = CONFIG.get('hashing', False)
    hash_dirs = CONFIG.get('hash_directories', [])

    # Starte den Scan mit dem force_restart Flag aus den Argumenten
    success = run_scan(scan_path, force_restart=args.restart)

    if success:
        logger.info("[Core Scan] Programm erfolgreich beendet.") # Geändert auf logger.info
        sys.exit(0)
    else:
        logger.error("[Core Scan] Programm mit Fehlern beendet.") # Geändert auf logger.error
        sys.exit(1)

if __name__ == "__main__":
    main()
