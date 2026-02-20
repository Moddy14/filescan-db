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
    drive_id = None
    drive_name_for_db = os.path.splitdrive(base_path)[0] + "/" # Standardisiere auf "X:/"
    
    # Load global hashing config if not already loaded
    global global_hashing, hash_dirs
    if 'global_hashing' not in globals():
        global_hashing = CONFIG.get('hashing', False)
        hash_dirs = CONFIG.get('hash_directories', [])
    
    # Problematische Windows-Ordner, die übersprungen werden sollten
    # Diese enthalten extrem viele kleine Dateien und verlangsamen den Scan massiv
    SKIP_PATHS = [
        r"C:\Windows\servicing\LCU",  # Windows Update Ordner mit Tausenden kleinen Dateien
        r"C:\Windows\servicing\Packages",  # Windows Package Cache
        r"C:\Windows\WinSxS\Backup",  # Windows Component Store Backup
        r"C:\Windows\Installer\$PatchCache$",  # Windows Installer Cache
        r"C:\Windows\SoftwareDistribution\Download",  # Windows Update Downloads
        r"C:\Windows\Temp",  # Temporäre Dateien
        r"C:\$Recycle.Bin",  # Papierkorb
        r"C:\System Volume Information",  # System Volume Information
        r"C:\Windows\CSC",  # Offline Files Cache
        r"C:\ProgramData\Microsoft\Windows\WER",  # Windows Error Reporting
        r"C:\Windows\Logs\CBS",  # Component Based Servicing Logs
    ]

    try:
        db = get_db_instance()
        logger.debug(f"[Core Scan DEBUG] DB instance obtained: {db}") # Geändert auf logger.debug

        # --- Laufwerksspezifische Scan-Logik ---
        # NIEMALS automatisch löschen! Nur fortsetzen oder hinzufügen
        db.cursor.execute("SELECT id FROM drives WHERE name = ?", (drive_name_for_db,))
        drive_exists = db.cursor.fetchone()
        temp_drive_id = drive_exists[0] if drive_exists else None
        
        if not force_restart and temp_drive_id:
            # Normaler Scan: Prüfe Fortsetzungspunkt
            resume_dir = db.get_last_scan_path(temp_drive_id)
            if resume_dir:
                logger.info(f"[Core Scan] Fortsetzung für '{drive_name_for_db}' ab: {resume_dir}")
            else:
                logger.info(f"[Core Scan] Neuer Scan für '{drive_name_for_db}' - füge zu vorhandenen Daten hinzu")
                # KEINE LÖSCHUNG! Scan fügt nur neue Dateien hinzu oder aktualisiert vorhandene
        elif force_restart:
            logger.info(f"[Core Scan] Restart für '{drive_name_for_db}' - überschreibt/aktualisiert vorhandene Daten")
        else:
            logger.info(f"[Core Scan] Neues Laufwerk '{drive_name_for_db}' wird erstellt")
        # --- Ende Laufwerks-Logik ---

        # Hole/Erstelle jetzt die drive_id für den aktuellen Scan (neu, wenn gelöscht wurde)
        drive_id = db.get_or_create_drive(drive_name_for_db) 
        if not drive_id: 
             logger.error(f"[Core Scan FEHLER] Konnte Laufwerk-ID nicht erstellen/abrufen für: {drive_name_for_db}. Scan wird abgebrochen.")
             return False
        logger.debug(f"[Core Scan DEBUG] Verwende drive_id: {drive_id}") 

    except Exception as db_ex:
        logger.error(f"[Core Scan FEHLER] Failed to get DB instance or handle drive: {db_ex}") 
        import traceback
        logger.error(traceback.format_exc()) 
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
        # WICHTIG: Lösche NUR die Daten des spezifischen Laufwerks!
        if drive_id:
            logger.info(f"[Core Scan] Lösche alte Daten für Laufwerk {drive_name_for_db} (ID: {drive_id})")
            if db.clear_drive_data(drive_id):
                logger.info(f"[Core Scan] Alte Daten für Laufwerk {drive_name_for_db} erfolgreich gelöscht")
            else:
                logger.error(f"[Core Scan] Fehler beim Löschen der alten Daten für Laufwerk {drive_name_for_db}")
                # Scan trotzdem fortsetzen, da die Daten möglicherweise überschrieben werden können
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

    # Häufiger kleinere Transaktionen durchführen anstatt einer großen
    transaction_active = False
    commit_interval = 250  # Nach x Verzeichnissen committen
    last_commit_time = time.time()
    max_transaction_time = 60.0  # Maximale Zeit einer Transaktion in Sekunden
    
    try:
        # Durchlaufe das Verzeichnis
        for root, dirs, files in os.walk(base_path, topdown=True):
            current_dir = os.path.normpath(root)
            process_this_dir_and_files = True # Standardmäßig alles verarbeiten
            
            # ---- Überspringe problematische Windows-Ordner ----
            skip_this_dir = False
            for skip_path in SKIP_PATHS:
                skip_path_norm = os.path.normpath(skip_path)
                # Prüfe ob current_dir mit einem der Skip-Pfade beginnt
                if current_dir.lower().startswith(skip_path_norm.lower()):
                    logger.info(f"[Core Scan] Überspringe problematischen Ordner: {current_dir}")
                    dirs[:] = []  # Verhindere Abstieg in Unterverzeichnisse
                    process_this_dir_and_files = False
                    skip_this_dir = True
                    break
            
            if skip_this_dir:
                continue  # Zum nächsten Verzeichnis

            # Starte eine neue Transaktion, wenn keine aktiv ist
            if not transaction_active:
                db.conn.execute("BEGIN")
                transaction_active = True
                last_commit_time = time.time()

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

                        # Für optimierte DB-Struktur: nur Dateiname (basename) verwenden
                        files_batch.append((dir_id, file, size, hash_val))
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

                # Fortschritt loggen (jetzt alle 1000 Verzeichnisse) und immer anzeigen (Level WARNING)
                if dir_count % 1000 == 0:
                    logger.warning(f"[Core Scan] Fortschritt: {dir_count} Verzeichnisse und {file_count} Dateien gescannt...") # Geändert auf WARNING

                # --- Update Scan Progress regelmäßig (jetzt alle 1000 Verzeichnisse) --- 
                if dir_count % 1000 == 0:
                     try:
                         # Committe vorher, um Fehler zu vermeiden
                         if transaction_active:
                             logger.info(f"[DB Commit] Committing progress transaction at {dir_count} directories")
                             db.conn.commit()
                             transaction_active = False
                             
                         db.update_scan_progress(drive_id, current_dir)
                     except Exception as e:
                         logger.warning(f"[Core Scan Warnung] Fehler beim Speichern des Fortschritts: {e}") # Geändert auf logger.warning
            # else: # Debugging, falls gewünscht
            #    logger.debug(f"[Core Scan Resuming] Verarbeitung übersprungen für {current_dir}") # Geändert auf logger.debug
            
            # Überprüfen, ob wir die aktuelle Transaktion committen sollten
            current_time = time.time()
            if transaction_active and (
                dir_count % commit_interval == 0 or  # Entweder basierend auf Anzahl der Verzeichnisse
                current_time - last_commit_time > max_transaction_time  # Oder basierend auf Zeit
            ):
                logger.info(f"[DB Commit] Committing intermediate transaction (dirs: {dir_count}, time: {current_time - last_commit_time:.1f}s)")
                db.conn.commit()
                transaction_active = False

        # Nach dem gesamten Walk (nur wenn keine Exception auftrat):
        logger.info("[Core Scan] os.walk beendet. Bereite Commit der Haupt-Transaktion vor...") # Geändert auf logger.info
        if transaction_active:
            logger.info(f"[DB Commit] Committing final walk transaction (scanner_core.py)") # Geändert auf logger.info
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

    # Erfolgreicher Abschluss (nur wenn kein Fehler beim letzten Commit auftrat)
    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"[Core Scan] ✅ Scan für {base_path} erfolgreich abgeschlossen!")
    logger.info(f"[Core Scan] Ergebnisse: {dir_count} Verzeichnisse, {file_count} Dateien in {duration:.2f}s")
    
    # Erweiterte Statistiken für optimierte DB
    try:
        # Extension-Statistiken für dieses Laufwerk
        db.cursor.execute("""
            SELECT e.name, e.category, COUNT(f.id) as count 
            FROM extensions e 
            JOIN files f ON e.id = f.extension_id 
            JOIN directories d ON f.directory_id = d.id 
            WHERE d.drive_id = ?
            GROUP BY e.id 
            HAVING count > 0
            ORDER BY count DESC 
            LIMIT 10
        """, (drive_id,))
        
        ext_stats = db.cursor.fetchall()
        if ext_stats:
            logger.info("[Core Scan] Top Extensions auf diesem Laufwerk:")
            for ext, category, count in ext_stats:
                logger.info(f"  {ext:12} ({category:10}): {count:>8,} Dateien")
        
        # Performance-Statistiken
        files_per_second = file_count / duration if duration > 0 else 0
        logger.info(f"[Core Scan] Performance: {files_per_second:.0f} Dateien/s, {dir_count/duration:.0f} Verzeichnisse/s")
                
    except Exception as e:
        logger.warning(f"[Core Scan] Konnte erweiterte Statistiken nicht erstellen: {e}")
    
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
    parser.add_argument("--scheduled", action="store_true",
                        help="Kennzeichnet, dass der Scan als geplanter Scan läuft.")
    parser.add_argument("--force", action="store_true",
                        help="Erzwingt den Start des Scans, auch wenn bereits ein anderer Scan läuft.")
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

    # DB-Instanz holen
    db = get_db_instance()
    
    # Interaktive Abfrage für Scan-Modus (nur wenn keine Argumente und interaktive Konsole)
    if not args.restart and not args.scheduled and sys.stdout.isatty():
        # Prüfe ob für dieses Laufwerk Daten existieren
        drive_name = os.path.splitdrive(scan_path)[0] + "/"
        db.cursor.execute("SELECT id FROM drives WHERE name = ?", (drive_name,))
        drive_exists = db.cursor.fetchone()
        
        if drive_exists:
            drive_id = drive_exists[0]
            # Prüfe ob Fortsetzungspunkt existiert
            resume_point = db.get_last_scan_path(drive_id)
            
            # Zähle vorhandene Daten
            db.cursor.execute("SELECT COUNT(*) FROM files f JOIN directories d ON f.directory_id = d.id WHERE d.drive_id = ?", (drive_id,))
            file_count = db.cursor.fetchone()[0]
            db.cursor.execute("SELECT COUNT(*) FROM directories WHERE drive_id = ?", (drive_id,))
            dir_count = db.cursor.fetchone()[0]
            
            if file_count > 0 or dir_count > 0:
                # Bei geplanten Scans keine interaktive Abfrage
                if args.scheduled:
                    if resume_point and not args.restart:
                        logger.info(f"[Scheduled Scan] Fortsetzen ab: {resume_point}")
                        print(f"[Scheduled Scan] Setze Scan für {drive_name} ab letztem Punkt fort...")
                    elif not resume_point and not args.restart:
                        # Ohne Fortsetzungspunkt und ohne --restart würde gelöscht werden
                        # Bei geplanten Scans soll automatisch mit --restart fortgefahren werden
                        logger.warning(f"[Scheduled Scan] Kein Fortsetzungspunkt vorhanden. Verwende automatisch --restart um Datenverlust zu vermeiden.")
                        print(f"[Scheduled Scan] Kein Fortsetzungspunkt - verwende --restart um Daten zu erhalten")
                        args.restart = True
                    else:
                        logger.info(f"[Scheduled Scan] Starte Scan für {drive_name} mit --restart")
                        print(f"[Scheduled Scan] Starte Scan für {drive_name} neu mit --restart...")
                else:
                    # Interaktiver Modus - Original-Code
                    print(f"\n╔══════════════════════════════════════════════════════════════╗")
                    print(f"║          SCAN-MODUS AUSWAHL für {drive_name:<28} ║")
                    print(f"╠══════════════════════════════════════════════════════════════╣")
                    print(f"║ Vorhandene Daten: {file_count:,} Dateien, {dir_count:,} Verzeichnisse{' '*10} ║")
                    
                    if resume_point:
                        print(f"║ Letzter Scan-Punkt: ...{resume_point[-35:]:>35} ║")
                        print(f"╠══════════════════════════════════════════════════════════════╣")
                        print(f"║ Optionen:                                                    ║")
                        print(f"║                                                              ║")
                        print(f"║ [1] FORTSETZEN (Standard)                                   ║")
                        print(f"║     → Setzt den Scan ab dem letzten Punkt fort              ║")
                        print(f"║     → Behält alle vorhandenen Daten                         ║")
                        print(f"║     → Schneller, da nur neue Bereiche gescannt werden       ║")
                        print(f"║                                                              ║")
                        print(f"║ [2] NEU STARTEN (--restart)                                 ║")
                        print(f"║     → Startet den Scan komplett von vorne                   ║")
                        print(f"║     → Überschreibt/aktualisiert bestehende Einträge         ║")
                        print(f"║     → Dauert länger, scannt alles erneut                    ║")
                    else:
                        print(f"║ KEIN Fortsetzungspunkt vorhanden!                           ║")
                        print(f"╠══════════════════════════════════════════════════════════════╣")
                        print(f"║ ⚠️  WARNUNG: Ohne Fortsetzungspunkt würden die Daten        ║")
                        print(f"║    automatisch GELÖSCHT werden!                             ║")
                        print(f"║                                                              ║")
                        print(f"║ Optionen:                                                    ║")
                        print(f"║                                                              ║")
                        print(f"║ [1] NEUER SCAN (Standard)                                   ║")
                        print(f"║     → LÖSCHT {file_count:,} Dateien und {dir_count:,} Verzeichnisse auf {drive_name}{' '*(8-len(str(file_count))-len(str(dir_count))-len(drive_name))} ║")
                        print(f"║     → NUR dieses Laufwerk, andere bleiben unverändert       ║")
                        print(f"║                                                              ║")
                        print(f"║ [2] NEU STARTEN OHNE LÖSCHEN (--restart)                    ║")
                        print(f"║     → Behält alle vorhandenen Daten                         ║")
                        print(f"║     → Überschreibt/aktualisiert bestehende Einträge         ║")
                        print(f"║     → Scannt alles erneut von Anfang an                     ║")
                        print(f"║                                                              ║")
                        print(f"║ [3] ABBRECHEN                                                ║")
                        print(f"║     → Beendet ohne Änderungen                               ║")
                    
                    print(f"╚══════════════════════════════════════════════════════════════╝")
                    
                    if resume_point:
                        choice = input("\nIhre Wahl [1-2, Enter für 1]: ").strip()
                        if choice == "" or choice == "1":
                            # Fortsetzen - nichts tun, args.restart bleibt False
                            print("→ Setze Scan fort...")
                        elif choice == "2":
                            args.restart = True
                            print("→ Starte Scan neu mit --restart...")
                        else:
                            print("Ungültige Eingabe. Scan wird abgebrochen.")
                            sys.exit(0)
                    else:
                        choice = input("\nIhre Wahl [1-3, Enter für 1]: ").strip()
                        if choice == "" or choice == "1":
                            # Neuer Scan mit Löschen - Sicherheitsabfrage
                            print(f"\n⚠️  Sie sind dabei, {file_count:,} Dateien und {dir_count:,} Verzeichnisse")
                            print(f"NUR von Laufwerk {drive_name} zu LÖSCHEN!")
                            print(f"Andere Laufwerke bleiben unverändert.")
                            confirm = input("Sind Sie sicher? Tippen Sie 'JA LÖSCHEN' zur Bestätigung: ")
                            if confirm != "JA LÖSCHEN":
                                print("Scan abgebrochen. Keine Daten wurden gelöscht.")
                                sys.exit(0)
                            # args.restart bleibt False, Löschung erfolgt automatisch
                            print("→ Lösche alte Daten und starte neuen Scan...")
                        elif choice == "2":
                            args.restart = True
                            print("→ Starte Scan neu mit --restart (ohne Löschen)...")
                        elif choice == "3":
                            print("Scan abgebrochen.")
                            sys.exit(0)
                        else:
                            print("Ungültige Eingabe. Scan wird abgebrochen.")
                            sys.exit(0)

    # Prüfe, ob bereits ein Scan läuft und erwerbe einen Lock
    scan_type = "scheduled" if args.scheduled else "manual"
    
    # Wenn --force angegeben wurde, prüfen wir nicht auf laufende Scans
    if not args.force:
        if db.is_scan_running():
            logger.error(f"[Core Scan] Ein anderer Scan läuft bereits. Dieser Scan wird abgebrochen. Verwende --force, um den Scan trotzdem zu starten.")
            sys.exit(2)
    
    # Erwerbe Lock (auch wenn --force verwendet wird, damit andere Scans den aktiven Scan sehen)
    lock_id = db.acquire_scan_lock(scan_type=scan_type)
    if not lock_id and not args.force:
        logger.error(f"[Core Scan] Konnte keinen Scan-Lock erwerben. Möglicherweise läuft ein anderer Scan. Verwende --force, um den Scan trotzdem zu starten.")
        sys.exit(3)
    
    # Scan-Ausführung in try-finally Block, damit der Lock auf jeden Fall freigegeben wird
    try:
        # Starte den Scan mit dem force_restart Flag aus den Argumenten
        success = run_scan(scan_path, force_restart=args.restart)

        if success:
            logger.info("[Core Scan] Programm erfolgreich beendet.") # Geändert auf logger.info
            exit_code = 0
        else:
            logger.error("[Core Scan] Programm mit Fehlern beendet.") # Geändert auf logger.error
            exit_code = 1
    finally:
        # Lock freigeben, auch wenn ein Fehler auftrat
        if lock_id:
            db.release_scan_lock(lock_id)
            logger.info(f"[Core Scan] Scan-Lock {lock_id} freigegeben.")
    
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
