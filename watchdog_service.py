# Entferne Debug-Datei-Logik
# DEBUG_FILE = r"C:\TempServiceTest\watchdog_startup_debug.txt"
# try:
#     with open(DEBUG_FILE, "w") as f: # 'w' zum Überschreiben bei jedem Start
#         f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Script started.\n")
# except Exception as early_e:
#     # Wenn selbst das scheitert, können wir nichts mehr tun.
#     pass # Ignoriere Fehler hier, da kein Logging verfügbar ist

import os
import time
import sys
import threading
import logging # Behalten für Typ-Hints etc., aber nicht mehr konfigurieren

# Entferne Debug-Kommentare

try:
    from watchdog.observers import Observer
    # Entferne Debug-Kommentare
except ImportError:
    # Entferne Debug-Kommentare
    sys.exit("Fehler: Watchdog-Bibliothek nicht gefunden. Bitte installieren: pip install watchdog")

# --- Logging Konfiguration (bereits auskommentiert) --- 


# --- Importiere eigene Module ---
try:
    from watchdog_monitor import FSHandler
    # Entferne Debug-Kommentare
    from models import get_db_instance
    # Entferne Debug-Kommentare
    from utils import CONFIG, get_available_drives, setup_logging
    # Eigene Log-Datei für den Service — verhindert Rotation-Konflikt mit scanner.log
    logger = setup_logging(log_filename="watchdog_service.log",
                           level_str=CONFIG.get('log_level', 'INFO'),
                           logger_name="WatchdogService")
    # Entferne Debug-Kommentare
except ImportError as import_err:
    # Entferne Debug-Kommentare
    try:
        logger.error(f"Kritischer Importfehler: {import_err}. Service kann nicht starten.")
    except NameError:
        print(f"Kritischer Importfehler: {import_err}. Service kann nicht starten.", file=sys.stderr)
    sys.exit(1)
except Exception as general_ex: # Fange auch andere Fehler beim Import ab
    # Entferne Debug-Kommentare
    try:
        logger.error(f"Unerwarteter Fehler beim Modulimport: {general_ex}. Service kann nicht starten.")
    except NameError:
        print(f"Unerwarteter Fehler beim Modulimport: {general_ex}. Service kann nicht starten.", file=sys.stderr)
    sys.exit(1)
# --- Ende Importe ---


# --- Globale Variablen ---
observer = None
stop_event = threading.Event()


# --- Funktionen ---
def start_monitoring():
    """Startet die Überwachung für die konfigurierten Pfade oder alle Laufwerke."""
    global observer
    observer = Observer()
    paths_to_watch = []

    # NEU: Nur kanonische Laufwerke überwachen (ohne Aliases wie T:\)
    try:
        from drive_alias_detector import get_canonical_drive_list
        available_drives = get_canonical_drive_list()
        if available_drives:
            logger.info(f"Überwache kanonische Laufwerke (ohne Aliases): {', '.join(available_drives)}")
            paths_to_watch.extend(available_drives)
        else:
            logger.warning("Keine kanonischen Laufwerke gefunden.")
    except ImportError:
        # Fallback wenn drive_alias_detector nicht verfügbar
        available_drives = get_available_drives()
        if available_drives:
            logger.info(f"Fallback - Überwache alle Laufwerke: {', '.join(available_drives)}")
            paths_to_watch.extend(available_drives)
    except Exception as e:
        logger.error(f"Fehler beim Ermitteln der Laufwerke: {e}")

    # Extra-Pfade für Watchdog aus config.json (z.B. U:\ für WSL)
    # T:\ wird hier NICHT hinzugefügt – C:\ deckt T:\ real-time ab (SUBST-Alias)
    try:
        from utils import load_config
        extra_watch = load_config().get('watchdog_extra_paths', [])
        for ep in extra_watch:
            ep_norm = os.path.normpath(ep)
            if os.path.isdir(ep_norm) and ep_norm not in paths_to_watch:
                paths_to_watch.append(ep_norm)
                logger.info(f"Extra-Watchdog-Pfad aus Config: {ep_norm}")
            elif not os.path.isdir(ep_norm):
                logger.warning(f"Extra-Watchdog-Pfad nicht verfügbar: {ep_norm}")
    except Exception as e:
        logger.warning(f"Fehler beim Laden der watchdog_extra_paths: {e}")

    if not paths_to_watch:
        logger.error("Keine Pfade zum Überwachen gefunden (weder automatisch noch in config). Der Dienst startet, aber überwacht nichts.")
        # Observer nicht starten, wenn nichts zu überwachen ist? Oder leer starten? Leer starten ist ok.
        # return False # Signalisiert, dass nichts gestartet wurde

    scheduled_count = 0
    scheduled_paths = []
    failed_paths = []
    skipped_paths = []
    for path in set(paths_to_watch): # set() um Duplikate zu vermeiden
        path = os.path.normpath(path)
        if os.path.isdir(path):
            try:
                event_handler = FSHandler(path) # Handler für jeden Pfad separat
                observer.schedule(event_handler, path, recursive=True)
                logger.info(f"Überwachung für '{path}' gestartet.")
                logger.warning(f"[Watchdog Startup] OK: '{path}' wird überwacht.")  # WARNING damit es sicher geloggt wird
                scheduled_count += 1
                scheduled_paths.append(path)
            except Exception as e:
                logger.error(f"Fehler beim Starten der Überwachung für '{path}': {e}")
                failed_paths.append(f"{path}: {e}")
        else:
            logger.warning(f"Pfad '{path}' ist kein gültiges Verzeichnis und wird ignoriert.")
            skipped_paths.append(path)

    # Startup-Report in eigene Datei schreiben (garantiert lesbar)
    try:
        startup_log = os.path.join(os.path.dirname(os.path.abspath(__file__)), "watchdog_startup_report.txt")
        with open(startup_log, "w", encoding="utf-8") as f:
            f.write(f"=== Watchdog Startup Report ===\n")
            f.write(f"Zeitpunkt: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"PID: {os.getpid()}\n")
            f.write(f"User: {os.environ.get('USERNAME', 'UNKNOWN')}\n\n")
            f.write(f"Geplante Pfade: {paths_to_watch}\n\n")
            f.write(f"Erfolgreich geschedult ({scheduled_count}):\n")
            for p in scheduled_paths:
                f.write(f"  OK: {p}\n")
            if failed_paths:
                f.write(f"\nFehlgeschlagen ({len(failed_paths)}):\n")
                for p in failed_paths:
                    f.write(f"  FAIL: {p}\n")
            if skipped_paths:
                f.write(f"\nÜbersprungen ({len(skipped_paths)}):\n")
                for p in skipped_paths:
                    f.write(f"  SKIP: {p}\n")
            f.write(f"\n=== Ende Report ===\n")
        logger.warning(f"[Watchdog Startup] Report geschrieben: {startup_log}")
    except Exception as e:
        logger.error(f"[Watchdog Startup] Konnte Report nicht schreiben: {e}")

    if scheduled_count == 0:
         logger.warning("Keine gültigen Überwachungen konnten gestartet werden.")
         # return False # Signalisiert, dass nichts gestartet wurde

    try:
        logger.info("Versuche Observer zu starten...")
        observer.start()
        # Längere Pause nach Start — Windows braucht Zeit für ReadDirectoryChangesW Handles
        time.sleep(3)
        if observer.is_alive():
            logger.info("Observer erfolgreich gestartet und aktiv.")
            logger.warning(f"[Watchdog Startup] Observer läuft. {scheduled_count} Laufwerke überwacht.")
            
            # Health-Check: Teste ob Events wirklich ankommen
            def _health_check():
                time.sleep(5)  # Warte bis Observer stabil
                test_results = {}
                for path in scheduled_paths:
                    test_file = os.path.join(path, ".watchdog_health_check.tmp")
                    try:
                        with open(test_file, "w") as f:
                            f.write("health_check")
                        time.sleep(0.5)
                        os.remove(test_file)
                        test_results[path] = "OK"
                    except Exception as e:
                        test_results[path] = f"FEHLER: {e}"
                        logger.warning(f"[Watchdog Health] Kann auf {path} nicht schreiben: {e}")
                
                # Report aktualisieren
                try:
                    startup_log = os.path.join(os.path.dirname(os.path.abspath(__file__)), "watchdog_startup_report.txt")
                    with open(startup_log, "a", encoding="utf-8") as f:
                        f.write(f"\n=== Health Check ({time.strftime('%H:%M:%S')}) ===\n")
                        for path, result in test_results.items():
                            f.write(f"  {path} -> {result}\n")
                except:
                    pass
            
            health_thread = threading.Thread(target=_health_check, daemon=True)
            health_thread.start()
            
            return True
        else:
            logger.error("Observer-Thread konnte nicht gestartet werden oder ist sofort wieder beendet.")
            return False
    except Exception as e:
        logger.error(f"Schwerwiegender Fehler beim Starten des Observers: {e}")
        return False

def stop_monitoring():
    """Stoppt die Überwachung."""
    global observer
    if observer and observer.is_alive():
        logger.info("Stoppe Observer...")
        observer.stop()
        observer.join() # Warten, bis der Observer-Thread beendet ist
        logger.info("Observer gestoppt.")
    else:
        logger.info("Kein aktiver Observer zum Stoppen gefunden.")

# --- Hauptlogik ---
def start_scheduled_scanner():
    """Startet den Scheduled Scanner als Hintergrund-Thread."""
    try:
        from scheduled_scanner import run_scheduler_loop
        scheduler_thread = threading.Thread(
            target=run_scheduler_loop,
            kwargs={'interval': 30, 'stop_event': stop_event},
            daemon=True,
            name="ScheduledScanner"
        )
        scheduler_thread.start()
        logger.info("[Watchdog Service] Scheduled Scanner als Hintergrund-Thread gestartet.")
        return scheduler_thread
    except ImportError:
        logger.warning("[Watchdog Service] scheduled_scanner nicht verfuegbar. Geplante Scans deaktiviert.")
        return None
    except Exception as e:
        logger.error(f"[Watchdog Service] Fehler beim Starten des Scheduled Scanners: {e}")
        return None


def main_service_loop():
    """Hauptschleife des Dienstes mit erweiteter Fehlerbehandlung."""
    retry_count = 0
    max_retries = 3
    scheduler_thread = None

    while retry_count < max_retries:
        try:
            logger.info(f"Watchdog Service wird gestartet... (Versuch {retry_count + 1}/{max_retries})")

            if not start_monitoring():
                logger.error(f"Fehler beim Starten der Ueberwachung (Versuch {retry_count + 1}).")
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(f"Warte 30 Sekunden vor naechstem Versuch...")
                    time.sleep(30)
                    continue
                else:
                    logger.error("Maximale Wiederholungen erreicht. Dienst wird beendet.")
                    return

            # Starte Scheduled Scanner als Hintergrund-Thread
            if scheduler_thread is None or not scheduler_thread.is_alive():
                scheduler_thread = start_scheduled_scanner()

            logger.info("Service laeuft. Warte auf Stop-Signal...")

            # Heartbeat-Ueberwachung
            heartbeat_counter = 0

            while not stop_event.is_set():
                # Pruefe alle 60 Sekunden, ob Observer noch aktiv ist
                if heartbeat_counter % 60 == 0:
                    if observer and not observer.is_alive():
                        logger.warning("Observer-Thread ist abgestuerzt. Starte Neustart...")
                        stop_monitoring()
                        raise Exception("Observer-Thread abgestuerzt")

                    # Log Heartbeat alle 10 Minuten
                    if heartbeat_counter % 600 == 0:
                        scheduler_status = "aktiv" if (scheduler_thread and scheduler_thread.is_alive()) else "inaktiv"
                        logger.info(f"Watchdog Service Heartbeat - Service laeuft normal (Scheduler: {scheduler_status})")

                        # Scheduler neu starten wenn er abgestuerzt ist
                        if scheduler_thread and not scheduler_thread.is_alive():
                            logger.warning("[Watchdog Service] Scheduled Scanner abgestuerzt, starte neu...")
                            scheduler_thread = start_scheduled_scanner()

                time.sleep(1)
                heartbeat_counter += 1

            break  # Normale Beendigung

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt empfangen. Beende Dienst...")
            stop_event.set()
            break
        except Exception as e:
            logger.error(f"Unerwarteter Fehler im Service (Versuch {retry_count + 1}): {e}")
            stop_monitoring()  # Cleanup
            retry_count += 1
            if retry_count < max_retries:
                logger.info(f"Warte 30 Sekunden vor Neustart...")
                time.sleep(30)
            else:
                logger.error("Maximale Wiederholungen erreicht. Service wird beendet.")

    # Aufraeumen
    stop_event.set()  # Signal an Scheduler-Thread
    stop_monitoring()
    logger.info("Watchdog Service beendet.")

# --- Windows Service Integration (Platzhalter) ---
# Diese Funktionen werden benötigt, wenn man pywin32 direkt verwenden würde.
# NSSM kümmert sich darum, das Skript einfach zu starten und zu stoppen.
# Wir brauchen hier keine komplexe Service-Steuerung mehr.

# --- Startpunkt ---
if __name__ == '__main__':
    # Entferne Debug-Kommentare
    main_service_loop()
    # Entferne Debug-Kommentare