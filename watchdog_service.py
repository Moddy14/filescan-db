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
    from utils import logger, CONFIG, get_available_drives
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

    # ALT: Pfade aus der Konfiguration (könnte man zusätzlich machen, aber erstmal nur Laufwerke)
    # config_paths = CONFIG.get("watchdog_auto_paths", [])
    # if isinstance(config_paths, list) and config_paths:
    #     logger.info(f"Pfade aus Konfiguration (watchdog_auto_paths): {', '.join(config_paths)}")
    #     paths_to_watch.extend(config_paths) # Fügt sie hinzu
    # else:
    #     if not available_drives: # Nur warnen, wenn weder Laufwerke noch Config-Pfade da sind
    #          logger.warning("Keine Pfade in 'watchdog_auto_paths' in config.json gefunden.")

    if not paths_to_watch:
        logger.error("Keine Pfade zum Überwachen gefunden (weder automatisch noch in config). Der Dienst startet, aber überwacht nichts.")
        # Observer nicht starten, wenn nichts zu überwachen ist? Oder leer starten? Leer starten ist ok.
        # return False # Signalisiert, dass nichts gestartet wurde

    scheduled_count = 0
    for path in set(paths_to_watch): # set() um Duplikate zu vermeiden
        path = os.path.normpath(path)
        if os.path.isdir(path):
            try:
                event_handler = FSHandler(path) # Handler für jeden Pfad separat
                observer.schedule(event_handler, path, recursive=True)
                logger.info(f"Überwachung für '{path}' gestartet.")
                scheduled_count += 1
            except Exception as e:
                logger.error(f"Fehler beim Starten der Überwachung für '{path}': {e}")
        else:
            logger.warning(f"Pfad '{path}' ist kein gültiges Verzeichnis und wird ignoriert.")

    if scheduled_count == 0:
         logger.warning("Keine gültigen Überwachungen konnten gestartet werden.")
         # return False # Signalisiert, dass nichts gestartet wurde

    try:
        logger.info("Versuche Observer zu starten...")
        observer.start()
        # Kurze Pause, um sicherzustellen, dass der Thread läuft
        time.sleep(1)
        if observer.is_alive():
            logger.info("Observer erfolgreich gestartet und aktiv.")
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
def main_service_loop():
    """Hauptschleife des Dienstes mit erweiteter Fehlerbehandlung."""
    retry_count = 0
    max_retries = 3
    
    while retry_count < max_retries:
        try:
            logger.info(f"Watchdog Service wird gestartet... (Versuch {retry_count + 1}/{max_retries})")
            
            if not start_monitoring():
                logger.error(f"Fehler beim Starten der Überwachung (Versuch {retry_count + 1}).")
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(f"Warte 30 Sekunden vor nächstem Versuch...")
                    time.sleep(30)
                    continue
                else:
                    logger.error("Maximale Wiederholungen erreicht. Dienst wird beendet.")
                    return
            
            logger.info("Service läuft. Warte auf Stop-Signal...")
            
            # Heartbeat-Überwachung
            heartbeat_counter = 0
            
            while not stop_event.is_set():
                # Prüfe alle 60 Sekunden, ob Observer noch aktiv ist
                if heartbeat_counter % 60 == 0:
                    if observer and not observer.is_alive():
                        logger.warning("Observer-Thread ist abgestürzt. Starte Neustart...")
                        stop_monitoring()
                        raise Exception("Observer-Thread abgestürzt")
                    
                    # Log Heartbeat alle 10 Minuten
                    if heartbeat_counter % 600 == 0:
                        logger.info("Watchdog Service Heartbeat - Service läuft normal")
                
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
    
    # Aufräumen
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