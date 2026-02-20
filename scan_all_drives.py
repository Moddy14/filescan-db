import os
import sys
import subprocess
import time
import logging

# Importiere zentrale Funktionen und Konstanten
try:
    from utils import logger, setup_logging, PROJECT_DIR, get_available_drives
    from models import get_db_instance
except ImportError:
    print("FEHLER: utils.py oder models.py nicht gefunden. Stelle sicher, dass das Skript im Hauptverzeichnis des Projekts liegt.")
    # Fallback für Logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    sys.exit(1)

# Logging für dieses Skript konfigurieren
log_file = "scan_all.log"
try:
    # Versuche, das Logging über die utils-Funktion zu initialisieren
    logger = setup_logging(log_filename=log_file, level_str="INFO", logger_name="ScanAllDrives")
except TypeError:
    # Fallback, falls setup_logging den logger_name Parameter nicht erwartet
    logger.warning("Alte Version von setup_logging erkannt. Initialisiere Fallback-Logging.")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s', filename=log_file)
    logger = logging.getLogger("ScanAllDrives_Fallback")


def run_scan_for_drive(drive_path):
    """Führt scanner_core.py für ein bestimmtes Laufwerk mit --restart aus."""
    logger.info(f"Starte Scan für Laufwerk: {drive_path} (mit --restart)")
    
    # Pfad zum Python-Interpreter dieses Skripts
    python_exe = sys.executable 
    # Pfad zum scanner_core.py Skript
    scanner_script = os.path.join(PROJECT_DIR, "scanner_core.py")
    
    if not os.path.exists(scanner_script):
        logger.error(f"scanner_core.py nicht gefunden unter: {scanner_script}")
        return False
        
    command = [python_exe, scanner_script, drive_path, "--restart"]
    
    try:
        # Führe den Scan aus und warte auf das Ergebnis
        result = subprocess.run(command, 
                                capture_output=True, 
                                text=True, 
                                check=True, # Löst CalledProcessError bei != 0 Exit Code aus
                                encoding='utf-8',
                                errors='replace'
                                )
        logger.info(f"Scan für {drive_path} erfolgreich abgeschlossen.")
        # Logge die Standardausgabe des Scanners (kann nützlich sein)
        if result.stdout:
             logger.debug(f"Scan-Ausgabe für {drive_path}:\n{result.stdout}")
        if result.stderr:
             logger.warning(f"Scan-Fehlerausgabe für {drive_path}:\n{result.stderr}")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Scan für {drive_path} fehlgeschlagen (Exit Code: {e.returncode}).")
        if e.stderr:
            logger.error(f"Stderr:\n{e.stderr}")
        if e.stdout:
            logger.error(f"Stdout:\n{e.stdout}")
        # Bei Exit Code 1: Warnung statt Fehler (könnte Zugriffsrechte sein)
        if e.returncode == 1:
            logger.warning(f"Laufwerk {drive_path} übersprungen (möglicherweise Zugriffsrechte oder leer)")
            return True  # Als Erfolg werten, um weitere Scans fortzusetzen
        return False
    except Exception as e:
        logger.error(f"Unerwarteter Fehler beim Ausführen des Scans für {drive_path}: {e}")
        return False

def main():
    logger.info("===== Starte Skript zum Scannen aller Laufwerke ====")
    
    # Datenbankinstanz holen (wird für Lock-Prüfung benötigt)
    try:
        db = get_db_instance()
    except Exception as e:
        logger.critical(f"Konnte keine Datenbankverbindung herstellen: {e}")
        logger.critical("Skript wird beendet.")
        sys.exit(1)
        
    # Verfügbare Laufwerke holen (nur kanonische, ohne Aliases)
    try:
        from drive_alias_detector import get_canonical_drive_list
        drives = get_canonical_drive_list()
        if not drives:
            logger.warning("Keine verfügbaren Laufwerke gefunden.")
            return
        logger.info(f"Kanonische Laufwerke (ohne Aliases): {', '.join(drives)}")
    except Exception as e:
        logger.error(f"Fehler beim Ermitteln der Laufwerke: {e}")
        # Fallback auf alte Methode
        drives = get_available_drives()
        logger.info(f"Fallback - Alle Laufwerke: {', '.join(drives)}")
        if not drives:
            return

    # Alle Laufwerke nacheinander scannen (robuste Version)
    failed_drives = []
    successful_drives = []
    skipped_drives = []
    
    for drive in drives:
        logger.info(f"--- Bearbeite Laufwerk: {drive} ---")
        
        # Prüfen, ob bereits ein Scan läuft (via DB Lock)
        if db.is_scan_running():
            logger.warning(f"Ein anderer Scan läuft bereits. Überspringe Laufwerk {drive}.")
            skipped_drives.append(drive)
            continue # Zum nächsten Laufwerk
            
        # Scan für das aktuelle Laufwerk ausführen
        success = run_scan_for_drive(drive)
        if success:
            successful_drives.append(drive)
            logger.info(f"Scan für {drive} erfolgreich abgeschlossen.")
        else:
            failed_drives.append(drive)
            logger.warning(f"Laufwerk {drive} fehlgeschlagen. Setze mit nächstem fort.")
        
        # Kurze Pause zwischen den Laufwerken (optional, zur Entlastung)
        time.sleep(5)
    
    # Zusammenfassung ausgeben
    logger.info("===== SCAN-ZUSAMMENFASSUNG ====")
    logger.info(f"Erfolgreich gescannt: {len(successful_drives)} Laufwerke")
    if successful_drives:
        logger.info(f"  → {', '.join(successful_drives)}")
    
    if skipped_drives:
        logger.warning(f"Übersprungen (anderer Scan lief): {len(skipped_drives)} Laufwerke")
        logger.warning(f"  → {', '.join(skipped_drives)}")
    
    if failed_drives:
        logger.warning(f"Fehlgeschlagen: {len(failed_drives)} Laufwerke")
        logger.warning(f"  → {', '.join(failed_drives)}")
        logger.info("===== Skript mit teilweisen Fehlern beendet ====")
    else:
        logger.info("===== Alle verfügbaren Laufwerke erfolgreich gescannt ====")

if __name__ == "__main__":
    main() 