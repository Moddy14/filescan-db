import os
import sys
import time
import datetime
import subprocess
import logging
import argparse

# Importiere zentrale Funktionen und Konstanten
from utils import logger, CONFIG, PROJECT_DIR, load_config, save_config
from models import get_db_instance

def get_scheduled_scans():
    """Holt alle geplanten Scans aus der Konfiguration."""
    config = load_config()
    return config.get('scheduled_scans', [])

def should_scan_run_now(scan_config):
    """Prüft, ob ein geplanter Scan jetzt ausgeführt werden soll.
    
    Args:
        scan_config: Dict mit Konfiguration des geplanten Scans
        
    Returns:
        bool: True wenn der Scan ausgeführt werden soll, False sonst
    """
    # Prüfe, ob der Scan aktiviert ist
    if not scan_config.get('enabled', True):
        return False
    
    # Holen der aktuellen Zeit
    now = datetime.datetime.now()
    current_time_str = now.strftime("%H:%M")
    
    # Holen der konfigurierten Zeit
    scheduled_time = scan_config.get('time', "00:00")
    
    # Vergleiche die Zeiten
    return current_time_str == scheduled_time

def execute_scan(scan_config):
    """Führt einen geplanten Scan durch.
    
    Args:
        scan_config: Dict mit Konfiguration des geplanten Scans
        
    Returns:
        bool: True wenn der Scan erfolgreich gestartet wurde, False sonst
    """
    scan_type = scan_config.get('scan_type', 'drive') # Standard ist 'drive'
    path = scan_config.get('path')
    restart = scan_config.get('restart', True)
    
    # Pfad zum Python-Interpreter
    python_exe = sys.executable
    
    command = []
    log_info_path = ""
    
    if scan_type == 'full':
        # Gesamtscan: scan_all_drives.py ausführen
        script_path = os.path.join(PROJECT_DIR, 'scan_all_drives.py')
        if not os.path.exists(script_path):
            logger.error(f"[Scheduled Scan] Skript nicht gefunden: {script_path}")
            return False
        command = [python_exe, script_path]
        log_info_path = "alle Laufwerke (Gesamtscan)"
    
    elif scan_type == 'drive':
        # Laufwerk/Ordner-Scan: scanner_core.py ausführen
        if not path or not os.path.exists(path):
            logger.error(f"[Scheduled Scan] Pfad nicht gefunden oder ungültig: {path}")
            return False
        script_path = os.path.join(PROJECT_DIR, 'scanner_core.py')
        if not os.path.exists(script_path):
            logger.error(f"[Scheduled Scan] Skript nicht gefunden: {script_path}")
            return False
        command = [python_exe, script_path, path, "--scheduled"]
        if restart:
            command.append("--restart")
        log_info_path = path
    else:
        logger.error(f"[Scheduled Scan] Unbekannter scan_type: {scan_type}")
        return False
    
    # Ausführen des Befehls
    try:
        logger.info(f"[Scheduled Scan] Starte geplanten Scan für {log_info_path}")
        # Verwende Popen, um den Prozess im Hintergrund zu starten
        # Leite stdout/stderr in eigene Logs um (optional, aber empfohlen)
        log_name_part = scan_type + (f"_{os.path.basename(path)}" if path else "")
        stdout_log = os.path.join(PROJECT_DIR, f"scheduled_stdout_{log_name_part}.log")
        stderr_log = os.path.join(PROJECT_DIR, f"scheduled_stderr_{log_name_part}.log")
        
        with open(stdout_log, 'ab') as f_out, open(stderr_log, 'ab') as f_err:
            process = subprocess.Popen(command, stdout=f_out, stderr=f_err)
            
        # Nicht auf Beendigung warten
        return True
    except Exception as e:
        logger.error(f"[Scheduled Scan] Fehler beim Starten des Scans für {log_info_path}: {e}")
        return False

def check_and_run_scheduled_scans():
    """Prüft alle geplanten Scans und führt sie bei Bedarf aus."""
    scheduled_scans = get_scheduled_scans()
    
    # Verlasse Funktion, wenn keine geplanten Scans konfiguriert sind
    if not scheduled_scans:
        logger.debug("[Scheduled Scan] Keine geplanten Scans konfiguriert.")
        return
    
    # Prüfen, ob ein Scan läuft
    db = get_db_instance()
    if db.is_scan_running():
        logger.warning("[Scheduled Scan] Ein Scan läuft bereits. Geplante Scans werden übersprungen.")
        return
    
    # Prüfe jeden konfigurierten Scan
    for scan_config in scheduled_scans:
        if should_scan_run_now(scan_config):
            # Ausführen des Scans
            success = execute_scan(scan_config)
            # Nach einem erfolgreichen Scan-Start müssen wir die weitere Ausführung beenden,
            # da sonst möglicherweise mehrere Scans parallel gestartet würden
            if success:
                logger.info(f"[Scheduled Scan] Scan für {scan_config.get('path')} wurde gestartet.")
                # Eine kurze Pause, damit der Scan-Prozess den Lock erwerben kann
                time.sleep(5)
                # Prüfe erneut, ob jetzt ein Scan läuft
                if db.is_scan_running():
                    logger.info("[Scheduled Scan] Scan läuft. Weitere geplante Scans werden übersprungen.")
                    break
                else:
                    logger.warning("[Scheduled Scan] Scan konnte nicht gestartet werden oder hat keinen Lock erworben.")
            else:
                logger.error(f"[Scheduled Scan] Scan für {scan_config.get('path')} konnte nicht gestartet werden.")

def main():
    """Hauptfunktion: Prüft einmalig auf fällige Scans oder läuft als Daemon."""
    parser = argparse.ArgumentParser(description="Verwaltet geplante Scans für den DateiScanner.")
    parser.add_argument("--daemon", action="store_true",
                       help="Läuft als Hintergrunddienst und prüft regelmäßig auf fällige Scans.")
    parser.add_argument("--interval", type=int, default=30,
                       help="Prüfintervall in Sekunden (nur im Daemon-Modus). Standard: 30.")
    args = parser.parse_args()
    
    if args.daemon:
        logger.info(f"[Scheduled Scanner] Starte im Daemon-Modus (Intervall: {args.interval} Sekunden).")
        try:
            while True:
                check_and_run_scheduled_scans()
                time.sleep(args.interval)
        except KeyboardInterrupt:
            logger.info("[Scheduled Scanner] Beende Daemon auf Benutzeranforderung.")
            sys.exit(0)
    else:
        logger.info("[Scheduled Scanner] Einmalige Prüfung auf fällige Scans.")
        check_and_run_scheduled_scans()
        logger.info("[Scheduled Scanner] Prüfung abgeschlossen.")

if __name__ == "__main__":
    main() 