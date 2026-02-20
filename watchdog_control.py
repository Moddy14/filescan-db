#!/usr/bin/env python3
"""
Watchdog Service Controller
Pausiert/Startet den Watchdog-Service für konfliktfreie Scans
"""

import os
import sys
import subprocess
import psutil
import time
import logging

logger = logging.getLogger(__name__)

def find_watchdog_pid():
    """Findet die PID des laufenden Watchdog-Services."""
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info.get('cmdline', [])
            if cmdline and 'watchdog_service.py' in ' '.join(cmdline):
                return proc.info['pid']
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None

def stop_watchdog():
    """Stoppt den Watchdog-Service sanft."""
    pid = find_watchdog_pid()
    if pid:
        try:
            # Versuche sanftes Beenden
            proc = psutil.Process(pid)
            proc.terminate()
            
            # Warte bis zu 5 Sekunden auf Beendigung
            gone, alive = psutil.wait_procs([proc], timeout=5)
            
            if alive:
                # Erzwinge Beendigung wenn nötig
                for p in alive:
                    p.kill()
                logger.warning(f"Watchdog-Service (PID {pid}) musste erzwungen beendet werden")
            else:
                logger.info(f"Watchdog-Service (PID {pid}) erfolgreich gestoppt")
            
            return True
        except Exception as e:
            logger.error(f"Fehler beim Stoppen des Watchdog-Service: {e}")
            return False
    else:
        logger.info("Watchdog-Service läuft nicht")
        return True

def start_watchdog():
    """Startet den Watchdog-Service."""
    if find_watchdog_pid():
        logger.info("Watchdog-Service läuft bereits")
        return True
    
    try:
        script_path = os.path.join(os.path.dirname(__file__), 'watchdog_service.py')
        if os.path.exists(script_path):
            # Starte im Hintergrund mit pythonw
            subprocess.Popen([sys.executable.replace('python.exe', 'pythonw.exe'), script_path],
                           cwd=os.path.dirname(__file__),
                           creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0)
            
            # Warte kurz und prüfe ob gestartet
            time.sleep(2)
            if find_watchdog_pid():
                logger.info("Watchdog-Service erfolgreich gestartet")
                return True
            else:
                logger.error("Watchdog-Service konnte nicht gestartet werden")
                return False
        else:
            logger.error(f"watchdog_service.py nicht gefunden: {script_path}")
            return False
    except Exception as e:
        logger.error(f"Fehler beim Starten des Watchdog-Service: {e}")
        return False

def pause_watchdog_for_scan(scan_function, *args, **kwargs):
    """
    Pausiert den Watchdog während eines Scans.
    
    Args:
        scan_function: Die Scan-Funktion die ausgeführt werden soll
        *args, **kwargs: Argumente für die Scan-Funktion
    
    Returns:
        Das Ergebnis der Scan-Funktion
    """
    watchdog_was_running = find_watchdog_pid() is not None
    
    if watchdog_was_running:
        logger.info("Pausiere Watchdog-Service für Scan...")
        stop_watchdog()
        time.sleep(1)  # Kurze Pause für saubere DB-Freigabe
    
    try:
        # Führe Scan aus
        result = scan_function(*args, **kwargs)
        return result
    finally:
        if watchdog_was_running:
            logger.info("Starte Watchdog-Service wieder...")
            time.sleep(1)
            start_watchdog()

def main():
    """Hauptfunktion für direkten Aufruf."""
    import argparse
    parser = argparse.ArgumentParser(description="Watchdog Service Controller")
    parser.add_argument("action", choices=["start", "stop", "restart", "status"],
                       help="Aktion die ausgeführt werden soll")
    args = parser.parse_args()
    
    if args.action == "stop":
        if stop_watchdog():
            print("[OK] Watchdog-Service gestoppt")
        else:
            print("[FEHLER] Konnte Watchdog-Service nicht stoppen")
            sys.exit(1)
    
    elif args.action == "start":
        if start_watchdog():
            print("[OK] Watchdog-Service gestartet")
        else:
            print("[FEHLER] Konnte Watchdog-Service nicht starten")
            sys.exit(1)
    
    elif args.action == "restart":
        stop_watchdog()
        time.sleep(2)
        if start_watchdog():
            print("[OK] Watchdog-Service neu gestartet")
        else:
            print("[FEHLER] Konnte Watchdog-Service nicht neu starten")
            sys.exit(1)
    
    elif args.action == "status":
        pid = find_watchdog_pid()
        if pid:
            print(f"[LÄUFT] Watchdog-Service läuft mit PID {pid}")
        else:
            print("[GESTOPPT] Watchdog-Service läuft nicht")

if __name__ == "__main__":
    main()