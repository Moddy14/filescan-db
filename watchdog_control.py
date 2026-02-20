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

SERVICE_NAME = "DateiScannerWatchdog"


def _is_nssm_service_running():
    """Prueft ob der Watchdog als Windows-Dienst (NSSM) laeuft."""
    try:
        result = subprocess.run(
            ['sc', 'query', SERVICE_NAME],
            capture_output=True, text=True, timeout=5,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
        )
        return 'RUNNING' in result.stdout.upper()
    except Exception:
        return False


def find_watchdog_pid():
    """Findet die PID des laufenden Watchdog-Services.

    Prueft sowohl Python-Prozesse als auch den Windows-Dienst (NSSM).
    Gibt die PID zurueck oder -1 wenn nur der Dienst laeuft (PID unbekannt).
    """
    # 1. Suche nach Python-Prozess
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info.get('cmdline', [])
            if cmdline and 'watchdog_service.py' in ' '.join(cmdline):
                return proc.info['pid']
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    # 2. Pruefe Windows-Dienst (NSSM)
    if _is_nssm_service_running():
        logger.info(f"[Watchdog] Dienst '{SERVICE_NAME}' laeuft als NSSM-Service")
        return -1  # Dienst laeuft, aber PID nicht direkt verfuegbar

    return None


def stop_watchdog():
    """Stoppt den Watchdog-Service sanft.

    Unterstuetzt sowohl direkte Python-Prozesse als auch NSSM-Dienste.
    """
    pid = find_watchdog_pid()

    if pid is None:
        logger.info("Watchdog-Service laeuft nicht")
        return True

    # Fall 1: NSSM-Dienst (pid == -1)
    if pid == -1:
        try:
            logger.info(f"[Watchdog] Stoppe NSSM-Dienst '{SERVICE_NAME}'...")
            result = subprocess.run(
                ['sc', 'stop', SERVICE_NAME],
                capture_output=True, text=True, timeout=10,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
            )
            # Warte bis Dienst gestoppt
            for _ in range(10):
                time.sleep(1)
                if not _is_nssm_service_running():
                    logger.info(f"[Watchdog] NSSM-Dienst '{SERVICE_NAME}' gestoppt")
                    return True
            logger.warning(f"[Watchdog] NSSM-Dienst konnte nicht innerhalb von 10s gestoppt werden")
            return False
        except Exception as e:
            logger.error(f"[Watchdog] Fehler beim Stoppen des NSSM-Dienstes: {e}")
            return False

    # Fall 2: Direkter Python-Prozess
    try:
        proc = psutil.Process(pid)
        proc.terminate()

        gone, alive = psutil.wait_procs([proc], timeout=5)

        if alive:
            for p in alive:
                p.kill()
            logger.warning(f"Watchdog-Service (PID {pid}) musste erzwungen beendet werden")
        else:
            logger.info(f"Watchdog-Service (PID {pid}) erfolgreich gestoppt")

        return True
    except Exception as e:
        logger.error(f"Fehler beim Stoppen des Watchdog-Service: {e}")
        return False

def start_watchdog():
    """Startet den Watchdog-Service.

    Versucht zuerst den NSSM-Dienst zu starten, dann Fallback auf direkten Start.
    """
    if find_watchdog_pid():
        logger.info("Watchdog-Service laeuft bereits")
        return True

    # Versuch 1: NSSM-Dienst starten
    try:
        result = subprocess.run(
            ['sc', 'query', SERVICE_NAME],
            capture_output=True, text=True, timeout=5,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
        )
        if result.returncode == 0 and 'STOPPED' in result.stdout.upper():
            logger.info(f"[Watchdog] Starte NSSM-Dienst '{SERVICE_NAME}'...")
            subprocess.run(
                ['sc', 'start', SERVICE_NAME],
                capture_output=True, text=True, timeout=10,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
            )
            for _ in range(10):
                time.sleep(1)
                if _is_nssm_service_running():
                    logger.info(f"[Watchdog] NSSM-Dienst '{SERVICE_NAME}' gestartet")
                    return True
            logger.warning("[Watchdog] NSSM-Dienst konnte nicht gestartet werden, versuche direkten Start")
    except Exception as e:
        logger.debug(f"[Watchdog] NSSM-Start fehlgeschlagen: {e}")

    # Versuch 2: Direkter Start als Python-Prozess
    try:
        script_path = os.path.join(os.path.dirname(__file__), 'watchdog_service.py')
        if os.path.exists(script_path):
            subprocess.Popen([sys.executable.replace('python.exe', 'pythonw.exe'), script_path],
                             cwd=os.path.dirname(__file__),
                             creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0)

            time.sleep(2)
            if find_watchdog_pid():
                logger.info("Watchdog-Service erfolgreich gestartet (direkter Prozess)")
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