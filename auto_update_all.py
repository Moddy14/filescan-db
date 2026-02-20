import os
import subprocess
import sys
import time
import psutil
from datetime import datetime
import logging

from utils import PROJECT_DIR, setup_logging

log_file = "build.log"
logger = setup_logging(log_filename=log_file, level_str="INFO")

PORTABLE_PATH = os.path.join(PROJECT_DIR, "scanner_portable")

def find_and_kill_app_processes():
    logger.info("Suche und beende laufende Anwendungs-Prozesse...")
    killed = 0
    app_scripts = [
        "scanner_core.py", "watchdog_monitor.py", "gui_launcher.py",
        "integrity_checker.py", "systray_launcher_full.py"
    ]
    for proc in psutil.process_iter(['pid', 'name', 'exe', 'cmdline']):
        pid = proc.pid
        try:
            name = proc.info.get('name', '').lower()
            exe = proc.info.get('exe', '').lower()
            cmdline = proc.info.get('cmdline') or []
            cmdline_str = " ".join(cmdline).lower()

            found = False
            for keyword in app_scripts:
                if name == keyword or exe.endswith(keyword) or any(arg.endswith(keyword) for arg in cmdline):
                    found = True
                    break
            
            if found:
                logger.info(f"Versuche Prozess zu beenden: PID {pid} - Name: {name} - Cmd: {cmdline_str}")
                target_proc = psutil.Process(pid)
                target_proc.terminate()
                try:
                    target_proc.wait(timeout=3)
                    logger.info(f"Prozess {pid} erfolgreich terminiert.")
                except psutil.TimeoutExpired:
                    logger.warning(f"Prozess {pid} reagiert nicht auf terminate, versuche kill...")
                    target_proc.kill()
                    target_proc.wait(timeout=1)
                    logger.info(f"Prozess {pid} durch kill beendet.")
                killed += 1

        except (psutil.NoSuchProcess, psutil.AccessDenied):
            logger.debug(f"Fehler beim Zugriff auf Prozess {pid}, übersprungen (vermutlich bereits beendet oder keine Rechte).")
            continue
        except Exception as e:
            logger.warning(f"Fehler bei Prüfung von Prozess {pid}: {e}")
            continue

    if killed == 0:
        logger.info("Keine laufenden App-Prozesse zum Beenden gefunden.")
    else:
        logger.info(f"{killed} Prozesse beendet.")
    time.sleep(1)

def run_build_script(script):
    logger.info(f"Führe Build-Skript aus: {script}...")
    script_path = os.path.join(PROJECT_DIR, script)
    if not os.path.exists(script_path):
         logger.error(f"Build-Skript nicht gefunden: {script_path}")
         return False
    try:
        result = subprocess.run([sys.executable, script_path],
                                capture_output=True,
                                text=True,
                                check=True,
                                encoding='utf-8',
                                errors='replace')
        logger.debug(f"Stdout von {script}:\n{result.stdout}")
        if result.stderr:
             logger.warning(f"Stderr von {script}:\n{result.stderr}")
        logger.info(f"{script} erfolgreich ausgeführt.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Fehler bei Ausführung von {script} (Return Code: {e.returncode}). Siehe build.log für Details.")
        logger.error(f"Stderr von {script}:\n{e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Unerwarteter Fehler beim Ausführen von {script}: {e}")
        return False

def main():
    logger.info("====== Starte Auto-Update/Build Prozess ======")
    all_success = True

    try:
        find_and_kill_app_processes()

        if not run_build_script("build_portable.py"):
            all_success = False
            logger.error("Fehler in build_portable.py. Überspringe nachfolgende Build-Schritte.")
        
        if all_success:
            if not run_build_script("build_release_package.py"):
                all_success = False
                logger.error("Fehler in build_release_package.py")

        if all_success:
            logger.info("====== Auto-Update/Build erfolgreich abgeschlossen ======")
        else:
             logger.error("====== Auto-Update/Build mit Fehlern abgeschlossen ======")
             sys.exit(1)

    except Exception as e:
        logger.exception("Auto-Update/Build fehlgeschlagen: Unerwarteter Fehler im Hauptablauf.")
        sys.exit(1)

if __name__ == "__main__":
    try:
        import psutil
    except ImportError:
        print("FEHLER: Bitte installiere zuerst 'psutil': pip install psutil")
        sys.exit(1)

    main()
