import os
import subprocess
import zipfile
from datetime import datetime
import logging
import sys

# Importiere setup_logging aus utils
from utils import PROJECT_DIR, setup_logging

# Konfiguriere Logging für dieses Skript
# Verwende dieselbe Log-Datei wie build_portable.py (oder eine andere?)
log_file = "build.log"
logger = setup_logging(log_filename=log_file, level_str="INFO")

# Basispfad
# BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # Kommt jetzt aus utils (ist PROJECT_DIR)
PORTABLE_DIR = os.path.join(PROJECT_DIR, "scanner_portable")
ZIP_PATH = os.path.join(PROJECT_DIR, "scanner_portable.zip")
BATCH_FILE = os.path.join(PORTABLE_DIR, "run_all.bat")
SCHEDULE_NAME = "DateiScanner-TäglicherScan"

def create_or_update_scheduled_task(task_name, script_name, trigger_type="MINUTE", trigger_modifier="15"):
    """Erstellt oder aktualisiert einen geplanten Task im Windows Task Scheduler.

    Args:
        task_name: Der Name des Tasks.
        script_name: Der Name des Python-Skripts im portable Verzeichnis, das ausgeführt werden soll.
        trigger_type: Der Typ des Triggers (z.B. "MINUTE", "ONSTART", "DAILY").
        trigger_modifier: Der Modifikator für den Trigger (z.B. Intervall in Minuten, Zeit HH:MM).
    """
    logger.info(f"Versuche Windows Task \''{task_name}\'' zu erstellen/aktualisieren...")
    # Pfad zum Skript und Python-Interpreter (ohne Konsole)
    target_script_path = os.path.join(PORTABLE_DIR, script_name)
    python_executable = os.path.join(PORTABLE_DIR, 'venv', 'Scripts', 'pythonw.exe')

    if not os.path.exists(python_executable) or not os.path.exists(target_script_path):
        logger.error(f"Benötigte Dateien für Task '{task_name}' nicht gefunden: {python_executable} oder {target_script_path}")
        raise FileNotFoundError(f"Benötigte Dateien für geplanten Task '{task_name}' fehlen.")

    # Aktion: PythonW ausführen mit dem Skript als Argument
    quoted_python_exe = f'"{python_executable}"'
    quoted_script_path = f'"{target_script_path}"'
    task_action = f'{quoted_python_exe} {quoted_script_path}'

    # Befehlsargumente zusammenstellen
    cmd_args = [
        'schtasks', '/create', '/tn', task_name, '/tr', task_action,
        '/sc', trigger_type
    ]
    # Füge Modifikator hinzu, falls für Trigger-Typ relevant
    if trigger_type in ["MINUTE", "HOURLY", "DAILY", "WEEKLY", "MONTHLY"]:
         # /MO für MINUTE/HOURLY, /ST für DAILY/WEEKLY/MONTHLY?
         # Korrektur: /MO ist für MINUTE, HOURLY, WEEKLY, MONTHLY Intervalle.
         #            /ST ist für DAILY (Zeit), ONSTART (Delay), ONLOGON (Delay), ONIDLE (IdleTime)
         if trigger_type == "MINUTE" or trigger_type == "HOURLY":
             cmd_args.extend(['/mo', trigger_modifier])
         elif trigger_type == "DAILY":
             cmd_args.extend(['/st', trigger_modifier]) # Erwartet HH:MM
         # TODO: Weitere Trigger-Typen bei Bedarf behandeln (WEEKLY, MONTHLY)
    elif trigger_type == "ONSTART":
         pass # Keine weiteren Argumente für /SC ONSTART nötig
    # Füge gemeinsame Argumente hinzu
    cmd_args.extend(['/ru', 'SYSTEM', '/rl', 'HIGHEST', '/f'])

    logger.info(f"Erstelle/Aktualisiere geplanten Task: {' '.join(cmd_args)}")
    try:
        result = subprocess.run(cmd_args, capture_output=True, text=True, check=True, shell=True)
        logger.info(f"Ausgabe von schtasks für Task '{task_name}': {result.stdout}")
        logger.info(f"Geplanter Task '{task_name}' erfolgreich erstellt/aktualisiert.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Fehler beim Erstellen des geplanten Tasks '{task_name}'. Return Code: {e.returncode}")
        logger.error(f"Fehlermeldung: {e.stderr}")
        return False # Gebe Fehler zurück, aber breche nicht ab
    except FileNotFoundError:
        logger.error(f"Fehler: 'schtasks' Befehl nicht gefunden für Task '{task_name}'.")
        return False
    except Exception as e:
         logger.error(f"Unerwarteter Fehler beim Erstellen des Tasks '{task_name}': {e}")
         return False

def zip_portable_directory():
    logger.info(f"Erstelle ZIP-Paket: {ZIP_PATH} aus {PORTABLE_DIR}")
    file_count = 0
    try:
        with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
            for root, _, files in os.walk(PORTABLE_DIR):
                for file in files:
                    abs_path = os.path.join(root, file)
                    # arcname ist der relative Pfad innerhalb der ZIP
                    rel_path = os.path.relpath(abs_path, PORTABLE_DIR)
                    zipf.write(abs_path, arcname=rel_path)
                    file_count += 1
        logger.info(f"{file_count} Dateien erfolgreich nach {ZIP_PATH} gepackt.")
    except Exception as e:
        logger.error(f"Fehler beim Erstellen der ZIP-Datei {ZIP_PATH}: {e}")
        raise # Fehler weiterleiten

def main():
    logger.info("===== Starte Build: Release-Paket ====")
    if not os.path.isdir(PORTABLE_DIR):
        logger.error(f"Verzeichnis '{PORTABLE_DIR}' nicht gefunden. Bitte zuerst build_portable.py ausführen.")
        sys.exit(1)

    # tasks_critical = True # Nicht mehr relevant, da keine Tasks erstellt werden
    # all_tasks_ok = True   # Nicht mehr relevant

    try:
        # *** ENTFERNT: Erstellung geplanter Tasks ***
        # scan_task_ok = create_or_update_scheduled_task(...)
        # if not scan_task_ok: all_tasks_ok = False
        # watchdog_task_ok = ... (bereits auskommentiert)
        # if not watchdog_task_ok: all_tasks_ok = False
        # if not all_tasks_ok and tasks_critical: ... (Fehlerprüfung entfernt)

        zip_portable_directory()
        logger.info("===== Build Release-Paket abgeschlossen ====")
        # *** ENTFERNT: Warnung bezüglich Task-Erstellung ***
        # if not all_tasks_ok and not tasks_critical: ...

    except Exception as e:
        logger.exception("Build Release-Paket fehlgeschlagen: Unerwarteter Fehler.")
        sys.exit(1)

if __name__ == "__main__":
    main()
