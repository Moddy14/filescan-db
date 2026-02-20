import os
import shutil
import subprocess
import sys
import logging

# Importiere setup_logging aus utils
from utils import PROJECT_DIR, setup_logging

# Konfiguriere Logging für dieses Skript
# Verwende eine separate Log-Datei für den Build-Prozess
log_file = "build.log"
logger = setup_logging(log_filename=log_file, level_str="INFO")

# Automatisch das Verzeichnis des Skripts ermitteln (DateiDB/)
# PROJECT_DIR = os.path.dirname(os.path.abspath(__file__)) # Kommt jetzt aus utils
PORTABLE_DIR = os.path.join(PROJECT_DIR, "scanner_portable")
PORTABLE_VENV = os.path.join(PORTABLE_DIR, "venv")
# *** GEÄNDERT: Sucht NSSM jetzt direkt im tools-Ordner ***
SOURCE_NSSM_EXE = os.path.join(PROJECT_DIR, "tools", "nssm.exe")
# *** Zielordner für Tools im portablen Verzeichnis ***
PORTABLE_TOOLS_DIR = os.path.join(PORTABLE_DIR, "tools")
# *** Zielpfad für die NSSM-Datei im portablen Verzeichnis ***
PORTABLE_NSSM_EXE = os.path.join(PORTABLE_TOOLS_DIR, "nssm.exe")

FILES_TO_COPY = [
    "scanner_core.py",
    "watchdog_monitor.py",
    "integrity_checker.py",
    "gui_launcher.py",
    "systray_launcher_full.py",
    "exporter.py",
    "models.py",
    "utils.py",
    "config.json",
    "requirements.txt",
    "README.md",
    # Dienst-Skripte
    "setup_watchdog_service.ps1",
    "run_watchdog_service.bat"
]

EXPORT_DIR = os.path.join(PORTABLE_DIR, "exports")
STARTER_BAT = os.path.join(PORTABLE_DIR, "run_scanner_gui.bat")

def ensure_clean():
    if os.path.exists(PORTABLE_DIR):
        logger.info(f"Lösche vorhandenes Verzeichnis: {PORTABLE_DIR}")
        try:
            shutil.rmtree(PORTABLE_DIR)
        except OSError as e:
            logger.error(f"Fehler beim Löschen von {PORTABLE_DIR}: {e}")
            sys.exit(1) # Abbruch bei Fehler
    try:
        os.makedirs(PORTABLE_DIR)
        os.makedirs(EXPORT_DIR)
        # *** NEU: Ziel-Tools-Ordner auch erstellen (falls leer) ***
        # Wird durch copy_tools_folder erledigt, wenn es kopiert wird.
        # os.makedirs(PORTABLE_TOOLS_DIR, exist_ok=True)
        logger.info(f"Verzeichnisse {PORTABLE_DIR} und {EXPORT_DIR} erstellt.")
    except OSError as e:
        logger.error(f"Fehler beim Erstellen der Verzeichnisse: {e}")
        sys.exit(1)

def copy_project_files():
    logger.info("Kopiere Projektdateien...")
    copied_count = 0
    for filename in FILES_TO_COPY:
        src = os.path.join(PROJECT_DIR, filename)
        dst = os.path.join(PORTABLE_DIR, filename)
        if not os.path.exists(src):
            logger.error(f"Fehlende Quelldatei: {src}")
            # Nicht sofort abbrechen, vielleicht sind andere Dateien noch kopierbar?
            # Besser: Nach der Schleife prüfen, ob Fehler auftraten.
            # Stattdessen hier eine Exception werfen, die in main() gefangen wird?
            raise FileNotFoundError(f"Fehlende Quelldatei: {src}")
        try:
            shutil.copy2(src, dst)
            copied_count += 1
        except Exception as e:
            logger.error(f"Fehler beim Kopieren von {src} nach {dst}: {e}")
            raise # Fehler weitergeben
    logger.info(f"{copied_count} Projektdateien kopiert.")

def copy_nssm():
    # *** GEÄNDERT: Logik angepasst an den neuen Quellpfad ***
    logger.info(f"Kopiere nssm.exe von {SOURCE_NSSM_EXE}...")
    if not os.path.isfile(SOURCE_NSSM_EXE):
        logger.error(f"nssm.exe nicht gefunden unter: {SOURCE_NSSM_EXE}")
        logger.error("Bitte stelle sicher, dass nssm.exe direkt im Ordner 'tools' liegt.")
        raise FileNotFoundError(f"nssm.exe nicht gefunden: {SOURCE_NSSM_EXE}")

    try:
        # Erstelle den Ziel-Tools-Ordner, falls er nicht existiert
        os.makedirs(PORTABLE_TOOLS_DIR, exist_ok=True)
        # Kopiere die spezifische EXE-Datei
        shutil.copy2(SOURCE_NSSM_EXE, PORTABLE_NSSM_EXE)

        if os.path.exists(PORTABLE_NSSM_EXE):
            logger.info(f"nssm.exe erfolgreich nach {PORTABLE_NSSM_EXE} kopiert.")
        else:
            logger.error(f"Kopieren von nssm.exe nach {PORTABLE_NSSM_EXE} fehlgeschlagen.")
            raise IOError(f"Fehler beim Kopieren von nssm.exe nach {PORTABLE_NSSM_EXE}")

    except Exception as e:
        logger.error(f"Fehler beim Kopieren von nssm.exe: {e}")
        raise

def create_venv():
    logger.info(f"Erstelle virtuelle Umgebung in {PORTABLE_VENV}...")
    try:
        # Führe den Befehl aus und leite stdout/stderr um (optional, für weniger Konsolenausgabe)
        result = subprocess.run([sys.executable, "-m", "venv", PORTABLE_VENV], capture_output=True, text=True, check=True)
        logger.debug(result.stdout) # Nur im Debug-Level ausgeben
        logger.info("Virtuelle Umgebung erstellt.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Fehler beim Erstellen der venv: {e}")
        logger.error(f"Stderr: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"Unerwarteter Fehler beim Erstellen der venv: {e}")
        raise

def install_requirements():
    logger.info("Installiere Pakete aus requirements.txt in venv...")
    python_exe = os.path.join(PORTABLE_VENV, "Scripts", "python.exe")
    requirements_file = os.path.join(PORTABLE_DIR, "requirements.txt")

    if not os.path.exists(requirements_file):
        logger.error(f"requirements.txt nicht im portablen Verzeichnis gefunden: {requirements_file}")
        raise FileNotFoundError("requirements.txt fehlt im Build")

    commands = [
        [python_exe, "-m", "pip", "install", "--upgrade", "pip"],
        [python_exe, "-m", "pip", "install", "-r", requirements_file]
    ]

    for cmd in commands:
        try:
            logger.info(f"Führe aus: {' '.join(cmd)}")
            # Zeige pip-Ausgabe im Log?
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, encoding='utf-8')
            # Logge nur bei Erfolg oder wenn Debug aktiv ist
            logger.debug(result.stdout)
            if result.stderr:
                 logger.warning(f"Stderr von pip: {result.stderr}") # Warnung für stderr
        except subprocess.CalledProcessError as e:
            logger.error(f"Fehler bei Installation: {' '.join(cmd)}")
            logger.error(f"Stderr: {e.stderr}")
            logger.error(f"Stdout: {e.stdout}")
            raise
        except Exception as e:
            logger.error(f"Unerwarteter Fehler bei Installation: {e}")
            raise
    logger.info("Pakete aus requirements.txt erfolgreich in venv installiert.")

def create_launcher_batch():
    logger.info("Erstelle Start-Batch 'run_scanner_gui.bat'...")
    content = f"""@echo off
cd /d %~dp0
call venv\\Scripts\\activate.bat
REM Startet die GUI synchron
python gui_launcher.py
exit /b %errorlevel%
"""
    try:
        with open(STARTER_BAT, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info("Start-Batch erstellt.")
    except Exception as e:
        logger.error(f"Fehler beim Erstellen der Batch-Datei {STARTER_BAT}: {e}")
        raise

def build_systray_exe():
    logger.info("Erstelle systray_launcher_full.exe mit PyInstaller...")
    pyinstaller_exe = os.path.join(PORTABLE_VENV, "Scripts", "pyinstaller.exe")
    target_py = os.path.join(PORTABLE_DIR, "systray_launcher_full.py")
    build_dir = os.path.join(PORTABLE_DIR, "build") # Explizit definieren
    spec_file = os.path.join(build_dir, "systray_launcher_full.spec")

    cmd = [
        pyinstaller_exe,
        "--onefile",
        "--noconsole", # Keine Konsole für Tray-App
        "--distpath", PORTABLE_DIR, # Ausgabe-EXE direkt in portable
        "--workpath", build_dir,
        "--specpath", build_dir, # Spec-Datei auch in build
        # Optional: Icon hinzufügen
        # "--icon", os.path.join(PROJECT_DIR, "icon.ico"),
        target_py
    ]

    try:
        logger.info(f"Führe PyInstaller aus: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, encoding='utf-8')
        logger.debug(result.stdout) # Nur bei Debug ausgeben
        if result.stderr:
             logger.warning(f"Stderr von PyInstaller: {result.stderr}")

        # Aufräumen
        logger.info("Räume PyInstaller Build-Dateien auf...")
        if os.path.isdir(build_dir):
             try:
                 shutil.rmtree(build_dir)
             except OSError as e:
                 logger.warning(f"Konnte Build-Verzeichnis nicht löschen: {build_dir} - {e}")
        # Spec-Datei wird normalerweise im specpath (build_dir) erstellt und mit diesem gelöscht
        # if os.path.exists(spec_file):
        #     try: os.remove(spec_file) except OSError as e: logger.warning(...) 

        logger.info("systray_launcher_full.exe erfolgreich erstellt.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Fehler bei PyInstaller: {' '.join(cmd)}")
        logger.error(f"Stderr: {e.stderr}")
        logger.error(f"Stdout: {e.stdout}")
        raise
    except Exception as e:
        logger.error(f"Unerwarteter Fehler bei PyInstaller: {e}")
        raise

def main():
    logger.info("===== Starte Build: Portable Scanner-Distribution ====")
    try:
        ensure_clean()
        copy_project_files()
        copy_nssm()
        create_venv()
        install_requirements()
        create_launcher_batch()
        build_systray_exe()
        logger.info("===== Build erfolgreich abgeschlossen in 'scanner_portable/' ====")
    except FileNotFoundError as e:
        logger.error(f"Build fehlgeschlagen: {e}")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        # Fehler wurde bereits in der Funktion geloggt
        logger.error(f"Build fehlgeschlagen: Externer Prozessfehler (siehe Logs oben).")
        sys.exit(1)
    except Exception as e:
        logger.exception("Build fehlgeschlagen: Unerwarteter Fehler.") # Loggt auch den Stacktrace
        sys.exit(1)

if __name__ == "__main__":
    main()
