import os
import hashlib
from datetime import datetime
import json
import logging
import sys
import time # Für Debug hinzugefügt
import logging.handlers

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_PATH = os.path.join(PROJECT_DIR, "scanner.log")
CONFIG_PATH = os.path.join(PROJECT_DIR, 'config.json') # Config Path als Konstante

# Globaler Logger wird später initialisiert
logger = None

def calculate_hash(filepath):
    """Berechnet den SHA256-Hash einer Datei. Gibt None bei Fehlern zurück."""
    try:
        hasher = hashlib.sha256()
        with open(filepath, "rb") as f:
            # Effizientes Lesen in Chunks
            for chunk in iter(lambda: f.read(8192), b""):
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()
    except FileNotFoundError:
        logger.error(f"[Hashing-Fehler] Datei nicht gefunden: {filepath}")
        return None
    except PermissionError:
        logger.error(f"[Hashing-Fehler] Keine Leseberechtigung für: {filepath}")
        return None
    except Exception as e:
        logger.error(f"[Hashing-Fehler] Unbekannter Fehler bei {filepath}: {e}")
        return None

def load_config():
    """Lädt die Konfiguration aus config.json."""
    # DEFAULT_CONFIG wie zuvor
    DEFAULT_CONFIG = {
        "base_path": None,
        "log_level": "INFO",
        "hashing": False,
        "hash_directories": [],
        "resume_scan": True,
        "scheduled_scans": [],
        "watchdog_auto_paths": []
    }
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            config = json.load(f)
            # Stelle sicher, dass alle Top-Level-Schlüssel vorhanden sind
            for key, value in DEFAULT_CONFIG.items():
                config.setdefault(key, value)
            
            # Stelle sicher, dass jedes Objekt in scheduled_scans die nötigen Keys hat
            if isinstance(config.get('scheduled_scans'), list):
                for scan_job in config['scheduled_scans']:
                    if isinstance(scan_job, dict):
                        scan_job.setdefault('path', None)
                        scan_job.setdefault('time', '00:00')
                        scan_job.setdefault('enabled', True)
                        scan_job.setdefault('restart', True)
                        scan_job.setdefault('scan_type', 'drive') # Standardmäßig Laufwerk/Ordner
            else:
                 config['scheduled_scans'] = []

            return config
    except FileNotFoundError:
        # Log/Print erst nach Logger-Initialisierung!
        # Gebe Default zurück, Fehler wird später geloggt.
        return DEFAULT_CONFIG # Wird speichern, wenn nicht vorhanden
    except json.JSONDecodeError as e:
        # Log/Print erst nach Logger-Initialisierung!
        # Gebe Default zurück, Fehler wird später geloggt.
        return DEFAULT_CONFIG # Wird versuchen, zu überschreiben
    except Exception as e:
         # Log/Print erst nach Logger-Initialisierung!
         # Gebe Default zurück, Fehler wird später geloggt.
         return DEFAULT_CONFIG

def save_config(config_data):
    """Speichert die Konfiguration in config.json."""
    try:
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, indent=4)
        # Logge Erfolg, falls logger existiert
        if logger:
            logger.info(f"[Konfiguration] config.json erfolgreich gespeichert.")
    except Exception as e:
        # Logge Fehler, falls logger existiert, sonst print
        error_msg = f"[Konfigurations-Fehler] Konnte config.json nicht speichern: {e}"
        if logger:
            logger.error(error_msg)
        else:
            print(error_msg, file=sys.stderr)

# --- Logging Setup ---
def _create_fallback_logger():
    """Erstellt einen minimalen Konsolen-Logger für den Fehlerfall."""
    fallback_logger = logging.getLogger('fallback_logger')
    # Verhindern, dass Nachrichten an den Root-Logger weitergegeben werden, falls dieser später doch funktioniert
    fallback_logger.propagate = False 
    # Sicherstellen, dass nicht immer wieder Handler hinzugefügt werden
    if not fallback_logger.hasHandlers():
        handler = logging.StreamHandler(sys.stderr) # Auf stderr ausgeben
        formatter = logging.Formatter("%(asctime)s - FALLBACK - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s")
        handler.setFormatter(formatter)
        fallback_logger.addHandler(handler)
        fallback_logger.setLevel(logging.WARNING) # Mindestens Warnungen anzeigen
    return fallback_logger

def setup_logging(log_filename="scanner.log", level_str="INFO", logger_name=None):
    try:
        # Konvertiere Level-String zu logging Level
        log_level = getattr(logging, level_str.upper(), logging.INFO)

        log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s")
        log_file_path = os.path.join(PROJECT_DIR, log_filename)

        if logger_name:
            logger_instance = logging.getLogger(logger_name)
        else:
            logger_instance = logging.getLogger() # Root-Logger

        logger_instance.setLevel(log_level)

        # Entferne vorhandene Handler
        for handler in logger_instance.handlers[:]:
            logger_instance.removeHandler(handler)
            handler.close() # Schließe Handler, um Ressourcen freizugeben

        # File Handler (UTF-8 erzwingen)
        max_bytes = 10 * 1024 * 1024 # 10 MB
        backup_count = 5
        # Versuche den Handler zu erstellen und hinzuzufügen
        file_handler = logging.handlers.RotatingFileHandler(
            log_file_path, 
            maxBytes=max_bytes, 
            backupCount=backup_count, 
            encoding='utf-8'
        )
        file_handler.setFormatter(log_formatter)
        logger_instance.addHandler(file_handler)

        # Console Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(log_formatter)
        console_handler.encoding = 'utf-8'
        logger_instance.addHandler(console_handler)

        logger_instance.info(f"Logging für '{logger_instance.name if logger_instance.name else 'root'}' erfolgreich konfiguriert (Level: {logging.getLevelName(log_level)}, Datei: {log_file_path})")
        return logger_instance

    except Exception as e:
        # KRITISCHER FEHLER: Logging konnte nicht initialisiert werden!
        # Gib Fehler auf stderr aus und returniere einen Fallback-Logger.
        print(f"--- KRITISCHER LOGGING FEHLER in setup_logging --- ", file=sys.stderr)
        print(f"Fehler: {e}", file=sys.stderr)
        print(f"Traceback:", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        print(f"----------------------------------------------------", file=sys.stderr)
        print(f"Initialisiere Fallback-Logger, der nur auf Konsole (stderr) schreibt.", file=sys.stderr)
        return _create_fallback_logger()

# --- Globale Variablen initialisieren ---
# Lade Config zuerst
_initial_config = load_config()

# Initialisiere den globalen Logger (jetzt mit Fehlerbehandlung)
logger = setup_logging(level_str=_initial_config.get('log_level', 'INFO'))

# Logge jetzt Konfigurationsfehler, falls aufgetreten
try:
    # Lade Config erneut, um sicherzugehen, dass wir die aktuellste haben (falls save_config im Fehlerfall lief)
    CONFIG = load_config()
    # Prüfe ob die Datei existierte oder neu erstellt wurde
    if not os.path.exists(CONFIG_PATH):
        logger.warning(f"[Konfiguration] config.json nicht gefunden. Standardwerte wurden verwendet und versucht zu speichern.")
        save_config(DEFAULT_CONFIG) # Speichern der Defaults, wenn nicht gefunden
    else:
        # Lade erneut, um sicherzustellen, dass sie gültig ist (falls sie vorher ungültig war)
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            json.load(f) # Teste nur das Laden
except json.JSONDecodeError as e:
    logger.error(f"[Konfiguration] config.json ist ungültig: {e}. Standardwerte werden verwendet und die Datei wird überschrieben.")
    CONFIG = DEFAULT_CONFIG
    save_config(CONFIG)
except FileNotFoundError: # Sollte nach obiger Logik nicht passieren, aber sicherheitshalber
    logger.error(f"[Konfiguration] config.json konnte auch nach Versuch nicht erstellt/gefunden werden. Verwende Standardwerte.")
    CONFIG = DEFAULT_CONFIG
except Exception as e:
    logger.error(f"[Konfiguration] Unerwarteter Fehler beim finalen Laden/Prüfen von config.json: {e}. Verwende Standardwerte.")
    CONFIG = DEFAULT_CONFIG

# Globale abgeleitete Werte
HASHING = CONFIG.get('hashing', False)
DB_PATH = os.path.join(PROJECT_DIR, "Dateien.db")

def get_available_drives():
    """Gibt eine Liste der verfügbaren Laufwerksbuchstaben zurück (z.B. ['C:\\', 'D:\\'])."""
    drives = []
    for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        drive_path = f"{letter}:\\"
        if os.path.exists(drive_path):
            drives.append(drive_path)
    return drives 