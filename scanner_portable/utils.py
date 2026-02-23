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
    config_path = os.path.join(PROJECT_DIR, 'config.json')
    # Standardwerte definieren, inklusive hash_directories
    DEFAULT_CONFIG = {
        "base_path": None,  # Kein Standard-Scanpfad
        "log_level": "INFO",
        "hashing": False, # Standardmäßig kein globales Hashing
        "hash_directories": [], # Standardmäßig keine spezifischen Hash-Verzeichnisse
        "resume_scan": True, # Standardmäßig Scan fortsetzen (für manuelle GUI-Scans)
        "scheduled_scans": [], # NEU: Liste von Objekten {path, time, enabled, restart}
        "watchdog_auto_paths": [] # NEU: Pfade für automatischen Watchdog-Start
    }
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
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
                        scan_job.setdefault('restart', True) # Standard für geplante ist True
            else:
                 config['scheduled_scans'] = [] # Fallback, falls es kein Liste ist

            return config
    except FileNotFoundError:
        # Logge Fehler erst, *nachdem* der Logger initialisiert wurde!
        # Wir geben hier nur die Default-Config zurück.
        # logger.warning(f"[Konfigurations-Fehler] config.json nicht gefunden. Verwende Standardwerte.")
        save_config(DEFAULT_CONFIG) # Versuche zu speichern, loggt intern Fehler falls nötig
        return DEFAULT_CONFIG
    except json.JSONDecodeError as e:
        # Logge Fehler erst, *nachdem* der Logger initialisiert wurde!
        # logger.error(f"[Konfigurations-Fehler] config.json ist ungültig: {e}. Verwende Standardwerte und versuche zu überschreiben.")
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG
    except Exception as e:
         # Logge Fehler erst, *nachdem* der Logger initialisiert wurde!
         # logger.error(f"[Konfigurations-Fehler] Unerwarteter Fehler beim Laden: {e}. Verwende Standardwerte.")
         return DEFAULT_CONFIG

def save_config(config_data):
    """Speichert die Konfiguration in config.json."""
    config_path = os.path.join(PROJECT_DIR, 'config.json')
    try:
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, indent=4)
    except Exception as e:
        # Logge Fehler erst, *nachdem* der Logger initialisiert wurde!
        # Oder logge auf Konsole?
        print(f"[Konfigurations-Fehler] Konnte config.json nicht speichern: {e}")
        # logger.error(f"[Konfigurations-Fehler] Konnte config.json nicht speichern: {e}")

# --- Logging Setup ---
def setup_logging(log_filename="scanner.log", level_str="INFO", logger_name=None):
    # Konvertiere Level-String zu logging Level
    log_level = getattr(logging, level_str.upper(), logging.INFO)

    log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s")
    log_file_path = os.path.join(PROJECT_DIR, log_filename)

    if logger_name:
        logger_instance = logging.getLogger(logger_name)
    else:
        logger_instance = logging.getLogger() # Root-Logger

    logger_instance.setLevel(log_level)

    # Entferne vorhandene Handler, um Duplikate zu vermeiden
    # (besonders wichtig, wenn die Funktion mehrmals aufgerufen wird)
    for handler in logger_instance.handlers[:]:
        logger_instance.removeHandler(handler)

    # File Handler (UTF-8 erzwingen)
    max_bytes = 10 * 1024 * 1024 # 10 MB
    backup_count = 5
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

    logger_instance.info(f"Logging für '{logger_instance.name if logger_instance.name else 'root'}' konfiguriert (Level: {logging.getLevelName(log_level)}, Datei: {log_file_path}, Rotation: maxBytes={max_bytes}, backupCount={backup_count})") # Startmeldung
    return logger_instance

# --- Globale Variablen initialisieren ---
# Lade Config zuerst, um Log-Level zu bekommen
# Wichtig: Das Logging in load_config/save_config funktioniert erst NACHDEM der Logger hier initialisiert wurde!
_initial_config = load_config()

# Globalen Logger initialisieren (wird von anderen Modulen importiert)
logger = setup_logging(level_str=_initial_config.get('log_level', 'INFO'))

# Jetzt können Fehler aus load_config/save_config geloggt werden (falls sie beim Initialladen auftraten)
if not os.path.exists(os.path.join(PROJECT_DIR, 'config.json')):
    logger.warning(f"[Konfigurations-Fehler] config.json nicht gefunden. Standardwerte wurden verwendet und gespeichert.")

# Globale Konfiguration und abgeleitete Werte
CONFIG = _initial_config
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