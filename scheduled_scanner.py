# -*- coding: utf-8 -*-
import os
import sys
import time
import datetime
import subprocess
import logging
import argparse
import json

# Importiere zentrale Funktionen und Konstanten
from utils import logger, CONFIG, PROJECT_DIR, load_config, save_config
from models import get_db_instance

# Datei zum Tracking der letzten Ausfuehrungen
_LAST_RUN_FILE = os.path.join(PROJECT_DIR, '.scheduled_last_runs.json')

# Nachhol-Queue: Liste von (ziel_datetime, scan_config) — wird einmalig beim Start befuellt
_catchup_queue = []
_catchup_initialized = False


def _load_last_runs():
    """Laedt die letzten Ausfuehrungszeitpunkte aus der Tracking-Datei."""
    try:
        if os.path.exists(_LAST_RUN_FILE):
            with open(_LAST_RUN_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"[Scheduled Scan] Konnte Last-Run-Datei nicht laden: {e}")
    return {}


def _save_last_runs(last_runs):
    """Speichert die letzten Ausfuehrungszeitpunkte in die Tracking-Datei."""
    try:
        with open(_LAST_RUN_FILE, 'w', encoding='utf-8') as f:
            json.dump(last_runs, f, indent=2)
    except Exception as e:
        logger.warning(f"[Scheduled Scan] Konnte Last-Run-Datei nicht speichern: {e}")


def _scan_key(scan_config):
    """Erzeugt einen eindeutigen Schluessel fuer einen geplanten Scan."""
    scan_type = scan_config.get('scan_type', 'drive')
    path = scan_config.get('path') or 'global'
    scheduled_time = scan_config.get('time', '00:00')
    return f"{scan_type}_{path}_{scheduled_time}"


def _parse_time(time_str):
    """Parst einen HH:MM-String in (hour, minute). Gibt None bei Fehler zurueck."""
    try:
        parts = time_str.split(":")
        return int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        return None


def get_scheduled_scans():
    """Holt alle geplanten Scans aus der Konfiguration."""
    config = load_config()
    return config.get('scheduled_scans', [])


def _initialize_catchup():
    """Prueft beim Start ob Scans verpasst wurden und plant sie zeitversetzt nach.

    Logik:
    - Findet alle Scans, deren geplante Zeit heute schon vorbei ist (ausserhalb des 5-Min-Fensters)
    - Sortiert sie nach geplanter Zeit
    - Erster verpasster Scan: sofort
    - Folgende: mit dem Original-Zeitabstand zum ersten
    Beispiel: Scan 05:00, Integrity 06:00, PC startet 08:00
              -> Scan sofort (08:00), Integrity in 1h (09:00)
    """
    global _catchup_queue, _catchup_initialized
    if _catchup_initialized:
        return
    _catchup_initialized = True

    scheduled_scans = get_scheduled_scans()
    if not scheduled_scans:
        return

    now = datetime.datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    last_runs = _load_last_runs()

    missed = []
    for scan_config in scheduled_scans:
        if not scan_config.get('enabled', True):
            continue

        key = _scan_key(scan_config)
        if last_runs.get(key) == today_str:
            continue  # Heute bereits ausgefuehrt

        time_parsed = _parse_time(scan_config.get('time', '00:00'))
        if not time_parsed:
            continue

        scheduled_dt = now.replace(hour=time_parsed[0], minute=time_parsed[1], second=0, microsecond=0)
        window_end = scheduled_dt + datetime.timedelta(minutes=5)

        # Nur nachholen wenn das 5-Min-Fenster bereits komplett vorbei ist
        if window_end < now:
            missed.append((scheduled_dt, scan_config))

    if not missed:
        return

    # Sortiere nach geplanter Zeit
    missed.sort(key=lambda x: x[0])

    first_scheduled_time = missed[0][0]

    for scheduled_dt, scan_config in missed:
        gap = scheduled_dt - first_scheduled_time
        target = now + gap  # Erster: now+0=sofort, Folgende: now+Abstand
        _catchup_queue.append((target, scan_config))
        logger.info(
            f"[Scheduled Scan] Verpasster Scan wird nachgeholt: "
            f"{scan_config.get('scan_type')} (geplant {scheduled_dt.strftime('%H:%M')}) "
            f"-> Nachholzeit: {target.strftime('%H:%M:%S')}"
        )

    logger.info(f"[Scheduled Scan] {len(_catchup_queue)} verpasste Scans werden zeitversetzt nachgeholt.")


def should_scan_run_now(scan_config):
    """Prueft, ob ein geplanter Scan jetzt ausgefuehrt werden soll.

    Verwendet ein 5-Minuten-Zeitfenster und Last-Execution-Tracking,
    um verpasste und doppelte Ausfuehrungen zu vermeiden.
    """
    if not scan_config.get('enabled', True):
        return False

    now = datetime.datetime.now()
    today_str = now.strftime("%Y-%m-%d")

    time_parsed = _parse_time(scan_config.get('time', "00:00"))
    if not time_parsed:
        logger.error(f"[Scheduled Scan] Ungueltige Zeitangabe: {scan_config.get('time')}")
        return False

    # Zeitfenster: geplante Zeit bis +5 Minuten
    scheduled_dt = now.replace(hour=time_parsed[0], minute=time_parsed[1], second=0, microsecond=0)
    window_end = scheduled_dt + datetime.timedelta(minutes=5)

    if not (scheduled_dt <= now <= window_end):
        return False

    # Wurde dieser Scan heute bereits ausgefuehrt?
    key = _scan_key(scan_config)
    last_runs = _load_last_runs()
    if last_runs.get(key) == today_str:
        return False

    return True


def _mark_scan_executed(scan_config):
    """Markiert einen Scan als heute ausgefuehrt."""
    key = _scan_key(scan_config)
    last_runs = _load_last_runs()
    last_runs[key] = datetime.datetime.now().strftime("%Y-%m-%d")
    _save_last_runs(last_runs)


def execute_scan(scan_config):
    """Fuehrt einen geplanten Scan durch.

    Returns:
        bool: True wenn der Scan erfolgreich gestartet wurde
    """
    scan_type = scan_config.get('scan_type', 'drive')
    path = scan_config.get('path')
    restart = scan_config.get('restart', True)

    python_exe = sys.executable
    command = []
    log_info_path = ""

    if scan_type == 'full':
        script_path = os.path.join(PROJECT_DIR, 'scan_all_drives.py')
        if not os.path.exists(script_path):
            logger.error(f"[Scheduled Scan] Skript nicht gefunden: {script_path}")
            return False
        command = [python_exe, script_path]
        log_info_path = "alle Laufwerke (Gesamtscan)"

    elif scan_type == 'drive':
        if not path or not os.path.exists(path):
            logger.error(f"[Scheduled Scan] Pfad nicht gefunden oder ungueltig: {path}")
            return False
        script_path = os.path.join(PROJECT_DIR, 'scanner_core.py')
        if not os.path.exists(script_path):
            logger.error(f"[Scheduled Scan] Skript nicht gefunden: {script_path}")
            return False
        command = [python_exe, script_path, path, "--scheduled"]
        if restart:
            command.append("--restart")
        log_info_path = path

    elif scan_type == 'integrity':
        script_path = os.path.join(PROJECT_DIR, 'integrity_checker.py')
        if not os.path.exists(script_path):
            logger.error(f"[Scheduled Scan] Skript nicht gefunden: {script_path}")
            return False
        command = [python_exe, script_path]
        if path and os.path.exists(path):
            command.append(path)
            log_info_path = f"Integritaetspruefung fuer {path}"
        else:
            log_info_path = "Integritaetspruefung (gesamte Datenbank)"

    else:
        logger.error(f"[Scheduled Scan] Unbekannter scan_type: {scan_type}")
        return False

    try:
        logger.info(f"[Scheduled Scan] Starte geplanten Scan: {log_info_path}")
        log_name_part = scan_type + (f"_{os.path.basename(path)}" if path else "")
        stdout_log = os.path.join(PROJECT_DIR, f"scheduled_stdout_{log_name_part}.log")
        stderr_log = os.path.join(PROJECT_DIR, f"scheduled_stderr_{log_name_part}.log")

        with open(stdout_log, 'ab') as f_out, open(stderr_log, 'ab') as f_err:
            subprocess.Popen(command, stdout=f_out, stderr=f_err)

        _mark_scan_executed(scan_config)
        return True
    except Exception as e:
        logger.error(f"[Scheduled Scan] Fehler beim Starten des Scans fuer {log_info_path}: {e}")
        return False


def check_and_run_scheduled_scans():
    """Prueft alle geplanten Scans und fuehrt sie bei Bedarf aus.

    Reihenfolge:
    1. Nachhol-Queue (verpasste Scans) hat Vorrang
    2. Regulaere Scans (im aktuellen Zeitfenster)
    Nie mehr als ein Scan gleichzeitig.
    """
    # Einmalig beim ersten Aufruf verpasste Scans erkennen
    _initialize_catchup()

    db = get_db_instance()

    # --- Nachhol-Queue zuerst pruefen ---
    if _catchup_queue:
        now = datetime.datetime.now()
        target_time, scan_config = _catchup_queue[0]

        # Ist die Zielzeit erreicht?
        if now >= target_time:
            # Laeuft bereits ein Scan? Dann warten.
            if db.is_scan_running():
                logger.debug("[Scheduled Scan] Nachhol-Scan wartet — anderer Scan laeuft noch.")
                return

            # Scan starten
            scan_desc = f"{scan_config.get('scan_type')} ({scan_config.get('path', 'global')})"
            logger.info(f"[Scheduled Scan] Starte nachgeholten Scan: {scan_desc}")
            success = execute_scan(scan_config)
            _catchup_queue.pop(0)  # Aus Queue entfernen (auch bei Fehler, sonst Endlosschleife)

            if success:
                logger.info(f"[Scheduled Scan] Nachgeholter Scan gestartet: {scan_desc}")
            else:
                logger.error(f"[Scheduled Scan] Nachgeholter Scan fehlgeschlagen: {scan_desc}")
            return  # Nicht noch einen regulaeren Scan starten

        # Zielzeit noch nicht erreicht — warten
        return

    # --- Regulaere geplante Scans ---
    scheduled_scans = get_scheduled_scans()
    if not scheduled_scans:
        return

    if db.is_scan_running():
        return

    for scan_config in scheduled_scans:
        if should_scan_run_now(scan_config):
            success = execute_scan(scan_config)
            if success:
                logger.info(
                    f"[Scheduled Scan] Scan gestartet: {scan_config.get('scan_type')} "
                    f"- {scan_config.get('path', 'global')}"
                )
                time.sleep(5)
                if db.is_scan_running():
                    logger.info("[Scheduled Scan] Scan laeuft. Weitere geplante Scans werden uebersprungen.")
                    break
            else:
                logger.error(f"[Scheduled Scan] Scan konnte nicht gestartet werden: {scan_config.get('scan_type')}")


def run_scheduler_loop(interval=30, stop_event=None):
    """Laeuft als Endlosschleife und prueft regelmaessig auf faellige Scans.

    Kann sowohl standalone als auch als Thread in watchdog_service.py verwendet werden.

    Args:
        interval: Pruefintervall in Sekunden (Standard: 30)
        stop_event: Optional threading.Event zum sauberen Beenden
    """
    logger.info(f"[Scheduled Scanner] Scheduler gestartet (Intervall: {interval}s)")
    try:
        while True:
            if stop_event and stop_event.is_set():
                logger.info("[Scheduled Scanner] Stop-Signal empfangen, beende Scheduler.")
                break
            check_and_run_scheduled_scans()
            # Warte in kleinen Schritten, um Stop-Signal schneller zu erkennen
            for _ in range(interval):
                if stop_event and stop_event.is_set():
                    break
                time.sleep(1)
    except Exception as e:
        logger.error(f"[Scheduled Scanner] Fehler in Scheduler-Schleife: {e}")


def main():
    """Hauptfunktion: Prueft einmalig auf faellige Scans oder laeuft als Daemon."""
    parser = argparse.ArgumentParser(description="Verwaltet geplante Scans fuer den DateiScanner.")
    parser.add_argument("--daemon", action="store_true",
                        help="Laeuft als Hintergrunddienst und prueft regelmaessig auf faellige Scans.")
    parser.add_argument("--interval", type=int, default=30,
                        help="Pruefintervall in Sekunden (nur im Daemon-Modus). Standard: 30.")
    args = parser.parse_args()

    if args.daemon:
        try:
            run_scheduler_loop(interval=args.interval)
        except KeyboardInterrupt:
            logger.info("[Scheduled Scanner] Beende Daemon auf Benutzeranforderung.")
            sys.exit(0)
    else:
        logger.info("[Scheduled Scanner] Einmalige Pruefung auf faellige Scans.")
        check_and_run_scheduled_scans()
        logger.info("[Scheduled Scanner] Pruefung abgeschlossen.")


if __name__ == "__main__":
    main()
