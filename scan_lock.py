"""
scan_lock.py – Koordiniert DB-Schreibpausen zwischen Bulk-Scans und FSHandler.

Problem: Wenn integrity_checker.py oder scan_all_drives.py läuft, halten sie
lange SQLite-Schreib-Transaktionen. Die FSHandler-Threads im Watchdog-Service
versuchen gleichzeitig zu schreiben → "database table is locked".

Lösung: ZWEI Mechanismen parallel:
  1. threading.Event (_write_allowed) – für In-Process-Koordination
     (scheduled_scanner.py ↔ watchdog_monitor.py laufen als Threads im
      selben Prozess watchdog_service.py → shared memory funktioniert)
  2. Lockdatei (SCAN_LOCK_FILE) – für externe Scan-Prozesse
     (scan_all_drives.py, integrity_checker.py oder Systray-gestartete Scans
      laufen als subprocess → anderer Prozess → Event nicht geteilt)
     Die Lockdatei wird von scan_all_drives.py / integrity_checker.py selbst
     geschrieben (Anfang → Ende). Der Watchdog liest sie bei jedem Event.

Verwendung:
  In watchdog_service.py (via scheduled_scanner.execute_scan):
      pause_fs_writes()  →  subprocess.run(scan)  →  resume_fs_writes()

  In scan_all_drives.py / integrity_checker.py (Anfang/Ende von main()):
      from scan_lock import write_scan_lockfile, remove_scan_lockfile
      write_scan_lockfile("scan_all_drives")
      ...
      remove_scan_lockfile()

  In watchdog_monitor.py (jeder Event-Handler):
      if not should_fs_write(): return  # Scan läuft → skip
"""

import os
import threading
import logging

# Nutze den Root-Logger (schreibt in scanner.log über den in utils.py konfigurierten Handler)
# getLogger(__name__) = "scan_lock" hätte keinen Handler → Messages würden verworfen
logger = logging.getLogger()

# --- Lockdatei-Pfad (neben der DB, im Projektverzeichnis) ---
try:
    from utils import PROJECT_DIR
    SCAN_LOCK_FILE = os.path.join(PROJECT_DIR, "scan.lock")
except Exception:
    # Fallback: relativ zum aktuellen Verzeichnis
    SCAN_LOCK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scan.lock")

# --- In-Process Event (für denselben Prozess, threading) ---
# Event: set = normaler Betrieb (FSHandler darf schreiben)
#        clear = Scan läuft (FSHandler soll warten/überspringen)
_write_allowed = threading.Event()
_write_allowed.set()  # Default: Schreiben erlaubt

_scan_name = ""  # Für Log-Ausgaben


# ──────────────────────────────────────────────────────────────
# Lockdatei-Mechanismus (prozessübergreifend)
# ──────────────────────────────────────────────────────────────

def _read_lockfile_info():
    """Liest scan.lock best-effort als key/value Dict."""
    info = {}
    try:
        with open(SCAN_LOCK_FILE, "r", encoding="utf-8", errors="replace") as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]
        if lines:
            info["scan_name"] = lines[0]
        for line in lines[1:]:
            if "=" in line:
                key, value = line.split("=", 1)
                info[key.strip()] = value.strip()
    except Exception as e:
        info["read_error"] = str(e)
    return info


def _pid_alive(pid_value) -> bool:
    try:
        pid = int(pid_value)
    except Exception:
        return True  # unbekanntes Format: lieber konservativ als Lock behandeln
    try:
        import psutil
        return psutil.pid_exists(pid)
    except Exception:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
        except Exception:
            return True


def write_scan_lockfile(scan_name: str = "scan"):
    """Schreibt eine Lockdatei um laufenden Scan zu signalisieren.

    Wichtig: Wenn bereits ein anderer Prozess die Lockdatei besitzt, wird sie
    NICHT überschrieben. Dadurch kann ein Child-Scan nicht versehentlich die
    Parent-Lockdatei ersetzen und später entfernen.
    """
    try:
        current_pid = os.getpid()
        if os.path.exists(SCAN_LOCK_FILE):
            existing = _read_lockfile_info()
            owner_pid = existing.get("pid")
            if owner_pid and str(owner_pid) != str(current_pid) and _pid_alive(owner_pid):
                logger.info(
                    f"[ScanLock] Bestehende Lockdatei bleibt erhalten: {SCAN_LOCK_FILE} "
                    f"(owner_pid={owner_pid}, requester={scan_name}, requester_pid={current_pid})"
                )
                return False
            # stale oder eigene Lockdatei: überschreiben/erneuern

        with open(SCAN_LOCK_FILE, "w", encoding="utf-8") as f:
            f.write(f"{scan_name}\npid={current_pid}\n")
        logger.info(f"[ScanLock] Lockdatei geschrieben: {SCAN_LOCK_FILE} ({scan_name}, pid={current_pid})")
        return True
    except Exception as e:
        logger.warning(f"[ScanLock] Konnte Lockdatei nicht schreiben: {e}")
        return False


def remove_scan_lockfile():
    """Entfernt die Lockdatei nach Scan-Ende, aber nur wenn sie diesem Prozess gehört."""
    try:
        if not os.path.exists(SCAN_LOCK_FILE):
            return False
        current_pid = os.getpid()
        existing = _read_lockfile_info()
        owner_pid = existing.get("pid")
        if owner_pid and str(owner_pid) != str(current_pid) and _pid_alive(owner_pid):
            logger.info(
                f"[ScanLock] Lockdatei nicht entfernt, anderer Owner aktiv: {SCAN_LOCK_FILE} "
                f"(owner_pid={owner_pid}, requester_pid={current_pid})"
            )
            return False
        os.remove(SCAN_LOCK_FILE)
        logger.info(f"[ScanLock] Lockdatei entfernt: {SCAN_LOCK_FILE} (pid={current_pid})")
        return True
    except Exception as e:
        logger.warning(f"[ScanLock] Konnte Lockdatei nicht entfernen: {e}")
        return False


def lockfile_exists() -> bool:
    """True wenn eine aktive Scan-Lockdatei existiert; stale Locks werden geheilt."""
    if not os.path.exists(SCAN_LOCK_FILE):
        return False
    existing = _read_lockfile_info()
    owner_pid = existing.get("pid")
    if owner_pid and not _pid_alive(owner_pid):
        try:
            os.remove(SCAN_LOCK_FILE)
            logger.warning(f"[ScanLock] Verwaiste Lockdatei entfernt: {SCAN_LOCK_FILE} (pid={owner_pid})")
            return False
        except Exception as e:
            logger.warning(f"[ScanLock] Verwaiste Lockdatei konnte nicht entfernt werden: {e}")
            return True
    return True


# ──────────────────────────────────────────────────────────────
# In-Process Event (für Threads im selben Prozess)
# ──────────────────────────────────────────────────────────────

def pause_fs_writes(scan_name: str = "Scan"):
    """Vor dem Bulk-Scan aufrufen. FSHandler-Threads überspringen DB-Writes.
    Schreibt ZUSÄTZLICH eine Lockdatei für externe Scan-Prozesse.
    """
    global _scan_name
    _scan_name = scan_name
    _write_allowed.clear()
    write_scan_lockfile(scan_name)
    logger.info(f"[ScanLock] FSHandler-Writes PAUSIERT für: {scan_name}")


def resume_fs_writes():
    """Nach dem Bulk-Scan aufrufen. FSHandler-Threads schreiben wieder normal.
    Entfernt ZUSÄTZLICH die Lockdatei.
    """
    global _scan_name
    logger.info(f"[ScanLock] FSHandler-Writes FORTGESETZT nach: {_scan_name}")
    _scan_name = ""
    _write_allowed.set()
    remove_scan_lockfile()


def should_fs_write(timeout: float = 0.0) -> bool:
    """
    FSHandler ruft das vor jedem DB-Write auf.

    Prüft BEIDE Mechanismen:
      - threading.Event (In-Process, für Scheduler-gestartete Scans)
      - Lockdatei (prozessübergreifend, für Systray/extern gestartete Scans)

    timeout=0.0 → sofort zurück (skip wenn Scan läuft)
    timeout>0   → warte bis zu N Sekunden auf Event-Freigabe
                  (Lockdatei wird immer sofort geprüft, kein Warten)

    Returns True wenn Schreiben erlaubt, False wenn Scan läuft.
    """
    # Lockdatei hat immer Vorrang (externer Prozess)
    if lockfile_exists():
        return False

    # In-Process Event prüfen
    if _write_allowed.is_set():
        return True
    if timeout > 0:
        return _write_allowed.wait(timeout=timeout)
    return False


def is_scan_running() -> bool:
    """True wenn gerade ein Bulk-Scan läuft (FSHandler sollte pausieren)."""
    return lockfile_exists() or not _write_allowed.is_set()
