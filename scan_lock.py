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

def write_scan_lockfile(scan_name: str = "scan"):
    """Schreibt eine Lockdatei um laufenden Scan zu signalisieren.
    Wird von scan_all_drives.py und integrity_checker.py aufgerufen.
    """
    try:
        with open(SCAN_LOCK_FILE, "w", encoding="utf-8") as f:
            f.write(f"{scan_name}\npid={os.getpid()}\n")
        logger.info(f"[ScanLock] Lockdatei geschrieben: {SCAN_LOCK_FILE} ({scan_name})")
    except Exception as e:
        logger.warning(f"[ScanLock] Konnte Lockdatei nicht schreiben: {e}")


def remove_scan_lockfile():
    """Entfernt die Lockdatei nach Scan-Ende.
    Wird von scan_all_drives.py und integrity_checker.py aufgerufen.
    """
    try:
        if os.path.exists(SCAN_LOCK_FILE):
            os.remove(SCAN_LOCK_FILE)
            logger.info(f"[ScanLock] Lockdatei entfernt: {SCAN_LOCK_FILE}")
    except Exception as e:
        logger.warning(f"[ScanLock] Konnte Lockdatei nicht entfernen: {e}")


def lockfile_exists() -> bool:
    """True wenn eine Scan-Lockdatei existiert (anderer Prozess scannt)."""
    return os.path.exists(SCAN_LOCK_FILE)


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
