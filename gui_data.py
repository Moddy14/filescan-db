# -*- coding: utf-8 -*-
"""
gui_data.py – GUI-freier Daten-Layer für die PyQt-Oberfläche.

Kapselt die Datenbank-Abfragen, die bisher direkt im UI-Thread von
gui_launcher.py ausgeführt wurden. Durch die Trennung sind die Abfragen
(a) automatisiert testbar und (b) in Worker-Threads auslagerbar, sodass die
Oberfläche bei größeren Datenbanken nicht mehr blockiert.

Es werden bewusst eigene Cursor verwendet, damit der geteilte db.cursor des
Singletons nicht mitten in einer anderen Operation überschrieben wird.
"""
import os


def get_drive_overview(db, drive_name):
    """Liefert eine Übersicht über ein Laufwerk.

    Args:
        db: DBManager-Instanz.
        drive_name: Laufwerksname in DB-Schreibweise (z. B. "C:/").

    Returns:
        dict mit den Schlüsseln:
            exists (bool), drive_id (int|None), file_count (int),
            dir_count (int), resume_point (str|None)
    """
    # read_query nutzt eine separate read-only Connection (Datei-DB), sodass
    # diese Lesezugriffe NICHT vom Schreib-Lock blockiert werden und den
    # geteilten db.cursor nicht berühren.
    rows = db.read_query("SELECT id FROM drives WHERE name = ?", (drive_name,))
    if not rows:
        return {
            "exists": False,
            "drive_id": None,
            "file_count": 0,
            "dir_count": 0,
            "resume_point": None,
        }

    drive_id = rows[0][0]
    file_count = db.read_query(
        "SELECT COUNT(*) FROM files f "
        "JOIN directories d ON f.directory_id = d.id "
        "WHERE d.drive_id = ?",
        (drive_id,),
    )[0][0]
    dir_count = db.read_query(
        "SELECT COUNT(*) FROM directories WHERE drive_id = ?", (drive_id,)
    )[0][0]
    rp = db.read_query(
        "SELECT last_path FROM scan_progress WHERE drive_id = ?", (drive_id,)
    )
    resume_point = rp[0][0] if rp else None

    return {
        "exists": True,
        "drive_id": drive_id,
        "file_count": file_count,
        "dir_count": dir_count,
        "resume_point": resume_point,
    }


def get_scan_status(db):
    """Liefert aktive Scan-Locks und Scan-Progress als strukturierte Daten.

    Verwendet einen eigenen Cursor (berührt den geteilten db.cursor nicht).

    Returns:
        dict mit:
            active_scans: Liste von (scan_type, start_time, pid, hostname)
            progress:     Liste von (drive_id, last_path, timestamp, drive_name)
    """
    # Separate read-only Connection (siehe read_query) – blockiert nicht den
    # Schreib-Lock und berührt den geteilten db.cursor nicht.
    active_scans = db.read_query(
        "SELECT scan_type, start_time, pid, hostname FROM scan_lock "
        "WHERE is_active = 1 ORDER BY start_time DESC"
    )
    progress = db.read_query(
        "SELECT sp.drive_id, sp.last_path, sp.timestamp, d.name "
        "FROM scan_progress sp LEFT JOIN drives d ON sp.drive_id = d.id"
    )
    return {"active_scans": active_scans, "progress": progress}


def clean_orphaned_locks(db):
    """Setzt alle aktiven Scan-Locks inaktiv und entfernt Scan-Progress-Einträge.

    Verwendet einen eigenen Cursor (berührt den geteilten db.cursor nicht).

    Returns:
        tuple (deleted_progress, deactivated_locks): Anzahl gelöschter
        Progress-Einträge und deaktivierter Locks.
    """
    cursor = db.conn.cursor()
    try:
        cursor.execute("DELETE FROM scan_progress")
        deleted = cursor.rowcount
        cursor.execute("UPDATE scan_lock SET is_active = 0 WHERE is_active = 1")
        deactivated = cursor.rowcount
        db.conn.commit()
        return deleted, deactivated
    finally:
        cursor.close()


def drive_name_from_path(path):
    """Ermittelt aus einem beliebigen Pfad den Laufwerksnamen in DB-Schreibweise.

    Beispiel: 'C:\\Users\\x' -> 'C:/'
    """
    return os.path.splitdrive(path)[0] + "/"
