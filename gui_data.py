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
    cursor = db.conn.cursor()
    try:
        cursor.execute("SELECT id FROM drives WHERE name = ?", (drive_name,))
        row = cursor.fetchone()
        if not row:
            return {
                "exists": False,
                "drive_id": None,
                "file_count": 0,
                "dir_count": 0,
                "resume_point": None,
            }

        drive_id = row[0]
        cursor.execute(
            "SELECT COUNT(*) FROM files f "
            "JOIN directories d ON f.directory_id = d.id "
            "WHERE d.drive_id = ?",
            (drive_id,),
        )
        file_count = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(*) FROM directories WHERE drive_id = ?", (drive_id,)
        )
        dir_count = cursor.fetchone()[0]

        # Bewusst eigener Cursor (nicht db.get_last_scan_path, das den geteilten
        # db.cursor verwenden würde):
        cursor.execute(
            "SELECT last_path FROM scan_progress WHERE drive_id = ?", (drive_id,)
        )
        rp_row = cursor.fetchone()
        resume_point = rp_row[0] if rp_row else None

        return {
            "exists": True,
            "drive_id": drive_id,
            "file_count": file_count,
            "dir_count": dir_count,
            "resume_point": resume_point,
        }
    finally:
        cursor.close()


def drive_name_from_path(path):
    """Ermittelt aus einem beliebigen Pfad den Laufwerksnamen in DB-Schreibweise.

    Beispiel: 'C:\\Users\\x' -> 'C:/'
    """
    return os.path.splitdrive(path)[0] + "/"
