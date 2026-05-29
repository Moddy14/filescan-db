# -*- coding: utf-8 -*-
"""
tests/test_gui_data.py – Tests für den extrahierten, GUI-freien Daten-Layer
(gui_data). Dieser kapselt die DB-Abfragen, die bisher direkt im PyQt-UI-Thread
von gui_launcher.py liefen, und macht sie testbar + in Worker-Threads nutzbar.
"""
import pytest


class TestGetDriveOverview:

    def test_nonexistent_drive(self, in_memory_db):
        import gui_data
        ov = gui_data.get_drive_overview(in_memory_db, "Z:/")
        assert ov["exists"] is False
        assert ov["drive_id"] is None
        assert ov["file_count"] == 0
        assert ov["dir_count"] == 0
        assert ov["resume_point"] is None

    def test_counts_files_and_dirs(self, in_memory_db):
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        d = db.get_or_create_directory(drive, "C:/data")
        db.batch_insert_files([(d, "a.txt", 1, None), (d, "b.txt", 2, None)])
        db.conn.commit()

        import gui_data
        ov = gui_data.get_drive_overview(db, "C:/")
        assert ov["exists"] is True
        assert ov["drive_id"] == drive
        assert ov["file_count"] == 2
        assert ov["dir_count"] >= 1

    def test_resume_point(self, in_memory_db):
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        db.update_scan_progress(drive, "C:/data/sub")

        import gui_data
        ov = gui_data.get_drive_overview(db, "C:/")
        assert ov["resume_point"] == "C:/data/sub"

    def test_clean_orphaned_locks(self, in_memory_db):
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        db.update_scan_progress(drive, "C:/somewhere")
        lock_id = db.acquire_scan_lock("test")
        assert db.is_scan_running() is True

        import gui_data
        deleted, deactivated = gui_data.clean_orphaned_locks(db)

        assert deleted >= 1
        assert deactivated >= 1
        assert db.is_scan_running() is False
        db.cursor.execute("SELECT COUNT(*) FROM scan_progress")
        assert db.cursor.fetchone()[0] == 0

    def test_does_not_use_shared_cursor(self, in_memory_db):
        """Der Daten-Layer darf den geteilten db.cursor nicht 'verbrauchen'
        (eigener Cursor), damit parallele UI-Operationen nicht gestört werden."""
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        # geteilten Cursor in einen definierten Zustand bringen
        db.cursor.execute("SELECT name FROM drives")
        import gui_data
        gui_data.get_drive_overview(db, "C:/")
        # der geteilte Cursor muss weiterhin sein eigenes Ergebnis liefern
        rows = db.cursor.fetchall()
        assert any(r[0] == "C:/" for r in rows)
