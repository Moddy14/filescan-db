# -*- coding: utf-8 -*-
"""
tests/test_watchdog_events.py – Tests für die Watchdog-Event-Handler
on_created und on_deleted (Datei + Verzeichnis). Diese kritischen
DB-Konsistenz-Pfade waren bisher nicht abgedeckt.
"""
import os
import pytest


class FakeEvent:
    def __init__(self, src, is_dir):
        self.src_path = src
        self.is_directory = is_dir


@pytest.fixture
def handler(in_memory_db, monkeypatch):
    import watchdog_monitor
    monkeypatch.setattr(watchdog_monitor, "get_db_instance", lambda path=None: in_memory_db)
    monkeypatch.setattr(watchdog_monitor, "_normalize_path_for_watchdog",
                        lambda p: os.path.normpath(p))
    h = watchdog_monitor.FSHandler("C:/")
    # Event-Vorbedingungen für den Test deterministisch machen
    monkeypatch.setattr(h, "_wait_for_write_window", lambda p: True)
    monkeypatch.setattr(h, "_is_ignored", lambda p: False)
    return h


class TestOnCreated:

    def test_create_directory_adds_entry(self, handler):
        db, drive_id = handler.db, handler.drive_id
        handler.on_created(FakeEvent("C:/newdir", True))
        db.cursor.execute(
            "SELECT COUNT(*) FROM directories WHERE drive_id = ? AND full_path = ?",
            (drive_id, "C:/newdir"),
        )
        assert db.cursor.fetchone()[0] == 1


class TestOnDeleted:

    def test_delete_directory_removes_entry_and_cascades(self, handler):
        db, drive_id = handler.db, handler.drive_id
        d = db.get_or_create_directory(drive_id, "C:/gone")
        db.batch_insert_files([(d, "f.txt", 1, None)])
        db.conn.commit()

        handler.on_deleted(FakeEvent("C:/gone", True))

        db.cursor.execute("SELECT COUNT(*) FROM directories WHERE id = ?", (d,))
        assert db.cursor.fetchone()[0] == 0, "Verzeichnis-Eintrag muss entfernt sein"
        db.cursor.execute("SELECT COUNT(*) FROM files WHERE directory_id = ?", (d,))
        assert db.cursor.fetchone()[0] == 0, "Dateien müssen via CASCADE entfernt sein"

    def test_delete_file_removes_only_that_file(self, handler):
        db, drive_id = handler.db, handler.drive_id
        d = db.get_or_create_directory(drive_id, "C:/docs")
        db.batch_insert_files([(d, "keep.txt", 1, None), (d, "remove.txt", 2, None)])
        db.conn.commit()

        handler.on_deleted(FakeEvent("C:/docs/remove.txt", False))

        db.cursor.execute("SELECT filename FROM files WHERE directory_id = ?", (d,))
        names = sorted(r[0] for r in db.cursor.fetchall())
        assert names == ["keep"], f"nur 'keep' darf bleiben, ist aber {names}"
