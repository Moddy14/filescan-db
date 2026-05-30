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


class TestOnModified:

    def test_modified_existing_file_is_indexed(self, handler, tmp_path):
        db = handler.db
        f = tmp_path / "data.txt"
        f.write_text("hello", encoding="utf-8")  # 5 Bytes, existiert physisch

        handler.on_modified(FakeEvent(str(f), False))

        db.cursor.execute("SELECT size FROM files WHERE filename = 'data'")
        row = db.cursor.fetchone()
        assert row is not None, "geänderte Datei muss indexiert werden"
        assert row[0] == 5

    def test_modified_directory_event_is_noop(self, handler):
        db = handler.db
        # Verzeichnis-Modify darf keine Datei-Eintraege erzeugen / nicht crashen
        handler.on_modified(FakeEvent("C:/somedir", True))
        db.cursor.execute("SELECT COUNT(*) FROM files")
        assert db.cursor.fetchone()[0] == 0


class TestPendingEventQueue:
    """Events, die waehrend eines Scan-Locks auflaufen, duerfen nicht verloren
    gehen, sondern werden gepuffert und beim naechsten Schreibfenster abgearbeitet."""

    def test_buffered_during_scanlock_then_drained(self, handler, monkeypatch):
        db = handler.db
        state = {"writable": False}
        monkeypatch.setattr(handler, "_wait_for_write_window", lambda p: state["writable"])

        # Scan laeuft (kein Schreibfenster) -> Event wird gepuffert, nicht verarbeitet
        handler.on_created(FakeEvent("C:/scanlockdir", True))
        db.cursor.execute("SELECT COUNT(*) FROM directories WHERE full_path = 'C:/scanlockdir'")
        assert db.cursor.fetchone()[0] == 0, "darf waehrend Scan-Lock nicht verarbeitet werden"
        assert len(handler._pending) == 1

        # Scan vorbei -> naechstes Event arbeitet Puffer + neues Event ab
        state["writable"] = True
        handler.on_created(FakeEvent("C:/afterscan", True))
        db.cursor.execute(
            "SELECT COUNT(*) FROM directories WHERE full_path IN ('C:/scanlockdir','C:/afterscan')"
        )
        assert db.cursor.fetchone()[0] == 2, "gepuffertes UND neues Event muessen verarbeitet sein"
        assert handler._pending == []

    def test_pending_buffer_respects_max(self, handler, monkeypatch):
        monkeypatch.setattr(handler, "_wait_for_write_window", lambda p: False)
        handler._pending_max = 3
        for i in range(10):
            handler.on_created(FakeEvent(f"C:/d{i}", True))
        assert len(handler._pending) == 3, "Puffer-Limit muss eingehalten werden"


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
