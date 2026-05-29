# -*- coding: utf-8 -*-
"""
tests/test_watchdog_move.py – Tests für die Verzeichnis-Move-Behandlung des
Watchdogs.

Hintergrund (Bug): on_moved() hat bei Verzeichnis-Moves bisher nur geloggt und
ist zurückgekehrt, ohne die full_path-Einträge der verschobenen Verzeichnisse
(und ihrer Nachkommen) zu aktualisieren. Folge: verwaiste Pfade, Duplikate beim
nächsten Scan, Integritätsfehler.

Diese Tests fixieren das korrekte Verhalten: ein Verzeichnis-Move aktualisiert
das Verzeichnis selbst UND alle Nachkommen; die zugehörigen Dateien bleiben über
ihre (unveränderte) directory_id verknüpft.
"""
import os
import pytest


class FakeMoveEvent:
    def __init__(self, src, dest, is_dir):
        self.src_path = src
        self.dest_path = dest
        self.is_directory = is_dir


@pytest.fixture
def handler(in_memory_db, monkeypatch):
    import watchdog_monitor
    # DB-Instanz und Pfad-Normalisierung für den Test deterministisch machen
    monkeypatch.setattr(watchdog_monitor, "get_db_instance", lambda path=None: in_memory_db)
    monkeypatch.setattr(watchdog_monitor, "_normalize_path_for_watchdog",
                        lambda p: os.path.normpath(p))
    h = watchdog_monitor.FSHandler("C:/")
    return h


class TestDirectoryMove:

    def test_handle_directory_move_updates_tree(self, handler):
        db, drive_id = handler.db, handler.drive_id
        old_id = db.get_or_create_directory(drive_id, "C:/old")
        sub_id = db.get_or_create_directory(drive_id, "C:/old/sub")
        db.batch_insert_files([(sub_id, "file.txt", 10, None)])
        db.conn.commit()

        moved = handler._handle_directory_move("C:/old", "C:/new")
        assert moved >= 2, "Verzeichnis und Nachkomme müssen aktualisiert werden"

        db.cursor.execute("SELECT full_path FROM directories WHERE id = ?", (old_id,))
        assert db.cursor.fetchone()[0] == "C:/new"
        db.cursor.execute("SELECT full_path FROM directories WHERE id = ?", (sub_id,))
        assert db.cursor.fetchone()[0] == "C:/new/sub"

        # keine alten Pfade mehr vorhanden
        db.cursor.execute("SELECT COUNT(*) FROM directories WHERE full_path LIKE 'C:/old%'")
        assert db.cursor.fetchone()[0] == 0, "es dürfen keine verwaisten alten Pfade bleiben"

        # Datei bleibt über die (unveränderte) directory_id verknüpft
        db.cursor.execute("SELECT COUNT(*) FROM files WHERE directory_id = ?", (sub_id,))
        assert db.cursor.fetchone()[0] == 1

    def test_directory_rename_only_basename(self, handler):
        db, drive_id = handler.db, handler.drive_id
        d = db.get_or_create_directory(drive_id, "C:/projects/alpha")
        db.conn.commit()
        handler._handle_directory_move("C:/projects/alpha", "C:/projects/beta")
        db.cursor.execute("SELECT full_path, directory_name FROM directories WHERE id = ?", (d,))
        full_path, name = db.cursor.fetchone()
        assert full_path == "C:/projects/beta"
        assert name == "beta"

    def test_on_moved_directory_invokes_update(self, handler, monkeypatch):
        """on_moved() muss bei Verzeichnis-Moves die DB tatsächlich aktualisieren."""
        db, drive_id = handler.db, handler.drive_id
        d = db.get_or_create_directory(drive_id, "C:/movedir")
        db.conn.commit()

        monkeypatch.setattr(handler, "_wait_for_write_window", lambda p: True)
        monkeypatch.setattr(handler, "_is_ignored", lambda p: False)

        handler.on_moved(FakeMoveEvent("C:/movedir", "C:/renamed", True))

        db.cursor.execute("SELECT full_path FROM directories WHERE id = ?", (d,))
        assert db.cursor.fetchone()[0] == "C:/renamed"
