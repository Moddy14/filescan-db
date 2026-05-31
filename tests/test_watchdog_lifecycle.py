# -*- coding: utf-8 -*-
"""
tests/test_watchdog_lifecycle.py – E2E-Lebenszyklus des Watchdogs.

Treibt die echten FSHandler-Event-Handler durch eine realistische Abfolge mit
ECHTEN Dateien auf der Platte: anlegen -> ändern -> umbenennen -> löschen, und
prüft nach JEDEM Vorgang, dass die DB den Zustand korrekt widerspiegelt.
Deckt zusätzlich die kritischen Fälle ab: gleichnamige Dateien mit verschiedener
Endung (logo.png/logo.svg) und Mehrpunkt-Namen ([none]).
"""
import os
import pytest
from models import split_name_ext


class FakeEvent:
    def __init__(self, src, is_dir=False):
        self.src_path = src
        self.is_directory = is_dir


class FakeMoveEvent:
    def __init__(self, src, dest, is_dir=False):
        self.src_path = src
        self.dest_path = dest
        self.is_directory = is_dir


def _drive_root(p):
    return os.path.splitdrive(str(p))[0] + "/"


def _norm(p):
    return os.path.normpath(str(p)).replace("\\", "/")


@pytest.fixture
def make_handler(in_memory_db, monkeypatch):
    import watchdog_monitor
    monkeypatch.setattr(watchdog_monitor, "get_db_instance", lambda path=None: in_memory_db)
    monkeypatch.setattr(watchdog_monitor, "_normalize_path_for_watchdog", lambda p: _norm(p))

    def _factory(drive):
        h = watchdog_monitor.FSHandler(drive)
        monkeypatch.setattr(h, "_wait_for_write_window", lambda p: True)
        monkeypatch.setattr(h, "_is_ignored", lambda p: False)
        return h
    return _factory


def _db_file_count(db, dirpath, basename):
    """Anzahl DB-Eintraege fuer 'basename' im Verzeichnis 'dirpath' (0 oder 1)."""
    filename, ext = split_name_ext(basename)
    db.cursor.execute(
        """
        SELECT COUNT(*) FROM files f
        JOIN directories d ON f.directory_id = d.id
        LEFT JOIN extensions e ON f.extension_id = e.id
        WHERE d.full_path = ? AND f.filename = ?
          AND CASE WHEN e.name IS NULL OR e.name = '[none]' THEN '[none]' ELSE e.name END = ?
        """,
        (_norm(dirpath), filename, ext),
    )
    return db.cursor.fetchone()[0]


def _db_files_in_dir(db, dirpath):
    db.cursor.execute(
        "SELECT COUNT(*) FROM files f JOIN directories d ON f.directory_id = d.id "
        "WHERE d.full_path = ?",
        (_norm(dirpath),),
    )
    return db.cursor.fetchone()[0]


class TestWatchdogFileLifecycle:

    def test_create_rename_delete_db_correct_after_each_step(self, in_memory_db, tmp_path, make_handler):
        db = in_memory_db
        h = make_handler(_drive_root(tmp_path))

        # --- 1) ANLEGEN ---
        f1 = tmp_path / "report.txt"
        f1.write_text("hello", encoding="utf-8")
        h.on_created(FakeEvent(str(f1), False))
        assert _db_file_count(db, tmp_path, "report.txt") == 1, "nach Anlegen muss die Datei in der DB sein"

        # --- 2) ÄNDERN (Größe) ---
        f1.write_text("hello world!!", encoding="utf-8")  # 13 Bytes
        h.on_modified(FakeEvent(str(f1), False))
        db.cursor.execute(
            "SELECT f.size FROM files f JOIN directories d ON f.directory_id=d.id "
            "WHERE d.full_path=? AND f.filename='report'", (_norm(tmp_path),))
        assert db.cursor.fetchone()[0] == 13, "geänderte Größe muss aktualisiert sein"

        # --- 3) UMBENENNEN report.txt -> notes.md ---
        f2 = tmp_path / "notes.md"
        f1.rename(f2)
        h.on_moved(FakeMoveEvent(str(f1), str(f2), False))
        assert _db_file_count(db, tmp_path, "report.txt") == 0, "alter Name muss weg sein"
        assert _db_file_count(db, tmp_path, "notes.md") == 1, "neuer Name muss in der DB sein"

        # --- 4) LÖSCHEN ---
        f2.unlink()
        h.on_deleted(FakeEvent(str(f2), False))
        assert _db_file_count(db, tmp_path, "notes.md") == 0, "gelöschte Datei darf nicht mehr in der DB sein"
        assert _db_files_in_dir(db, tmp_path) == 0, "Verzeichnis darf keine Datei-Einträge mehr haben"

    def test_same_stem_different_extension_coexist_then_delete_one(self, in_memory_db, tmp_path, make_handler):
        db = in_memory_db
        h = make_handler(_drive_root(tmp_path))

        png = tmp_path / "logo.png"
        svg = tmp_path / "logo.svg"
        png.write_bytes(b"a")
        svg.write_bytes(b"bb")
        h.on_created(FakeEvent(str(png), False))
        h.on_created(FakeEvent(str(svg), False))
        assert _db_file_count(db, tmp_path, "logo.png") == 1
        assert _db_file_count(db, tmp_path, "logo.svg") == 1, "gleichnamige Datei mit anderer Endung muss koexistieren"

        # nur logo.png löschen -> logo.svg bleibt
        png.unlink()
        h.on_deleted(FakeEvent(str(png), False))
        assert _db_file_count(db, tmp_path, "logo.png") == 0
        assert _db_file_count(db, tmp_path, "logo.svg") == 1, "die andere Datei darf NICHT mitgelöscht werden"

    def test_multidot_name_lifecycle(self, in_memory_db, tmp_path, make_handler):
        db = in_memory_db
        h = make_handler(_drive_root(tmp_path))

        # Mehrpunkt-Name -> Endung gilt als [none], voller Name bleibt
        f = tmp_path / "backup.2026"
        f.write_text("x", encoding="utf-8")
        h.on_created(FakeEvent(str(f), False))
        assert _db_file_count(db, tmp_path, "backup.2026") == 1, "Mehrpunkt-Datei muss indexiert sein"
        # keine Müll-Endung '.2026' angelegt
        db.cursor.execute("SELECT COUNT(*) FROM extensions WHERE name = '.2026'")
        assert db.cursor.fetchone()[0] == 0, "keine Müll-Endung anlegen"

        f.unlink()
        h.on_deleted(FakeEvent(str(f), False))
        assert _db_file_count(db, tmp_path, "backup.2026") == 0, "nach Löschen weg"
