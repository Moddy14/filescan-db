# -*- coding: utf-8 -*-
"""
tests/test_watchdog_ignore.py – Tests für die Lärmfilterung des Watchdogs
(FSHandler._is_ignored). Diese Logik entscheidet, welche Dateisystem-Events
verarbeitet werden – falsch-negative Filterung würde echte Änderungen
verschlucken, falsch-positive würde die DB mit Lärm fluten.
"""
import os
import pytest


@pytest.fixture
def handler(in_memory_db, monkeypatch):
    import watchdog_monitor
    monkeypatch.setattr(watchdog_monitor, "get_db_instance", lambda path=None: in_memory_db)
    monkeypatch.setattr(watchdog_monitor, "_normalize_path_for_watchdog",
                        lambda p: os.path.normpath(p))
    # _is_ignored hier BEWUSST nicht patchen – das ist der Testgegenstand.
    return watchdog_monitor.FSHandler("C:/")


class TestIsIgnored:

    @pytest.mark.parametrize("name", ["desktop.ini", "thumbs.db", "Thumbs.db", ".DS_Store"])
    def test_ignored_filenames(self, handler, name):
        assert handler._is_ignored(f"C:/Users/Test/{name}") is True

    @pytest.mark.parametrize("ext", [".tmp", ".log", ".pyc", ".bak", ".db-wal", ".db-shm"])
    def test_ignored_extensions(self, handler, ext):
        assert handler._is_ignored(f"C:/Users/Test/file{ext}") is True

    def test_windows_dir_ignored(self, handler):
        windir = os.environ.get("WINDIR", "C:\\Windows")
        assert handler._is_ignored(os.path.join(windir, "System32", "x.dll")) is True

    def test_recycle_bin_ignored(self, handler):
        assert handler._is_ignored("D:/$Recycle.Bin/S-1-5-21/file.txt") is True

    def test_normal_file_not_ignored(self, handler):
        assert handler._is_ignored("C:/Users/Moddy/Documents/report.docx") is False
