# -*- coding: utf-8 -*-
"""
tests/test_cleanup_functions.py – Regressionstests für die Cleanup-Funktionen.

Hintergrund: Nach dem Schema-Refactoring (path/file_path -> full_path/filename
+extension_id) referenzierten cleanup_removed_dirs und cleanup_removed_files
weiterhin die alten Spaltennamen und lösten beim Aufruf einen
sqlite3.OperationalError aus ("no such column: path" bzw. "file_path").

Diese Tests fixieren das korrigierte Verhalten:
  (a) Die Funktionen dürfen NICHT mehr crashen.
  (b) Es werden nur Einträge entfernt, die weder im frisch gescannten Set
      enthalten sind NOCH auf dem Dateisystem existieren.
"""
import pytest


class TestCleanupRemovedDirs:

    def test_does_not_raise_on_new_schema(self, in_memory_db):
        """Regression: cleanup_removed_dirs darf keinen OperationalError werfen."""
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        db.get_or_create_directory(drive_id, "C:/KeepMe")
        db.cleanup_removed_dirs(drive_id, {"C:/KeepMe"})  # darf nicht crashen

    def test_removes_stale_dir(self, in_memory_db):
        """Ein nicht gescanntes, nicht existierendes Verzeichnis wird entfernt."""
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        keep = db.get_or_create_directory(drive_id, "C:/KeepMe")
        stale = db.get_or_create_directory(drive_id, "C:/GoneDir_xyz_123")

        db.cleanup_removed_dirs(drive_id, {"C:/KeepMe"})

        db.cursor.execute("SELECT id FROM directories WHERE id = ?", (stale,))
        assert db.cursor.fetchone() is None, "veraltetes Verzeichnis muss entfernt werden"
        db.cursor.execute("SELECT id FROM directories WHERE id = ?", (keep,))
        assert db.cursor.fetchone() is not None, "gescanntes Verzeichnis muss erhalten bleiben"

    def test_keeps_dir_that_exists_on_disk(self, in_memory_db, tmp_path):
        """Ein real existierendes Verzeichnis wird auch ungescannt NICHT entfernt."""
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        real_path = str(tmp_path).replace("\\", "/")
        rid = db.get_or_create_directory(drive_id, real_path)

        db.cleanup_removed_dirs(drive_id, set())  # leeres Scan-Set

        db.cursor.execute("SELECT id FROM directories WHERE id = ?", (rid,))
        assert db.cursor.fetchone() is not None, "existierendes Verzeichnis darf nicht gelöscht werden"


class TestCleanupRemovedFiles:

    def test_does_not_raise_on_new_schema(self, in_memory_db):
        """Regression: cleanup_removed_files darf keinen OperationalError werfen."""
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        dir_id = db.get_or_create_directory(drive_id, "C:/Docs")
        db.batch_insert_files([(dir_id, "keep.txt", 10, None)])
        db.conn.commit()
        db.cleanup_removed_files({"C:/Docs/keep.txt"})  # darf nicht crashen

    def test_removes_stale_file(self, in_memory_db):
        """Eine nicht gescannte, nicht existierende Datei wird entfernt; andere bleiben."""
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        dir_id = db.get_or_create_directory(drive_id, "C:/Docs")
        db.batch_insert_files([
            (dir_id, "keep.txt", 10, None),
            (dir_id, "gone.txt", 20, None),
        ])
        db.conn.commit()

        db.cleanup_removed_files({"C:/Docs/keep.txt"})

        db.cursor.execute("SELECT filename FROM files ORDER BY filename")
        remaining = [r[0] for r in db.cursor.fetchall()]
        assert remaining == ["keep"], f"nur 'keep' darf übrig bleiben, ist aber {remaining}"

    def test_handles_files_without_extension(self, in_memory_db):
        """Extensionslose Dateien müssen korrekt per Pfad rekonstruiert werden.

        Die files_legacy-View hängt für Dateien ohne Extension fälschlich
        '[none]' an; cleanup_removed_files darf solche (gescannten) Dateien
        deshalb NICHT versehentlich löschen.
        """
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        dir_id = db.get_or_create_directory(drive_id, "C:/Docs")
        db.batch_insert_files([(dir_id, "Makefile", 5, None)])  # keine Extension
        db.conn.commit()

        db.cleanup_removed_files({"C:/Docs/Makefile"})

        db.cursor.execute("SELECT COUNT(*) FROM files WHERE filename = 'Makefile'")
        assert db.cursor.fetchone()[0] == 1, "gescannte, extensionslose Datei muss erhalten bleiben"
