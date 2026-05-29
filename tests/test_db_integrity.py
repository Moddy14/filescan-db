# -*- coding: utf-8 -*-
"""
tests/test_db_integrity.py – Datenintegritätstests für die FK-/CASCADE-Regeln.

Beweisen, dass die Foreign-Key-Constraints mit ON DELETE CASCADE tatsächlich
aktiv sind (nicht nur das PRAGMA gesetzt ist) – also keine Waisen-Datensätze
entstehen, wenn Laufwerke/Verzeichnisse entfernt werden.
"""
import pytest


class TestForeignKeyEnforcement:

    def test_pragma_foreign_keys_on(self, in_memory_db):
        db = in_memory_db
        db.cursor.execute("PRAGMA foreign_keys")
        assert db.cursor.fetchone()[0] == 1

    def test_delete_drive_cascades_everything(self, in_memory_db):
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        d = db.get_or_create_directory(drive, "C:/data")
        db.batch_insert_files([(d, "f.txt", 1, None)])
        db.update_scan_progress(drive, "C:/data")
        db.conn.commit()

        db.cursor.execute("DELETE FROM drives WHERE id = ?", (drive,))
        db.conn.commit()

        db.cursor.execute("SELECT COUNT(*) FROM directories WHERE drive_id = ?", (drive,))
        assert db.cursor.fetchone()[0] == 0, "directories muss CASCADE-gelöscht sein"
        db.cursor.execute("SELECT COUNT(*) FROM scan_progress WHERE drive_id = ?", (drive,))
        assert db.cursor.fetchone()[0] == 0, "scan_progress muss CASCADE-gelöscht sein"
        db.cursor.execute("SELECT COUNT(*) FROM files")
        assert db.cursor.fetchone()[0] == 0, "files muss via directory-CASCADE weg sein"

    def test_delete_directory_cascades_files(self, in_memory_db):
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        d = db.get_or_create_directory(drive, "C:/docs")
        db.batch_insert_files([(d, "a.txt", 1, None), (d, "b.txt", 2, None)])
        db.conn.commit()

        db.cursor.execute("DELETE FROM directories WHERE id = ?", (d,))
        db.conn.commit()

        db.cursor.execute("SELECT COUNT(*) FROM files WHERE directory_id = ?", (d,))
        assert db.cursor.fetchone()[0] == 0, "files im gelöschten Verzeichnis müssen weg sein"

    def test_delete_parent_directory_cascades_children(self, in_memory_db):
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        parent = db.get_or_create_directory(drive, "C:/projects")
        child = db.get_or_create_directory(drive, "C:/projects/app")
        db.batch_insert_files([(child, "main.py", 1, None)])
        db.conn.commit()

        db.cursor.execute("DELETE FROM directories WHERE id = ?", (parent,))
        db.conn.commit()

        db.cursor.execute("SELECT COUNT(*) FROM directories WHERE id = ?", (child,))
        assert db.cursor.fetchone()[0] == 0, "Unterverzeichnis muss via parent-CASCADE weg sein"
        db.cursor.execute("SELECT COUNT(*) FROM files WHERE directory_id = ?", (child,))
        assert db.cursor.fetchone()[0] == 0, "Dateien im Unterverzeichnis müssen weg sein"
