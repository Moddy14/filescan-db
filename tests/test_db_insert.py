# -*- coding: utf-8 -*-
"""
tests/test_db_insert.py – Charakterisierungs- und Verhaltenstests für
DBManager.insert_file_optimized (vom Watchdog genutzt, bisher 0 % Coverage).

Diese Tests fixieren das korrekte Ist-Verhalten, BEVOR im DB-Layer toter Code
(der nie erreichte 'in_cache is False'-Zweig) entfernt wird – so ist garantiert,
dass das Refactoring das Verhalten nicht verändert.
"""
import pytest


class TestInsertFileOptimized:

    def test_insert_new_file_splits_extension(self, in_memory_db):
        """Neue Datei wird eingefügt; Endung wird vom Namen getrennt."""
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        dir_id = db.get_or_create_directory(drive_id, "C:/Docs")

        db.insert_file_optimized(dir_id, "report.pdf", 100, None)
        db.conn.commit()

        db.cursor.execute(
            "SELECT filename, size FROM files WHERE directory_id = ?", (dir_id,)
        )
        assert db.cursor.fetchone() == ("report", 100)

    def test_reinsert_updates_without_duplicating(self, in_memory_db):
        """Erneutes Einfügen derselben Datei aktualisiert (kein Duplikat)."""
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        dir_id = db.get_or_create_directory(drive_id, "C:/Docs")

        db.insert_file_optimized(dir_id, "report.pdf", 100, None)
        db.conn.commit()
        db.insert_file_optimized(dir_id, "report.pdf", 250, "newhash")
        db.conn.commit()

        db.cursor.execute(
            "SELECT COUNT(*) FROM files WHERE directory_id = ?", (dir_id,)
        )
        assert db.cursor.fetchone()[0] == 1

        db.cursor.execute(
            "SELECT size, hash FROM files WHERE directory_id = ? AND filename = 'report'",
            (dir_id,),
        )
        assert db.cursor.fetchone() == (250, "newhash")

    def test_insert_file_without_extension(self, in_memory_db):
        """Eine Datei ohne Endung behält ihren vollen Namen als filename."""
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        dir_id = db.get_or_create_directory(drive_id, "C:/Docs")

        db.insert_file_optimized(dir_id, "Makefile", 50, None)
        db.conn.commit()

        db.cursor.execute(
            "SELECT filename FROM files WHERE directory_id = ?", (dir_id,)
        )
        assert db.cursor.fetchone()[0] == "Makefile"

    def test_insert_assigns_extension_id(self, in_memory_db):
        """Die Datei wird mit der passenden Extension verknüpft."""
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        dir_id = db.get_or_create_directory(drive_id, "C:/Media")

        db.insert_file_optimized(dir_id, "clip.mp4", 2048, None)
        db.conn.commit()

        db.cursor.execute(
            """
            SELECT e.name FROM files f
            JOIN extensions e ON f.extension_id = e.id
            WHERE f.directory_id = ? AND f.filename = 'clip'
            """,
            (dir_id,),
        )
        assert db.cursor.fetchone()[0] == ".mp4"

    def test_stale_cache_hit_still_inserts(self, in_memory_db):
        """Cache-Inkonsistenz: Eintrag im FileCache, aber nicht in der DB.

        insert_file_optimized darf die Datei dann nicht 'verlieren', sondern
        muss sie trotzdem einfügen (robuster UPDATE-or-INSERT statt blindem
        UPDATE, das 0 Zeilen trifft).
        """
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        dir_id = db.get_or_create_directory(drive_id, "C:/Docs")

        # Cache fälschlich befüllen, ohne dass die Zeile in der DB existiert
        db.file_cache.add(dir_id, "ghost")

        db.insert_file_optimized(dir_id, "ghost.txt", 123, None)
        db.conn.commit()

        db.cursor.execute(
            "SELECT size FROM files WHERE directory_id = ? AND filename = 'ghost'",
            (dir_id,),
        )
        row = db.cursor.fetchone()
        assert row is not None, "Datei darf bei Cache-Inkonsistenz nicht verloren gehen"
        assert row[0] == 123
