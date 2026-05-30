# -*- coding: utf-8 -*-
"""
tests/test_filename_collision.py – Regression gegen aktiven Datenverlust.

Frueher war der UNIQUE-Index nur (directory_id, filename) OHNE extension_id, und
filename ist der Name OHNE Endung. Dadurch kollidierten gleichnamige Dateien mit
verschiedener Endung (logo.png vs logo.svg, index.html/.css/.js) und alle bis auf
eine gingen per INSERT OR IGNORE verloren. Der Fix erweitert den UNIQUE-Index sowie
Cache/Dedup um extension_id.
"""
import pytest


def _names(db, dir_id):
    db.cursor.execute(
        "SELECT f.filename, e.name FROM files f "
        "LEFT JOIN extensions e ON f.extension_id = e.id "
        "WHERE f.directory_id = ? ORDER BY f.filename, e.name",
        (dir_id,),
    )
    return [(fn, ext) for fn, ext in db.cursor.fetchall()]


class TestNoFileLossOnSameStem:

    def test_batch_insert_keeps_all_same_stem_files(self, in_memory_db):
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        d = db.get_or_create_directory(drive, "C:/web")
        db.batch_insert_files([
            (d, "logo.png", 111, None),
            (d, "logo.svg", 222, None),
            (d, "index.html", 333, None),
            (d, "index.css", 444, None),
            (d, "index.js", 555, None),
        ])
        db.conn.commit()
        db.cursor.execute("SELECT COUNT(*) FROM files WHERE directory_id = ?", (d,))
        assert db.cursor.fetchone()[0] == 5, f"Datenverlust! Gespeichert: {_names(db, d)}"

    def test_insert_file_optimized_keeps_both(self, in_memory_db):
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        d = db.get_or_create_directory(drive, "C:/web")
        db.insert_file_optimized(d, "logo.png", 111, None)
        db.insert_file_optimized(d, "logo.svg", 222, None)
        db.conn.commit()
        rows = _names(db, d)
        assert (".png" in [r[1] for r in rows]) and (".svg" in [r[1] for r in rows])
        assert len(rows) == 2, f"Datenverlust! {rows}"

    def test_same_extension_same_stem_is_still_one_row(self, in_memory_db):
        """Echte Duplikate (gleicher Name UND gleiche Endung) bleiben EINE Zeile (UPDATE)."""
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        d = db.get_or_create_directory(drive, "C:/web")
        db.insert_file_optimized(d, "logo.png", 111, None)
        db.insert_file_optimized(d, "logo.png", 999, None)  # gleiche Datei, neue Groesse
        db.conn.commit()
        db.cursor.execute("SELECT COUNT(*), MAX(size) FROM files WHERE directory_id = ?", (d,))
        cnt, size = db.cursor.fetchone()
        assert cnt == 1 and size == 999


class TestUniqueIndexMigration:

    def test_fresh_schema_index_includes_extension(self, in_memory_db):
        db = in_memory_db
        db.cursor.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' "
            "AND name='idx_files_directory_filename'"
        )
        sql = db.cursor.fetchone()[0]
        assert "extension_id" in sql

    def test_migration_replaces_legacy_two_column_index(self, in_memory_db):
        db = in_memory_db
        # Simuliere ALTEN Zustand: 2-spaltiger UNIQUE-Index
        db.cursor.execute("DROP INDEX IF EXISTS idx_files_directory_filename")
        db.cursor.execute(
            "CREATE UNIQUE INDEX idx_files_directory_filename ON files (directory_id, filename)"
        )
        db.conn.commit()
        # ensure_schema erneut -> Migration soll greifen
        db.ensure_schema()
        db.cursor.execute(
            "SELECT sql FROM sqlite_master WHERE name='idx_files_directory_filename'"
        )
        assert "extension_id" in db.cursor.fetchone()[0]
