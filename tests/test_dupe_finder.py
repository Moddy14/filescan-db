# -*- coding: utf-8 -*-
"""
tests/test_dupe_finder.py – Tests für die gemeinsame Duplikat-Such-Engine
(dupe_finder), die als wiederverwendbarer, getesteter Kern für die
Duplikat-Tools in Dateien_Skripte/ dient.
"""
import pytest


class TestHashDuplicates:

    def test_finds_identical_hashes(self, in_memory_db):
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        d = db.get_or_create_directory(drive, "C:/D")
        db.batch_insert_files([
            (d, "a.txt", 10, "HASH1"),
            (d, "b.txt", 20, "HASH1"),   # gleicher Hash -> Duplikat
            (d, "c.txt", 30, "HASH2"),   # einzigartig
        ])
        db.conn.commit()

        import dupe_finder
        dups = dict(dupe_finder.find_hash_duplicates(db))
        assert dups.get("HASH1") == 2
        assert "HASH2" not in dups

    def test_ignores_null_and_empty_hash(self, in_memory_db):
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        d = db.get_or_create_directory(drive, "C:/D")
        db.batch_insert_files([
            (d, "x.txt", 1, None),
            (d, "y.txt", 2, None),
        ])
        db.conn.commit()

        import dupe_finder
        assert dupe_finder.find_hash_duplicates(db) == []


class TestDuplicateFilesDetailed:

    def test_hash_detail_rows(self, in_memory_db):
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        d1 = db.get_or_create_directory(drive, "C:/A")
        d2 = db.get_or_create_directory(drive, "C:/B")
        db.batch_insert_files([
            (d1, "x.txt", 10, "H1"),
            (d2, "y.txt", 20, "H1"),   # gleicher Hash -> Gruppe
            (d1, "z.txt", 5, "H2"),    # einzigartig
        ])
        db.conn.commit()

        import dupe_finder
        rows = dupe_finder.find_duplicate_files(db, method="hash")
        # (id, full_path, filename, extension, size, hash, dup_count)
        assert len(rows) == 2
        assert all(r[5] == "H1" for r in rows)   # hash
        assert all(r[6] == 2 for r in rows)      # dup_count

    def test_name_size_detail_no_none_suffix(self, in_memory_db):
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        d1 = db.get_or_create_directory(drive, "C:/A")
        d2 = db.get_or_create_directory(drive, "C:/B")
        db.batch_insert_files([
            (d1, "Makefile", 100, None),
            (d2, "Makefile", 100, None),  # gleicher Name+Größe, ohne Extension
        ])
        db.conn.commit()

        import dupe_finder
        rows = dupe_finder.find_duplicate_files(db, method="name_size")
        assert len(rows) == 2
        # extension-Spalte (Index 3) darf kein '[none]' sein
        assert all(r[3] == "" for r in rows)


class TestNameSizeDuplicates:

    def test_finds_same_name_and_size(self, in_memory_db):
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        d1 = db.get_or_create_directory(drive, "C:/A")
        d2 = db.get_or_create_directory(drive, "C:/B")
        db.batch_insert_files([
            (d1, "report.txt", 100, None),
            (d2, "report.txt", 100, None),  # gleicher Name+Größe in anderem Dir
            (d1, "unique.txt", 5, None),
        ])
        db.conn.commit()

        import dupe_finder
        dups = dupe_finder.find_name_size_duplicates(db)
        names = {(fn, sz): c for fn, sz, c in dups}
        assert names.get(("report", 100)) == 2
        assert ("unique", 5) not in names
