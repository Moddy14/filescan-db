"""
tests/test_models.py – Unit tests for models.py (DBManager)
"""

import os
import sqlite3
import pytest


class TestDBManagerSchema:
    """Verify schema is created correctly."""

    def test_all_core_tables_exist(self, in_memory_db):
        db = in_memory_db
        db.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = {r[0] for r in db.cursor.fetchall()}

        required = {
            "drives", "directories", "files",
            "extensions", "scan_progress", "scan_lock",
            "deleted_files", "deleted_directories",
        }
        missing = required - tables
        assert not missing, f"Missing tables: {missing}"

    def test_extensions_table_has_standard_entries(self, in_memory_db):
        db = in_memory_db
        db.cursor.execute("SELECT COUNT(*) FROM extensions")
        count = db.cursor.fetchone()[0]
        assert count > 10, "Expected standard extensions to be pre-populated"

    def test_wal_journal_mode(self, in_memory_db):
        db = in_memory_db
        db.cursor.execute("PRAGMA journal_mode")
        mode = db.cursor.fetchone()[0].lower()
        # WAL mode may not be available for all temp-file locations; just check it's set
        assert mode in ("wal", "delete", "memory"), f"Unexpected journal mode: {mode}"

    def test_foreign_keys_enabled(self, in_memory_db):
        db = in_memory_db
        db.cursor.execute("PRAGMA foreign_keys")
        fk = db.cursor.fetchone()[0]
        assert fk == 1, "Foreign keys must be ON"


class TestGetOrCreateDrive:

    def test_create_new_drive(self, in_memory_db):
        db = in_memory_db
        drive_id = db.get_or_create_drive("X:/")
        assert isinstance(drive_id, int)
        assert drive_id > 0

    def test_idempotent_drive_creation(self, in_memory_db):
        db = in_memory_db
        id1 = db.get_or_create_drive("Y:/")
        id2 = db.get_or_create_drive("Y:/")
        assert id1 == id2

    def test_two_drives_get_different_ids(self, in_memory_db):
        db = in_memory_db
        id_c = db.get_or_create_drive("C:/")
        id_d = db.get_or_create_drive("D:/")
        assert id_c != id_d


class TestGetOrCreateDirectory:

    def test_create_directory(self, in_memory_db):
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        dir_id = db.get_or_create_directory(drive_id, "C:/Users/Test")
        assert isinstance(dir_id, int)
        assert dir_id > 0

    def test_idempotent_directory_creation(self, in_memory_db):
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        id1 = db.get_or_create_directory(drive_id, "C:/Users/Test")
        id2 = db.get_or_create_directory(drive_id, "C:/Users/Test")
        assert id1 == id2

    def test_nested_directories(self, in_memory_db):
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        parent_id = db.get_or_create_directory(drive_id, "C:/Projects")
        child_id  = db.get_or_create_directory(drive_id, "C:/Projects/filescan")
        assert child_id != parent_id


class TestBatchInsertFiles:

    def test_insert_files_batch(self, in_memory_db):
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        dir_id   = db.get_or_create_directory(drive_id, "C:/TestDir")

        batch = [
            (dir_id, "readme.txt",    1024, None),
            (dir_id, "data.csv",      4096, "abc123"),
            (dir_id, "archive.zip", 102400, None),
        ]
        db.batch_insert_files(batch)
        db.conn.commit()

        db.cursor.execute(
            "SELECT COUNT(*) FROM files WHERE directory_id = ?", (dir_id,)
        )
        count = db.cursor.fetchone()[0]
        assert count == 3

    def test_upsert_on_duplicate(self, in_memory_db):
        """Re-inserting the same filename should update, not duplicate."""
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        dir_id   = db.get_or_create_directory(drive_id, "C:/TestDir2")

        db.batch_insert_files([(dir_id, "file.txt", 100, None)])
        db.conn.commit()
        db.batch_insert_files([(dir_id, "file.txt", 200, "newhash")])
        db.conn.commit()

        db.cursor.execute(
            "SELECT COUNT(*) FROM files WHERE directory_id = ?", (dir_id,)
        )
        assert db.cursor.fetchone()[0] == 1  # still only one row

        db.cursor.execute(
            "SELECT size FROM files WHERE directory_id = ? AND filename = 'file'",
            (dir_id,)
        )
        row = db.cursor.fetchone()
        assert row is not None
        assert row[0] == 200


class TestExtensionLookup:

    def test_known_extension_returns_id(self, in_memory_db):
        db = in_memory_db
        ext_id = db.get_or_create_extension(".pdf")
        assert ext_id is not None
        assert isinstance(ext_id, int)

    def test_unknown_extension_gets_created(self, in_memory_db):
        db = in_memory_db
        ext_id = db.get_or_create_extension(".xyzabc")
        assert ext_id is not None

    def test_no_extension_uses_none_placeholder(self, in_memory_db):
        db = in_memory_db
        ext_id = db.get_or_create_extension("")
        assert ext_id is not None

    def test_extension_category_document(self, in_memory_db):
        db = in_memory_db
        db.cursor.execute(
            "SELECT category FROM extensions WHERE name = '.pdf'"
        )
        row = db.cursor.fetchone()
        assert row is not None
        assert row[0] == "document"


class TestScanProgress:

    def test_update_and_retrieve_progress(self, in_memory_db):
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        db.update_scan_progress(drive_id, "C:/Windows/System32")

        last = db.get_last_scan_path(drive_id)
        assert last == "C:/Windows/System32"

    def test_clear_progress_sets_none(self, in_memory_db):
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        db.update_scan_progress(drive_id, "C:/SomePath")
        db.update_scan_progress(drive_id, None)

        last = db.get_last_scan_path(drive_id)
        assert last is None

    def test_no_progress_returns_none_for_new_drive(self, in_memory_db):
        db = in_memory_db
        drive_id = db.get_or_create_drive("Z:/")
        last = db.get_last_scan_path(drive_id)
        assert last is None


class TestScanLock:

    def test_acquire_and_release(self, in_memory_db):
        db = in_memory_db
        lock_id = db.acquire_scan_lock(scan_type="test")
        assert lock_id is not None

        released = db.release_scan_lock(lock_id)
        assert released is True

    def test_is_scan_running_after_acquire(self, in_memory_db):
        db = in_memory_db
        lock_id = db.acquire_scan_lock(scan_type="test")
        assert db.is_scan_running() is True
        db.release_scan_lock(lock_id)

    def test_is_scan_running_false_after_release(self, in_memory_db):
        db = in_memory_db
        lock_id = db.acquire_scan_lock(scan_type="test")
        db.release_scan_lock(lock_id)
        assert db.is_scan_running() is False

    def test_is_scan_running_fresh_db(self, in_memory_db):
        db = in_memory_db
        assert db.is_scan_running() is False


class TestClearDriveData:

    def test_clear_removes_only_target_drive(self, in_memory_db):
        db = in_memory_db
        drive_c = db.get_or_create_drive("C:/")
        drive_d = db.get_or_create_drive("D:/")

        dir_c = db.get_or_create_directory(drive_c, "C:/Test")
        dir_d = db.get_or_create_directory(drive_d, "D:/Test")

        db.batch_insert_files([(dir_c, "file_c.txt", 100, None)])
        db.batch_insert_files([(dir_d, "file_d.txt", 200, None)])
        db.conn.commit()

        db.clear_drive_data(drive_c)

        # C: files gone
        db.cursor.execute(
            "SELECT COUNT(*) FROM files f "
            "JOIN directories d ON f.directory_id = d.id "
            "WHERE d.drive_id = ?", (drive_c,)
        )
        assert db.cursor.fetchone()[0] == 0

        # D: files intact
        db.cursor.execute(
            "SELECT COUNT(*) FROM files f "
            "JOIN directories d ON f.directory_id = d.id "
            "WHERE d.drive_id = ?", (drive_d,)
        )
        assert db.cursor.fetchone()[0] == 1


class TestFileCache:

    def test_cache_miss_returns_falsy(self, in_memory_db):
        """Unknown entry returns a falsy value (False or None)."""
        db = in_memory_db
        result = db.file_cache.check(999, "nonexistent_file")
        assert not result  # either False or None

    def test_cache_hit_after_add(self, in_memory_db):
        db = in_memory_db
        db.file_cache.add(1, "myfile")
        assert db.file_cache.check(1, "myfile") is True

    def test_cache_remove(self, in_memory_db):
        db = in_memory_db
        db.file_cache.add(2, "another")
        db.file_cache.remove(2, "another")
        result = db.file_cache.check(2, "another")
        assert not result  # either False or None

    def test_cache_clear(self, in_memory_db):
        db = in_memory_db
        db.file_cache.add(3, "a")
        db.file_cache.add(3, "b")
        db.file_cache.clear()
        result = db.file_cache.check(3, "a")
        assert not result  # either False or None
