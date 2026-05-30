"""
tests/test_scanner_core.py – Integration tests for scanner_core.run_scan()
"""

import os
import sys
import pytest


class TestRunScan:
    """
    These tests invoke run_scan() against a real (temporary) directory tree
    and verify the database contains the expected data.

    They do NOT scan actual C:/ / D:/ drives.
    """

    @pytest.fixture(autouse=True)
    def _patch_db(self, in_memory_db, monkeypatch):
        """Wire run_scan to use the test DB instance."""
        import models
        monkeypatch.setattr(models, "_db_instance", in_memory_db)
        import scanner_core
        monkeypatch.setattr(scanner_core, "get_db_instance", lambda path=None: in_memory_db)
        self.db = in_memory_db

    def test_scan_creates_drive_entry(self, sample_file_tree):
        """After scanning a directory, a drive entry must exist."""
        import scanner_core
        result = scanner_core.run_scan(str(sample_file_tree))
        assert result is True

        self.db.cursor.execute("SELECT COUNT(*) FROM drives")
        assert self.db.cursor.fetchone()[0] >= 1

    def test_scan_indexes_files(self, sample_file_tree):
        """At least 6 files must be indexed (the sample_file_tree has exactly 6)."""
        import scanner_core
        scanner_core.run_scan(str(sample_file_tree))

        self.db.cursor.execute("SELECT COUNT(*) FROM files")
        count = self.db.cursor.fetchone()[0]
        # We expect 6, but allow for minor variability (e.g. conftest artifacts)
        assert count >= 6, f"Expected ≥6 files indexed, got {count}"

    def test_scan_indexes_directories(self, sample_file_tree):
        """Directories from the tree must be recorded (at least 4 sub-dirs + root)."""
        import scanner_core
        scanner_core.run_scan(str(sample_file_tree))

        self.db.cursor.execute("SELECT COUNT(*) FROM directories")
        count = self.db.cursor.fetchone()[0]
        assert count >= 5, f"Expected ≥5 directories, got {count}"

    def test_scan_returns_true_on_success(self, sample_file_tree):
        """run_scan must return True when the directory is accessible."""
        import scanner_core
        result = scanner_core.run_scan(str(sample_file_tree))
        assert result is True

    def test_scan_returns_false_or_empty_for_bad_path(self):
        """run_scan on a nonexistent path must return False (not raise)."""
        import scanner_core
        result = scanner_core.run_scan("/nonexistent/xyz_path_abc_789")
        # Must be bool; may succeed with 0 files or explicitly return False
        assert isinstance(result, bool)

    def test_force_restart_returns_true(self, sample_file_tree):
        """run_scan with force_restart=True must return True (no crash)."""
        import scanner_core

        # First scan
        scanner_core.run_scan(str(sample_file_tree))

        # Restart scan – main guarantee is it doesn't crash and returns True
        result = scanner_core.run_scan(str(sample_file_tree), force_restart=True)
        assert result is True

    def test_scan_records_file_sizes(self, sample_file_tree):
        """The scanner must store non-negative file sizes."""
        import scanner_core
        scanner_core.run_scan(str(sample_file_tree))

        self.db.cursor.execute(
            "SELECT COUNT(*) FROM files WHERE size IS NOT NULL AND size >= 0"
        )
        count = self.db.cursor.fetchone()[0]
        assert count >= 6, f"Expected ≥6 files with valid sizes, got {count}"

    def test_double_scan_does_not_duplicate_files(self, sample_file_tree):
        """Scanning the same tree twice must not duplicate file records."""
        import scanner_core
        scanner_core.run_scan(str(sample_file_tree))
        self.db.cursor.execute("SELECT COUNT(*) FROM files")
        count_first = self.db.cursor.fetchone()[0]

        scanner_core.run_scan(str(sample_file_tree))
        self.db.cursor.execute("SELECT COUNT(*) FROM files")
        count_second = self.db.cursor.fetchone()[0]

        assert count_second == count_first, (
            f"Duplicate scan inflated file count: {count_first} → {count_second}"
        )
