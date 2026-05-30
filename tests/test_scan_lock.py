"""
tests/test_scan_lock.py – Unit tests for scan_lock.py
"""

import os
import pytest


class TestScanLockFile:

    @pytest.fixture(autouse=True)
    def _patch_lockfile_path(self, tmp_path, monkeypatch):
        """Redirect the lockfile into a temp dir so tests don't pollute PROJECT_DIR."""
        import scan_lock
        lock_path = str(tmp_path / "test_scan.lock")
        monkeypatch.setattr(scan_lock, "SCAN_LOCK_FILE", lock_path)
        self._lock_path = lock_path
        yield

    def test_write_creates_lockfile(self):
        """write_scan_lockfile() must create the lock file on disk."""
        import scan_lock
        scan_lock.write_scan_lockfile("test:C:/")
        assert os.path.isfile(scan_lock.SCAN_LOCK_FILE)

    def test_lockfile_contains_label(self):
        """The lock file must contain the label passed to write_scan_lockfile()."""
        import scan_lock
        scan_lock.write_scan_lockfile("scanner_core:C:/")
        content = open(scan_lock.SCAN_LOCK_FILE, encoding="utf-8").read()
        assert "scanner_core:C:/" in content

    def test_remove_deletes_lockfile(self):
        """remove_scan_lockfile() must delete the lock file."""
        import scan_lock
        scan_lock.write_scan_lockfile("test:C:/")
        scan_lock.remove_scan_lockfile()
        assert not os.path.isfile(scan_lock.SCAN_LOCK_FILE)

    def test_should_fs_write_true_when_no_lock(self):
        """should_fs_write() returns True when no lock file exists."""
        import scan_lock
        # Ensure lock doesn't exist
        if os.path.exists(scan_lock.SCAN_LOCK_FILE):
            os.remove(scan_lock.SCAN_LOCK_FILE)
        assert scan_lock.should_fs_write() is True

    def test_should_fs_write_false_when_locked(self):
        """should_fs_write() returns False when lock file is present."""
        import scan_lock
        scan_lock.write_scan_lockfile("test:C:/")
        assert scan_lock.should_fs_write() is False

    def test_remove_nonexistent_is_safe(self, tmp_path, monkeypatch):
        """remove_scan_lockfile() on a non-existent file must not raise."""
        import scan_lock
        monkeypatch.setattr(scan_lock, "SCAN_LOCK_FILE", str(tmp_path / "no_lock.lock"))
        # Should not raise
        scan_lock.remove_scan_lockfile()
