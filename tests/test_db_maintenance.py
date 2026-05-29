# -*- coding: utf-8 -*-
"""
tests/test_db_maintenance.py – Tests für die DB-Wartung (scan_lock-Historie).

Die scan_lock-Tabelle sammelt historische (inaktive) Lock-Einträge an
(Audit-Bloat). cleanup_scan_lock_history entfernt alte inaktive Einträge,
behält aber alle AKTIVEN Locks und die neuesten N inaktiven.
"""
import pytest


def _insert_inactive_locks(db, n):
    for i in range(n):
        db.cursor.execute(
            "INSERT INTO scan_lock (scan_type, start_time, pid, hostname, is_active) "
            "VALUES (?, ?, ?, ?, 0)",
            ("test", f"2026-01-{i % 28 + 1:02d}T00:00:00", 1000 + i, "host"),
        )
    db.conn.commit()


class TestCleanupScanLockHistory:

    def test_keeps_active_and_recent_inactive(self, in_memory_db):
        db = in_memory_db
        _insert_inactive_locks(db, 10)
        active_id = db.acquire_scan_lock("manual")  # 1 aktiver Lock
        assert db.is_scan_running() is True

        db.cursor.execute("SELECT COUNT(*) FROM scan_lock")
        assert db.cursor.fetchone()[0] == 11

        removed = db.cleanup_scan_lock_history(keep_recent=3)
        assert removed == 7  # 10 inaktive - 3 behalten

        db.cursor.execute("SELECT COUNT(*) FROM scan_lock")
        assert db.cursor.fetchone()[0] == 4   # 3 inaktive + 1 aktiv
        db.cursor.execute("SELECT COUNT(*) FROM scan_lock WHERE is_active = 1")
        assert db.cursor.fetchone()[0] == 1   # aktiver Lock bleibt erhalten

    def test_noop_when_below_keep_threshold(self, in_memory_db):
        db = in_memory_db
        _insert_inactive_locks(db, 2)
        removed = db.cleanup_scan_lock_history(keep_recent=20)
        assert removed == 0
        db.cursor.execute("SELECT COUNT(*) FROM scan_lock")
        assert db.cursor.fetchone()[0] == 2

    def test_never_deletes_active_locks(self, in_memory_db):
        db = in_memory_db
        db.acquire_scan_lock("manual")
        db.cleanup_scan_lock_history(keep_recent=0)  # aggressivste Bereinigung
        db.cursor.execute("SELECT COUNT(*) FROM scan_lock WHERE is_active = 1")
        assert db.cursor.fetchone()[0] == 1
