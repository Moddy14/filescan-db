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


class TestCleanupOrphanExtensions:

    def test_removes_orphan_junk_keeps_used_and_standard(self, in_memory_db):
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        d = db.get_or_create_directory(drive, "C:/D")
        db.batch_insert_files([(d, "a.py", 1, None)])  # .py wird genutzt
        db.conn.commit()

        # verwaiste, automatisch angelegte Extension (KEIN mime_type)
        db.cursor.execute(
            "INSERT INTO extensions (name, category, is_binary) VALUES ('.junk123', 'other', 0)"
        )
        db.conn.commit()

        removed = db.cleanup_orphan_extensions()
        assert removed >= 1

        def count(name):
            db.cursor.execute("SELECT COUNT(*) FROM extensions WHERE name = ?", (name,))
            return db.cursor.fetchone()[0]

        assert count(".junk123") == 0, "verwaiste Müll-Extension muss entfernt werden"
        assert count(".py") == 1, "genutzte Extension muss bleiben"
        assert count(".pdf") == 1, "Standard-Extension (mit mime_type) muss bleiben (auch ungenutzt)"

    def test_does_not_break_fk(self, in_memory_db):
        """Nach der Bereinigung dürfen keine files auf gelöschte Extensions zeigen."""
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        d = db.get_or_create_directory(drive, "C:/D")
        db.batch_insert_files([(d, "x.zip", 1, None), (d, "y.weirdext999", 2, None)])
        db.conn.commit()
        db.cleanup_orphan_extensions()
        # Jede Datei muss weiterhin eine gültige extension_id haben
        db.cursor.execute(
            "SELECT COUNT(*) FROM files f LEFT JOIN extensions e ON f.extension_id = e.id "
            "WHERE f.extension_id IS NOT NULL AND e.id IS NULL"
        )
        assert db.cursor.fetchone()[0] == 0, "keine verwaisten extension_id-Verweise"
