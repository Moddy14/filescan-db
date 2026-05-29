# -*- coding: utf-8 -*-
"""
tests/test_concurrency.py – Thread-Safety-Tests für den DBManager.

Der DBManager nutzt eine geteilte sqlite3-Connection (check_same_thread=False)
und serialisiert Zugriffe über einen globalen RLock (@with_lock). Diese Tests
verifizieren, dass parallele Schreib-/Leseoperationen aus mehreren Threads
korrekt und ohne Fehler/Datenverlust ablaufen.
"""
import threading
import pytest


class TestConcurrentAccess:

    def test_concurrent_inserts_are_consistent(self, in_memory_db):
        db = in_memory_db
        drive = db.get_or_create_drive("C:/")
        d = db.get_or_create_directory(drive, "C:/data")

        errors = []

        def worker(n):
            try:
                for i in range(20):
                    db.insert_file_optimized(d, f"f_{n}_{i}.txt", i, None)
            except Exception as e:  # noqa: BLE001 - Testdiagnose
                errors.append(repr(e))

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        db.conn.commit()

        assert not errors, f"Thread-Fehler aufgetreten: {errors}"
        db.cursor.execute("SELECT COUNT(*) FROM files WHERE directory_id = ?", (d,))
        assert db.cursor.fetchone()[0] == 100, "5 Threads x 20 Dateien = 100 erwartet"

    def test_concurrent_drive_creation_idempotent(self, in_memory_db):
        """Mehrere Threads, die dasselbe Laufwerk anlegen, dürfen keine
        Duplikate erzeugen (UNIQUE + Race-Handling)."""
        db = in_memory_db
        ids = []
        errors = []

        def worker():
            try:
                ids.append(db.get_or_create_drive("Z:/"))
            except Exception as e:  # noqa: BLE001
                errors.append(repr(e))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Thread-Fehler: {errors}"
        assert len(set(ids)) == 1, f"alle Threads müssen dieselbe drive_id sehen: {ids}"
        db.cursor.execute("SELECT COUNT(*) FROM drives WHERE name = 'Z:/'")
        assert db.cursor.fetchone()[0] == 1, "kein Duplikat-Laufwerk"
