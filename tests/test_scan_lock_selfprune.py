# -*- coding: utf-8 -*-
"""
tests/test_scan_lock_selfprune.py – Root-Cause-Fix gegen scan_lock-Bloat.

Frueher INSERTete acquire_scan_lock pro Scan eine Zeile und release_scan_lock
setzte sie nur auf is_active=0 (loeschte NIE) -> die Tabelle wuchs unbegrenzt und
musste extern bereinigt werden. acquire_scan_lock beschneidet nun die inaktive
Historie automatisch auf die letzten N Eintraege; aktive Locks bleiben unangetastet.
"""
import pytest
from models import SCAN_LOCK_HISTORY_KEEP


class TestAcquireSelfPrunesHistory:

    def test_history_stays_bounded_over_many_cycles(self, in_memory_db):
        db = in_memory_db
        for _ in range(50):
            lid = db.acquire_scan_lock("manual")
            assert lid is not None
            db.release_scan_lock(lid)

        db.cursor.execute("SELECT COUNT(*) FROM scan_lock")
        total = db.cursor.fetchone()[0]
        # Nach dem letzten Zyklus: hoechstens KEEP inaktive (+ keine aktive mehr)
        assert total <= SCAN_LOCK_HISTORY_KEEP + 1, f"scan_lock waechst unbegrenzt: {total}"

    def test_keeps_newest_history_entries(self, in_memory_db):
        db = in_memory_db
        last_ids = []
        for _ in range(SCAN_LOCK_HISTORY_KEEP + 10):
            lid = db.acquire_scan_lock("manual")
            db.release_scan_lock(lid)
            last_ids.append(lid)

        db.cursor.execute("SELECT id FROM scan_lock ORDER BY id DESC LIMIT 1")
        newest_in_db = db.cursor.fetchone()[0]
        assert newest_in_db == last_ids[-1], "neueste Historie muss erhalten bleiben"

    def test_active_lock_is_never_pruned(self, in_memory_db):
        db = in_memory_db
        # viele inaktive erzeugen
        for _ in range(SCAN_LOCK_HISTORY_KEEP + 5):
            lid = db.acquire_scan_lock("manual")
            db.release_scan_lock(lid)
        # jetzt einen aktiven Lock halten und erneut acquiren-Pruning ausloesen
        active = db.acquire_scan_lock("manual")
        assert active is not None
        assert db.is_scan_running() is True
        db.cursor.execute("SELECT COUNT(*) FROM scan_lock WHERE is_active = 1")
        assert db.cursor.fetchone()[0] == 1
