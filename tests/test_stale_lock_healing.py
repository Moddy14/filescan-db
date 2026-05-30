# -*- coding: utf-8 -*-
"""
tests/test_stale_lock_healing.py – Selbstheilung verwaister scan_lock-Eintraege.

Wird ein Scanner gekillt/stuerzt ab, bleibt sein scan_lock is_active=1 verwaist.
scan_all_drives nutzt is_scan_running() OHNE Heilung -> ueberspringt danach ALLE
Laufwerke (im Deployment live beobachtet). heal_stale_scan_locks() gibt Locks frei,
deren Prozess auf DIESEM Host nicht mehr existiert; Locks lebender Prozesse bleiben.
"""
import os
import socket
import pytest


def _insert_lock(db, pid, host=None, active=1):
    host = host if host is not None else socket.gethostname()
    db.cursor.execute(
        "INSERT INTO scan_lock (scan_type, start_time, pid, hostname, is_active) "
        "VALUES ('manual', '2026-05-30T00:00:00', ?, ?, ?)",
        (pid, host, active),
    )
    db.conn.commit()


class TestHealStaleScanLocks:

    def test_releases_lock_of_dead_pid(self, in_memory_db):
        db = in_memory_db
        _insert_lock(db, pid=999999)  # PID existiert praktisch sicher nicht
        assert db.is_scan_running() is True
        healed = db.heal_stale_scan_locks()
        assert healed == 1
        assert db.is_scan_running() is False

    def test_keeps_lock_of_live_pid(self, in_memory_db):
        db = in_memory_db
        _insert_lock(db, pid=os.getpid())  # dieser Test-Prozess lebt
        healed = db.heal_stale_scan_locks()
        assert healed == 0
        assert db.is_scan_running() is True

    def test_keeps_foreign_host_lock(self, in_memory_db):
        db = in_memory_db
        _insert_lock(db, pid=999999, host="ein-anderer-host")  # PID auf fremdem Host nicht pruefbar
        healed = db.heal_stale_scan_locks()
        assert healed == 0
        assert db.is_scan_running() is True

    def test_noop_when_no_active_locks(self, in_memory_db):
        db = in_memory_db
        assert db.heal_stale_scan_locks() == 0
