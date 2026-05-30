# -*- coding: utf-8 -*-
"""
tests/test_watchdog_review_fixes.py – Fixes aus dem Codex-PR-Review.

P1: _reinitialize_db_if_needed muss das DBManager-Singleton zuruecksetzen (close()),
    sonst liefert get_db_instance() die geschlossene Instanz zurueck und der Watchdog
    haengt bis zum Service-Neustart.
P2: Gepufferte Events (waehrend Scan-Lock) muessen auch dann nachgearbeitet werden,
    wenn KEIN weiteres Watched-Event mehr kommt – ueber einen periodischen Hintergrund-
    Drainer (_maybe_drain_pending), nicht nur aus _gate heraus.
"""
import os
import pytest


class FakeEvent:
    def __init__(self, src, is_dir=False):
        self.src_path = src
        self.is_directory = is_dir


class TestP1ReinitResetsSingleton:

    def test_reinit_gets_fresh_working_instance(self, monkeypatch):
        import models
        import watchdog_monitor as wm
        from models import DBManager

        # Echte Singleton-Semantik nachbilden: get_db_instance liefert _db_instance,
        # legt bei None eine NEUE :memory:-Instanz an.
        monkeypatch.setattr(models, "_db_instance", None, raising=False)

        def fake_get(path=None):
            if models._db_instance is None:
                models._db_instance = DBManager(":memory:")
            return models._db_instance

        monkeypatch.setattr(wm, "get_db_instance", fake_get)
        monkeypatch.setattr(wm, "_normalize_path_for_watchdog", lambda p: os.path.normpath(p))

        h = wm.FSHandler("C:/")
        old = h.db
        assert old is not None

        # Verbindung kaputt machen (simuliert abgebrochene DB)
        old.conn.close()

        ok = h._reinitialize_db_if_needed()
        assert ok is True, "Reinit muss erfolgreich wiederherstellen"
        assert h.db is not old, "muss eine FRISCHE Instanz sein (Singleton zurueckgesetzt)"
        # frische Verbindung muss funktionieren
        h.db.cursor.execute("SELECT 1")
        assert h.db.cursor.fetchone()[0] == 1


class TestP2BackgroundDrain:

    def test_pending_drained_by_background_without_new_event(self, in_memory_db, monkeypatch):
        import watchdog_monitor as wm
        monkeypatch.setattr(wm, "get_db_instance", lambda path=None: in_memory_db)
        monkeypatch.setattr(wm, "_normalize_path_for_watchdog", lambda p: os.path.normpath(p))

        state = {"write": False}
        monkeypatch.setattr(wm, "should_fs_write", lambda timeout=0: state["write"])

        h = wm.FSHandler("C:/")
        monkeypatch.setattr(h, "_is_ignored", lambda p: False)

        # Scan-Lock aktiv -> Event wird gepuffert (nicht verarbeitet)
        h.on_created(FakeEvent("C:/lockdir", True))
        assert len(h._pending) == 1
        in_memory_db.cursor.execute("SELECT COUNT(*) FROM directories WHERE full_path='C:/lockdir'")
        assert in_memory_db.cursor.fetchone()[0] == 0

        # Scan vorbei, ABER kein neues Event -> Hintergrund-Drain muss nacharbeiten
        state["write"] = True
        h._maybe_drain_pending()

        assert h._pending == [], "Hintergrund-Drain muss den Puffer leeren"
        in_memory_db.cursor.execute("SELECT COUNT(*) FROM directories WHERE full_path='C:/lockdir'")
        assert in_memory_db.cursor.fetchone()[0] == 1, "gepuffertes Event muss verarbeitet sein"

    def test_maybe_drain_noop_while_locked(self, in_memory_db, monkeypatch):
        import watchdog_monitor as wm
        monkeypatch.setattr(wm, "get_db_instance", lambda path=None: in_memory_db)
        monkeypatch.setattr(wm, "_normalize_path_for_watchdog", lambda p: os.path.normpath(p))
        state = {"write": False}
        monkeypatch.setattr(wm, "should_fs_write", lambda timeout=0: state["write"])
        h = wm.FSHandler("C:/")
        monkeypatch.setattr(h, "_is_ignored", lambda p: False)
        h.on_created(FakeEvent("C:/stilllocked", True))
        # Lock noch aktiv -> Drain darf NICHT verarbeiten
        h._maybe_drain_pending()
        assert len(h._pending) == 1, "bei aktivem Lock nicht drainen"
