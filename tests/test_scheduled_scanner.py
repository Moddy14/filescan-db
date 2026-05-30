# -*- coding: utf-8 -*-
"""
tests/test_scheduled_scanner.py – Tests für die Scheduler-Kernlogik
(Zeit-Parsing, Scan-Schlüssel, Last-Run-Tracking, Fälligkeits-Prüfung).

Wichtig: Das echte .scheduled_last_runs.json (vom laufenden Scheduler genutzt)
wird NIE angefasst – _LAST_RUN_FILE wird in jedem Test auf eine Temp-Datei
umgebogen.
"""
import datetime
import json
import pytest


class TestParseTime:
    def test_valid(self):
        from scheduled_scanner import _parse_time
        assert _parse_time("06:30") == (6, 30)

    def test_invalid_string(self):
        from scheduled_scanner import _parse_time
        assert _parse_time("not-a-time") is None

    def test_empty(self):
        from scheduled_scanner import _parse_time
        assert _parse_time("") is None


class TestParseTimeRanges:
    """_parse_time muss ausserhalb gueltiger Bereiche None liefern, sonst crasht
    should_scan_run_now via datetime.replace(hour=24)."""

    @pytest.mark.parametrize("bad", ["24:00", "12:60", "-1:30", "12:-5", "99:99"])
    def test_out_of_range_is_none(self, bad):
        from scheduled_scanner import _parse_time
        assert _parse_time(bad) is None

    @pytest.mark.parametrize("good,exp", [("00:00", (0, 0)), ("23:59", (23, 59)), ("09:05", (9, 5))])
    def test_valid_bounds(self, good, exp):
        from scheduled_scanner import _parse_time
        assert _parse_time(good) == exp


class TestShouldScanRunNowInvalidTime:
    def test_invalid_time_returns_false_without_crash(self, tmp_path, monkeypatch):
        import scheduled_scanner as ss
        monkeypatch.setattr(ss, "_LAST_RUN_FILE", str(tmp_path / "lr.json"))
        cfg = {"scan_type": "drive", "path": "C:/", "time": "25:00"}
        # darf NICHT mit ValueError (replace(hour=25)) abstuerzen
        assert ss.should_scan_run_now(cfg) is False


class TestScanKey:
    def test_stable_for_same_config(self):
        from scheduled_scanner import _scan_key
        cfg = {"scan_type": "drive", "path": "C:/", "time": "05:00"}
        assert _scan_key(cfg) == _scan_key(dict(cfg))

    def test_distinct_for_different_configs(self):
        from scheduled_scanner import _scan_key
        a = _scan_key({"scan_type": "drive", "path": "C:/", "time": "05:00"})
        b = _scan_key({"scan_type": "integrity", "path": "C:/", "time": "05:00"})
        assert a != b


class TestLastRunsRoundTrip:
    def test_save_then_load(self, tmp_path, monkeypatch):
        import scheduled_scanner as ss
        monkeypatch.setattr(ss, "_LAST_RUN_FILE", str(tmp_path / "lr.json"))
        ss._save_last_runs({"drive_C:/_05:00": "2026-05-29"})
        assert ss._load_last_runs() == {"drive_C:/_05:00": "2026-05-29"}

    def test_load_missing_returns_empty(self, tmp_path, monkeypatch):
        import scheduled_scanner as ss
        monkeypatch.setattr(ss, "_LAST_RUN_FILE", str(tmp_path / "does_not_exist.json"))
        assert ss._load_last_runs() == {}


class TestShouldScanRunNow:
    def _cfg_at(self, dt, **extra):
        cfg = {"scan_type": "drive", "path": "C:/", "time": dt.strftime("%H:%M")}
        cfg.update(extra)
        return cfg

    def test_runs_inside_window(self, tmp_path, monkeypatch):
        import scheduled_scanner as ss
        monkeypatch.setattr(ss, "_LAST_RUN_FILE", str(tmp_path / "lr.json"))
        cfg = self._cfg_at(datetime.datetime.now())  # geplante Zeit = jetzt
        assert ss.should_scan_run_now(cfg) is True

    def test_disabled_never_runs(self, tmp_path, monkeypatch):
        import scheduled_scanner as ss
        monkeypatch.setattr(ss, "_LAST_RUN_FILE", str(tmp_path / "lr.json"))
        cfg = self._cfg_at(datetime.datetime.now(), enabled=False)
        assert ss.should_scan_run_now(cfg) is False

    def test_already_run_today_skips(self, tmp_path, monkeypatch):
        import scheduled_scanner as ss
        monkeypatch.setattr(ss, "_LAST_RUN_FILE", str(tmp_path / "lr.json"))
        cfg = self._cfg_at(datetime.datetime.now())
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        ss._save_last_runs({ss._scan_key(cfg): today})
        assert ss.should_scan_run_now(cfg) is False

    def test_outside_window_does_not_run(self, tmp_path, monkeypatch):
        import scheduled_scanner as ss
        monkeypatch.setattr(ss, "_LAST_RUN_FILE", str(tmp_path / "lr.json"))
        far = datetime.datetime.now() + datetime.timedelta(hours=2)
        cfg = self._cfg_at(far)
        assert ss.should_scan_run_now(cfg) is False
