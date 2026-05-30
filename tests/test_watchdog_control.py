# -*- coding: utf-8 -*-
"""
tests/test_watchdog_control.py – Tests für die Watchdog-Prozesssteuerung.

WICHTIG: Alle Prozess-/Dienst-Operationen werden gemockt – es wird NIEMALS ein
echter Watchdog-Prozess gesucht, gestoppt oder gestartet.
"""
import pytest


class _FakeProc:
    def __init__(self, pid, cmdline):
        self.info = {"pid": pid, "name": "python.exe", "cmdline": cmdline}


class TestFindWatchdogPid:

    def test_finds_python_process(self, monkeypatch):
        import watchdog_control
        procs = [
            _FakeProc(111, ["python.exe", "other.py"]),
            _FakeProc(222, ["pythonw.exe", "C:/x/watchdog_service.py"]),
        ]
        monkeypatch.setattr(watchdog_control.psutil, "process_iter",
                            lambda attrs=None: iter(procs))
        assert watchdog_control.find_watchdog_pid() == 222

    def test_returns_none_when_absent(self, monkeypatch):
        import watchdog_control
        monkeypatch.setattr(watchdog_control.psutil, "process_iter",
                            lambda attrs=None: iter([]))
        monkeypatch.setattr(watchdog_control, "_is_nssm_service_running", lambda: False)
        assert watchdog_control.find_watchdog_pid() is None

    def test_returns_minus_one_for_nssm_service(self, monkeypatch):
        import watchdog_control
        monkeypatch.setattr(watchdog_control.psutil, "process_iter",
                            lambda attrs=None: iter([]))
        monkeypatch.setattr(watchdog_control, "_is_nssm_service_running", lambda: True)
        assert watchdog_control.find_watchdog_pid() == -1


class TestPauseWatchdogForScan:

    def test_stops_runs_restarts_when_running(self, monkeypatch):
        import watchdog_control
        calls = []
        monkeypatch.setattr(watchdog_control, "find_watchdog_pid", lambda: 999)
        monkeypatch.setattr(watchdog_control, "stop_watchdog", lambda: calls.append("stop"))
        monkeypatch.setattr(watchdog_control, "start_watchdog", lambda: calls.append("start"))
        monkeypatch.setattr(watchdog_control.time, "sleep", lambda s: None)

        result = watchdog_control.pause_watchdog_for_scan(
            lambda: (calls.append("scan"), "RESULT")[1]
        )

        assert result == "RESULT"
        assert calls == ["stop", "scan", "start"]

    def test_runs_scan_without_restart_when_not_running(self, monkeypatch):
        import watchdog_control
        calls = []
        monkeypatch.setattr(watchdog_control, "find_watchdog_pid", lambda: None)
        monkeypatch.setattr(watchdog_control, "stop_watchdog", lambda: calls.append("stop"))
        monkeypatch.setattr(watchdog_control, "start_watchdog", lambda: calls.append("start"))
        monkeypatch.setattr(watchdog_control.time, "sleep", lambda s: None)

        result = watchdog_control.pause_watchdog_for_scan(lambda: "DONE")

        assert result == "DONE"
        assert "stop" not in calls and "start" not in calls  # kein Eingriff noetig
