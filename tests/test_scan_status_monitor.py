# -*- coding: utf-8 -*-
"""
tests/test_scan_status_monitor.py – Coverage für den puren Konsolen-Formatter
des Scan-Status-Monitors (bisher ungetestet).
"""
import pytest
from scan_status_monitor import format_scan_info_for_console


def _empty_info(summary="Kein Scan aktiv."):
    return {
        "status_summary": summary,
        "active_scans": [],
        "orphaned_locks": [],
        "recent_scans": [],
    }


class TestFormatScanInfo:

    def test_empty_state_has_header_and_summary(self):
        out = format_scan_info_for_console(_empty_info("Kein Scan aktiv."))
        assert "SCAN STATUS MONITOR" in out
        assert "Kein Scan aktiv." in out
        # Keine Abschnitte ohne Daten
        assert "AKTIVE SCANS" not in out
        assert "VERWAISTE LOCKS" not in out

    def test_active_scan_rendered(self):
        info = _empty_info()
        info["active_scans"] = [{
            "scan_type": "manual", "pid": 1234, "hostname": "PC",
            "start_time": "2026-05-29T05:00:00", "runtime": "00:12:30",
        }]
        out = format_scan_info_for_console(info)
        assert "[AKTIVE SCANS]" in out
        assert "manual" in out and "1234" in out and "PC" in out
        assert "00:12:30" in out

    def test_orphaned_lock_warning_rendered(self):
        info = _empty_info()
        info["orphaned_locks"] = [{
            "lock_id": 7, "scan_type": "scheduled", "pid": 999,
            "start_time": "2026-05-29T01:00:00",
        }]
        out = format_scan_info_for_console(info)
        assert "VERWAISTE LOCKS" in out
        assert "Lock ID: 7" in out
        assert "existiert nicht mehr" in out

    def test_recent_scans_capped_at_five(self):
        info = _empty_info()
        info["recent_scans"] = [
            {"scan_type": f"s{i}", "start_time": f"t{i}"} for i in range(8)
        ]
        out = format_scan_info_for_console(info)
        assert "KÜRZLICHE SCANS" in out
        # nur die ersten 5 werden gelistet
        assert "s0" in out and "s4" in out
        assert "s5" not in out and "s7" not in out

    def test_returns_str(self):
        assert isinstance(format_scan_info_for_console(_empty_info()), str)
