# -*- coding: utf-8 -*-
"""
tests/test_gui_services.py – Tests für die Dienst-Status-Parserlogik
(gui_services.parse_service_state), ausgelagert aus der GUI.
"""
import pytest

from gui_services import parse_service_state


@pytest.mark.parametrize("state_token,expected", [
    ("RUNNING", "RUNNING"),
    ("STOPPED", "STOPPED"),
    ("PAUSED", "PAUSED"),
    ("START_PENDING", "START_PENDING"),
    ("STOP_PENDING", "STOP_PENDING"),
    ("CONTINUE_PENDING", "CONTINUE_PENDING"),
    ("PAUSE_PENDING", "PAUSE_PENDING"),
])
def test_parse_known_states(state_token, expected):
    sc_output = f"SERVICE_NAME: X\n        STATE              : 4  {state_token}\n"
    assert parse_service_state(sc_output.lower()) == expected


def test_stop_pending_not_confused_with_stopped():
    out = "        STATE              : 3  STOP_PENDING\n"
    assert parse_service_state(out.lower()) == "STOP_PENDING"


def test_no_state_line_returns_unknown():
    assert parse_service_state("SERVICE_NAME: X\nTYPE: 10 WIN32\n") == "UNKNOWN"

def test_empty_output_returns_unknown():
    assert parse_service_state("") == "UNKNOWN"


from gui_services import parse_integrity_line


class TestParseIntegrityLine:

    def test_phase(self):
        assert parse_integrity_line("@@PHASE:dirs") == {"kind": "phase", "phase": "dirs"}

    def test_progress(self):
        r = parse_integrity_line("@@PROGRESS:50:200")
        assert r == {"kind": "progress", "current": 50, "total": 200, "pct": 25}

    def test_progress_zero_total(self):
        r = parse_integrity_line("@@PROGRESS:0:0")
        assert r["kind"] == "progress" and r["pct"] == 0

    def test_result_valid_json(self):
        r = parse_integrity_line('@@RESULT:{"missing_files": 3}')
        assert r["kind"] == "result" and r["data"]["missing_files"] == 3

    def test_result_invalid_json(self):
        r = parse_integrity_line("@@RESULT:not-json")
        assert r["kind"] == "result_error"

    def test_plain_log_line(self):
        assert parse_integrity_line("irgendeine Meldung") == {"kind": "log", "text": "irgendeine Meldung"}

    def test_empty_line(self):
        assert parse_integrity_line("   ") == {"kind": "empty"}
