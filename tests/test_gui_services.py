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
