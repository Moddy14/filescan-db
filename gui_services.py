# -*- coding: utf-8 -*-
"""
gui_services.py – Reine Helfer für Dienst-/Prozess-Status (ohne PyQt/UI).

Aus gui_launcher.py ausgelagert, damit die Parsing-Logik unabhängig von der
GUI und ohne Qt automatisiert testbar ist.
"""


def parse_service_state(output):
    """Parst die Ausgabe von 'sc query <service>' in einen Status-String.

    Args:
        output: Die (idealerweise bereits lowercase) stdout-Ausgabe von sc query.

    Returns:
        Einen der Status-Strings: RUNNING, PAUSED, STOPPED, START_PENDING,
        STOP_PENDING, CONTINUE_PENDING, PAUSE_PENDING oder UNKNOWN.

    Hinweis: Der Sonderfall „pausiert, aber Watchdog läuft als Prozess"
    (RUNNING_AS_PROCESS) wird bewusst vom Aufrufer behandelt, da er einen
    Prozess-Check benötigt.
    """
    for line in output.splitlines():
        if "state" in line.lower():
            s = line.lower()
            if "running" in s:
                return "RUNNING"
            if "paused" in s:
                return "PAUSED"
            if "stopped" in s:
                return "STOPPED"
            if "start_pending" in s:
                return "START_PENDING"
            if "stop_pending" in s:
                return "STOP_PENDING"
            if "continue_pending" in s:
                return "CONTINUE_PENDING"
            if "pause_pending" in s:
                return "PAUSE_PENDING"
            return "UNKNOWN"
    return "UNKNOWN"
