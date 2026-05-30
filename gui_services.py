# -*- coding: utf-8 -*-
"""
gui_services.py – Reine Helfer für Dienst-/Prozess-Status und das Parsen von
Subprozess-Ausgaben (ohne PyQt/UI).

Aus gui_launcher.py ausgelagert, damit die Parsing-Logik unabhängig von der
GUI und ohne Qt automatisiert testbar ist.
"""
import json


def parse_integrity_line(line):
    """Klassifiziert eine stdout-Zeile des integrity_checker.

    Returns:
        dict mit Schlüssel 'kind':
          - 'empty'                                  (leere Zeile)
          - 'phase'  + 'phase'                       (@@PHASE:...)
          - 'progress' + 'current'/'total'/'pct'     (@@PROGRESS:c:t)
          - 'result' + 'data'                        (@@RESULT:<json>)
          - 'result_error' + 'raw'                   (@@RESULT mit ungültigem JSON)
          - 'log' + 'text'                           (normale Ausgabe)
    """
    line = line.strip()
    if not line:
        return {"kind": "empty"}

    if line.startswith("@@PHASE:"):
        return {"kind": "phase", "phase": line[8:]}

    if line.startswith("@@PROGRESS:"):
        parts = line[11:].split(":")
        if len(parts) == 2:
            try:
                current = int(parts[0])
                total = int(parts[1])
                pct = int(current / total * 100) if total > 0 else 0
                return {"kind": "progress", "current": current, "total": total, "pct": pct}
            except ValueError:
                return {"kind": "log", "text": line}
        return {"kind": "log", "text": line}

    if line.startswith("@@RESULT:"):
        try:
            return {"kind": "result", "data": json.loads(line[9:])}
        except (ValueError, json.JSONDecodeError):
            return {"kind": "result_error", "raw": line[9:]}

    return {"kind": "log", "text": line}


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
