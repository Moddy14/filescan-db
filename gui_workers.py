# -*- coding: utf-8 -*-
"""
gui_workers.py – Worker-Threads (QThread) für die PyQt-GUI.

Aus gui_launcher.py ausgelagert, um den Monolithen zu verkleinern und die
Worker unabhängig von der restlichen GUI testbar/wartbar zu halten.
"""
import os

from PyQt5 import QtCore


class LogUpdater(QtCore.QThread):
    """Liest eine Logdatei inkrementell und sendet neue Zeilen per Signal."""
    log_updated = QtCore.pyqtSignal(str)

    def __init__(self, log_file, parent=None):
        super().__init__(parent)
        self.log_file = log_file
        self._running = True
        self._last_size = 0

    def run(self):
        while self._running:
            try:
                if os.path.exists(self.log_file):
                    with open(self.log_file, "r", encoding="utf-8") as f:
                        f.seek(self._last_size)
                        new_data = f.read()
                        if new_data:
                            self.log_updated.emit(new_data)
                            self._last_size = f.tell()
            except Exception as e:
                self.log_updated.emit(f"[Log-Fehler] {e}\n")
            self.msleep(1000)

    def stop(self):
        self._running = False


class ScanWorker(QtCore.QThread):
    """Führt den Scan-Vorgang in einem separaten Thread aus."""
    scan_progress = QtCore.pyqtSignal(str)        # Fortschrittsmeldungen
    scan_finished = QtCore.pyqtSignal(bool, str)  # (Erfolg, Nachricht)

    def __init__(self, base_path, parent=None):
        super().__init__(parent)
        self.base_path = base_path
        self._running = True

    def run(self):
        # Aktuell ungenutzt (Scans laufen über QProcess in MainWindow).
        # Platzhalter beibehalten, damit bestehende Referenzen stabil bleiben.
        pass

    def stop(self):
        """Signalisiert dem Thread, dass er beendet werden soll."""
        self._running = False
