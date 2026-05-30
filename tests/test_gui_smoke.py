# -*- coding: utf-8 -*-
"""
tests/test_gui_smoke.py – Import-Smoke-Test für die PyQt-GUI.

Verifiziert, dass gui_launcher (inkl. der Auslagerung nach gui_data) ohne
Syntax-, Namens- oder Importfehler geladen werden kann. Es wird KEINE
QApplication erzeugt (nur Modul-Import / Klassendefinitionen), damit der Test
headless und ohne Fenster läuft.
"""
import pytest


def test_gui_launcher_imports_clean():
    import gui_launcher
    # Kernklassen müssen definiert sein
    assert hasattr(gui_launcher, "MainWindow")
    assert hasattr(gui_launcher, "ScanWorker")
    assert hasattr(gui_launcher, "LogUpdater")
    # Der ausgelagerte Daten-Layer muss eingebunden sein
    assert hasattr(gui_launcher, "gui_data")


def test_gui_data_is_used_for_overview():
    """gui_launcher verwendet den ausgelagerten Daten-Layer (kein Rückfall
    auf den geteilten db.cursor in start_scan)."""
    import inspect
    import gui_launcher

    src = inspect.getsource(gui_launcher.MainWindow.start_scan)
    assert "gui_data.get_drive_overview" in src
    # die alten Direktzugriffe dürfen in start_scan nicht mehr vorkommen
    assert "SELECT id FROM drives WHERE name" not in src
