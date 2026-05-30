# -*- coding: utf-8 -*-
"""
tests/test_scanner_reparse.py – Schutz gegen Windows-Junction-Endlosschleifen.

Zyklische Junctions (z.B. AppData\\Local\\Anwendungsdaten -> AppData\\Local,
"Documents and Settings" -> Users) treiben os.walk sonst in Endlos-Rekursion und
erzeugen hunderttausende Phantom-Verzeichnisse. scanner_core darf Reparse-Points
(Junctions/Symlinks) NICHT betreten.
"""
import os
import subprocess
import pytest


def _make_junction(link, target):
    """Erzeugt eine Windows-Junction (kein Admin noetig). True bei Erfolg."""
    try:
        r = subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(target)],
                           capture_output=True, text=True)
        return r.returncode == 0 and os.path.exists(str(link))
    except Exception:
        return False


class TestReparsePointDetection:

    def test_detects_junction_but_not_normal_dir(self, tmp_path):
        from scanner_core import _is_reparse_point
        real = tmp_path / "real"
        real.mkdir()
        link = tmp_path / "jlink"
        if not _make_junction(link, real):
            pytest.skip("mklink /J nicht moeglich (kein Windows / keine Rechte)")
        assert _is_reparse_point(str(real)) is False
        assert _is_reparse_point(str(link)) is True

    def test_missing_path_is_false(self):
        from scanner_core import _is_reparse_point
        assert _is_reparse_point(r"C:\does\not\exist_xyz_123") is False


class TestScanSkipsJunctions:

    @pytest.fixture(autouse=True)
    def _patch_db(self, in_memory_db, monkeypatch):
        import models
        monkeypatch.setattr(models, "_db_instance", in_memory_db)
        import scanner_core
        monkeypatch.setattr(scanner_core, "get_db_instance", lambda path=None: in_memory_db)
        self.db = in_memory_db

    def test_junction_is_not_descended(self, tmp_path):
        import scanner_core
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "a.txt").write_text("x", encoding="utf-8")
        link = tmp_path / "jlink"
        if not _make_junction(link, sub):
            pytest.skip("mklink /J nicht moeglich")

        scanner_core.run_scan(str(tmp_path))

        # Die Junction darf NICHT betreten/indexiert werden
        self.db.cursor.execute(
            "SELECT COUNT(*) FROM directories WHERE full_path LIKE '%jlink%'"
        )
        assert self.db.cursor.fetchone()[0] == 0, "Junction wurde betreten (Endlos-Risiko)"
