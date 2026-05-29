# -*- coding: utf-8 -*-
"""
tests/test_scanner_core_hardening.py – End-to-End-Nachweis der drei Fixes durch
den ECHTEN Scanner-Walk (scanner_core.run_scan über einen realen Temp-Baum).

Beweist, dass der Gesamtlauf:
  (1) gleichnamige Dateien mit verschiedener Endung NICHT mehr verschluckt,
  (2) aus Mehrpunkt-Dateinamen keine Muell-Endungen mehr anlegt.
"""
import pytest


class TestScannerCoreHardening:

    @pytest.fixture(autouse=True)
    def _patch_db(self, in_memory_db, monkeypatch):
        import models
        monkeypatch.setattr(models, "_db_instance", in_memory_db)
        import scanner_core
        monkeypatch.setattr(scanner_core, "get_db_instance", lambda path=None: in_memory_db)
        self.db = in_memory_db

    def test_same_stem_different_extension_both_indexed(self, tmp_path):
        web = tmp_path / "web"
        web.mkdir()
        (web / "logo.png").write_bytes(b"a")
        (web / "logo.svg").write_bytes(b"bb")
        (web / "index.html").write_text("x", encoding="utf-8")
        (web / "index.css").write_text("y", encoding="utf-8")

        import scanner_core
        assert scanner_core.run_scan(str(tmp_path)) is True

        # Beide 'logo'-Dateien muessen ueberleben (frueher Datenverlust!)
        self.db.cursor.execute("""
            SELECT e.name FROM files f
            JOIN extensions e ON f.extension_id = e.id
            WHERE f.filename = 'logo' ORDER BY e.name
        """)
        assert [r[0] for r in self.db.cursor.fetchall()] == ['.png', '.svg']

        # ebenso die index-Trias
        self.db.cursor.execute("""
            SELECT e.name FROM files f
            JOIN extensions e ON f.extension_id = e.id
            WHERE f.filename = 'index' ORDER BY e.name
        """)
        assert [r[0] for r in self.db.cursor.fetchall()] == ['.css', '.html']

    def test_multidot_names_create_no_junk_extension(self, tmp_path):
        (tmp_path / "backup.2026").write_text("x", encoding="utf-8")
        (tmp_path / "data.1776402205").write_text("y", encoding="utf-8")
        (tmp_path / "real.pdf").write_bytes(b"%PDF")

        import scanner_core
        assert scanner_core.run_scan(str(tmp_path)) is True

        # KEINE Muell-Endung angelegt
        self.db.cursor.execute(
            "SELECT COUNT(*) FROM extensions WHERE name IN ('.2026', '.1776402205')"
        )
        assert self.db.cursor.fetchone()[0] == 0

        # Mehrpunkt-Dateien dennoch indexiert (voller Name im filename, ext '[none]')
        self.db.cursor.execute("SELECT COUNT(*) FROM files WHERE filename = 'backup.2026'")
        assert self.db.cursor.fetchone()[0] == 1
        self.db.cursor.execute("SELECT COUNT(*) FROM files WHERE filename = 'data.1776402205'")
        assert self.db.cursor.fetchone()[0] == 1
        # echte Endung bleibt erhalten
        self.db.cursor.execute(
            "SELECT COUNT(*) FROM files f JOIN extensions e ON f.extension_id=e.id "
            "WHERE f.filename='real' AND e.name='.pdf'"
        )
        assert self.db.cursor.fetchone()[0] == 1
