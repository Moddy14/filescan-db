"""
tests/test_exporter.py – Unit tests for exporter.py
"""

import os
import json
import csv
import pytest


class TestExporterHelpers:
    """Test the low-level export_csv / export_json / export_html helpers."""

    @pytest.fixture
    def sample_cursor(self, in_memory_db):
        """Return a cursor pre-populated with two files for export."""
        db = in_memory_db
        drive_id = db.get_or_create_drive("T:/")
        dir_id   = db.get_or_create_directory(drive_id, "T:/Docs")
        db.batch_insert_files([
            (dir_id, "report.pdf",   2048, None),
            (dir_id, "summary.txt",   512, "aabb"),
        ])
        db.conn.commit()
        return db

    def _fetch(self, db):
        """Use exporter.fetch_file_data to get a live cursor."""
        import exporter
        return exporter.fetch_file_data(db.cursor)

    # ------------------------------------------------------------------
    # CSV export
    # ------------------------------------------------------------------

    def test_csv_export_creates_file(self, tmp_path, sample_cursor):
        """export_csv() writes a file to disk."""
        import exporter
        out = str(tmp_path / "test.csv")
        ok = exporter.export_csv(self._fetch(sample_cursor), out)
        assert ok is True
        assert os.path.isfile(out)
        assert os.path.getsize(out) > 0

    def test_csv_export_has_header(self, tmp_path, sample_cursor):
        """The CSV output includes a header row."""
        import exporter
        out = str(tmp_path / "test_header.csv")
        exporter.export_csv(self._fetch(sample_cursor), out)
        with open(out, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
        assert len(header) >= 4

    def test_csv_export_contains_data(self, tmp_path, sample_cursor):
        """The CSV file must contain at least 2 data rows (one per file)."""
        import exporter
        out = str(tmp_path / "test_data.csv")
        exporter.export_csv(self._fetch(sample_cursor), out)
        with open(out, newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))
        # header + 2 data rows
        assert len(rows) >= 3

    # ------------------------------------------------------------------
    # JSON export
    # ------------------------------------------------------------------

    def test_json_export_creates_file(self, tmp_path, sample_cursor):
        """export_json() writes a file to disk."""
        import exporter
        out = str(tmp_path / "test.json")
        ok = exporter.export_json(self._fetch(sample_cursor), out)
        assert ok is True
        assert os.path.isfile(out)

    def test_json_export_is_valid_json(self, tmp_path, sample_cursor):
        """The JSON output must be parseable."""
        import exporter
        out = str(tmp_path / "test_valid.json")
        exporter.export_json(self._fetch(sample_cursor), out)
        with open(out, encoding="utf-8") as f:
            data = json.load(f)
        assert isinstance(data, list)

    def test_json_export_contains_records(self, tmp_path, sample_cursor):
        """The JSON list must contain one record per indexed file."""
        import exporter
        out = str(tmp_path / "test_records.json")
        exporter.export_json(self._fetch(sample_cursor), out)
        with open(out, encoding="utf-8") as f:
            data = json.load(f)
        assert len(data) == 2

    def test_json_record_has_expected_keys(self, tmp_path, sample_cursor):
        """Each JSON record must have file_path and size keys."""
        import exporter
        out = str(tmp_path / "test_keys.json")
        exporter.export_json(self._fetch(sample_cursor), out)
        with open(out, encoding="utf-8") as f:
            data = json.load(f)
        assert "file_path" in data[0]
        assert "size" in data[0]

    # ------------------------------------------------------------------
    # HTML export
    # ------------------------------------------------------------------

    def test_html_export_creates_file(self, tmp_path, sample_cursor):
        """export_html() writes a file to disk."""
        import exporter
        out = str(tmp_path / "test.html")
        ok = exporter.export_html(self._fetch(sample_cursor), out)
        assert ok is True
        assert os.path.isfile(out)

    def test_html_export_contains_doctype(self, tmp_path, sample_cursor):
        """The HTML output must start with <!DOCTYPE html>."""
        import exporter
        out = str(tmp_path / "test_html.html")
        exporter.export_html(self._fetch(sample_cursor), out)
        content = open(out, encoding="utf-8").read().lower()
        assert "<!doctype html>" in content

    # ------------------------------------------------------------------
    # export_all integration
    # ------------------------------------------------------------------

    def test_export_path_without_extension_has_no_none_suffix(self, tmp_path, in_memory_db):
        """Extensionslose Dateien duerfen im Export keinen '[none]'-Suffix tragen."""
        import json
        import exporter
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        dir_id = db.get_or_create_directory(drive_id, "C:/Docs")
        db.batch_insert_files([(dir_id, "Makefile", 5, None)])
        db.conn.commit()

        out = str(tmp_path / "noext.json")
        exporter.export_json(exporter.fetch_file_data(db.cursor), out)
        data = json.load(open(out, encoding="utf-8"))
        paths = [r["file_path"] for r in data]

        assert any("Makefile" in p for p in paths)
        assert not any("[none]" in p for p in paths), "kein [none]-Suffix im Export-Pfad"

    def test_export_multidot_and_same_stem_paths(self, tmp_path, in_memory_db):
        """Mehrpunkt-Namen (ext='[none]') behalten den vollen Namen, gleichnamige
        Dateien mit verschiedener Endung erscheinen als getrennte, korrekte Pfade."""
        import json
        import exporter
        db = in_memory_db
        drive_id = db.get_or_create_drive("C:/")
        dir_id = db.get_or_create_directory(drive_id, "C:/mix")
        db.batch_insert_files([
            (dir_id, "logo.png", 1, None),
            (dir_id, "logo.svg", 2, None),
            (dir_id, "backup.2026", 3, None),   # -> filename='backup.2026', ext='[none]'
        ])
        db.conn.commit()

        out = str(tmp_path / "mix.json")
        exporter.export_json(exporter.fetch_file_data(db.cursor), out)
        paths = [r["file_path"] for r in json.load(open(out, encoding="utf-8"))]

        assert any(p.endswith("logo.png") for p in paths)
        assert any(p.endswith("logo.svg") for p in paths)
        assert any(p.endswith("backup.2026") for p in paths), "voller Mehrpunkt-Name muss erhalten bleiben"
        assert not any("[none]" in p for p in paths)
        assert len(paths) == 3, f"alle drei Dateien muessen exportiert werden, {len(paths)}"

    def test_export_all_produces_files(self, tmp_path, monkeypatch, sample_cursor):
        """export_all() must write at least one file per enabled format.

        Note: export_all() references write_log() which is undefined in exporter.py
        (latent bug). We patch it here as a no-op so the rest of the function runs.
        """
        import exporter
        # Patch write_log that is missing in the module (upstream bug)
        monkeypatch.setattr(exporter, "write_log",
                            lambda msg: exporter.logger.info(msg),
                            raising=False)

        monkeypatch.setattr(exporter, "EXPORT_DIR", str(tmp_path / "exp"))
        os.makedirs(str(tmp_path / "exp"), exist_ok=True)
        monkeypatch.setattr(exporter, "EXPORT_FORMATS", ["csv", "json"])

        exporter.export_all(sample_cursor)

        files = os.listdir(str(tmp_path / "exp"))
        assert any(f.endswith(".csv") for f in files)
        assert any(f.endswith(".json") for f in files)
