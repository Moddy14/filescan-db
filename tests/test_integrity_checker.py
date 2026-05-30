# -*- coding: utf-8 -*-
"""
tests/test_integrity_checker.py – Integrationstests für integrity_checker.

Prüft check_integrity() gegen ein echtes temporäres Dateisystem:
fehlende Verzeichnisse/Dateien werden aus der DB entfernt, vorhandene
bleiben erhalten, geänderte Dateigrößen werden aktualisiert.
(Modul vorher 0 % Coverage.)
"""
import os
import pytest


def _drive_of(path):
    return os.path.splitdrive(str(path))[0] + "/"


class TestCheckIntegrity:

    def test_removes_missing_keeps_present(self, in_memory_db, tmp_path):
        db = in_memory_db
        drive = db.get_or_create_drive(_drive_of(tmp_path))

        present_dir = tmp_path / "present"
        present_dir.mkdir()
        (present_dir / "keep.txt").write_text("hello", encoding="utf-8")  # 5 Bytes

        present_id = db.get_or_create_directory(drive, str(present_dir))
        absent_id = db.get_or_create_directory(drive, str(tmp_path / "absent_xyz"))

        db.batch_insert_files([
            (present_id, "keep.txt", 5, None),    # existiert physisch
            (present_id, "ghost.txt", 99, None),  # existiert NICHT
        ])
        db.conn.commit()

        import integrity_checker
        integrity_checker.check_integrity(db)

        # Fehlendes Verzeichnis entfernt, vorhandenes behalten
        db.cursor.execute("SELECT id FROM directories WHERE id = ?", (absent_id,))
        assert db.cursor.fetchone() is None, "fehlendes Verzeichnis muss entfernt werden"
        db.cursor.execute("SELECT id FROM directories WHERE id = ?", (present_id,))
        assert db.cursor.fetchone() is not None, "vorhandenes Verzeichnis muss bleiben"

        # Fehlende Datei entfernt, vorhandene behalten
        db.cursor.execute("SELECT filename FROM files WHERE directory_id = ?", (present_id,))
        names = sorted(r[0] for r in db.cursor.fetchall())
        assert names == ["keep"], f"nur 'keep' darf bleiben, ist aber {names}"

    def test_updates_changed_size(self, in_memory_db, tmp_path):
        db = in_memory_db
        drive = db.get_or_create_drive(_drive_of(tmp_path))

        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "file.bin").write_bytes(b"x" * 100)  # echte Größe 100

        dir_id = db.get_or_create_directory(drive, str(data_dir))
        db.batch_insert_files([(dir_id, "file.bin", 1, None)])  # falsche Größe in DB
        db.conn.commit()

        import integrity_checker
        integrity_checker.check_integrity(db)

        db.cursor.execute(
            "SELECT size FROM files WHERE directory_id = ? AND filename = 'file'", (dir_id,)
        )
        assert db.cursor.fetchone()[0] == 100, "geänderte Dateigröße muss aktualisiert werden"

    def test_empty_db_runs_clean(self, in_memory_db):
        """check_integrity auf leerer DB darf nicht crashen."""
        import integrity_checker
        integrity_checker.check_integrity(in_memory_db)  # darf nicht werfen

    def test_multidot_and_same_stem_files_not_falsely_removed(self, in_memory_db, tmp_path):
        """Regression: nach der Endungs-Haertung darf der Integritaets-Check
        Mehrpunkt-Dateien ([none]) und gleichnamige Dateien mit verschiedener
        Endung NICHT faelschlich als 'fehlend' aus der DB loeschen (Pfad-
        Rekonstruktion full_path/filename(+ext) muss exakt zum Basename passen)."""
        db = in_memory_db
        drive = db.get_or_create_drive(_drive_of(tmp_path))
        d = tmp_path / "mix"
        d.mkdir()
        # reale Dateien auf der Platte
        (d / "backup.2026").write_text("x", encoding="utf-8")          # -> filename='backup.2026', ext='[none]'
        (d / "logo.png").write_bytes(b"a")                              # -> 'logo' + '.png'
        (d / "logo.svg").write_bytes(b"bb")                             # -> 'logo' + '.svg'
        (d / "data.1776402205").write_text("y", encoding="utf-8")       # -> '[none]'

        dir_id = db.get_or_create_directory(drive, str(d))
        db.batch_insert_files([
            (dir_id, "backup.2026", 1, None),
            (dir_id, "logo.png", 1, None),
            (dir_id, "logo.svg", 2, None),
            (dir_id, "data.1776402205", 1, None),
        ])
        db.conn.commit()

        before = db.cursor.execute(
            "SELECT COUNT(*) FROM files WHERE directory_id = ?", (dir_id,)
        ).fetchone()[0]
        assert before == 4, f"Setup: 4 Dateien erwartet, {before}"

        import integrity_checker
        integrity_checker.check_integrity(db)

        after = db.cursor.execute(
            "SELECT COUNT(*) FROM files WHERE directory_id = ?", (dir_id,)
        ).fetchone()[0]
        assert after == 4, (
            f"Integritaets-Check hat real existierende Dateien faelschlich entfernt: "
            f"{before} -> {after} (Pfad-Rekonstruktion fehlerhaft)"
        )
