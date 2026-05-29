# -*- coding: utf-8 -*-
"""
tests/conftest.py – Gemeinsame Pytest-Fixtures und Test-Isolation.

Stellt vollständig isolierte In-Memory-Datenbanken sowie einen temporären
Beispiel-Dateibaum bereit.

Sicherheitsgarantien:
- Es wird NIEMALS auf die produktive Live-Datenbank (utils.DB_PATH /
  Dateien.db) zugegriffen. Die Fixture instanziiert den DBManager ausdrücklich
  mit ':memory:' und verwendet NICHT das globale Singleton mit Default-Pfad.
- Das Logging wird in eine wegwerfbare Temp-Datei umgeleitet, damit ein
  parallel laufender Watchdog-/Scanner-Prozess die echte scanner.log nicht
  mit den Tests teilen muss.
"""
import os
import sys
import logging
import tempfile

import pytest

# --- Importpfad: scanner_portable/ (Parent dieses tests/-Ordners) ----------
_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_TESTS_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)


# --- Logging vom produktiven scanner.log wegleiten ------------------------
# utils.py konfiguriert beim Import den Root-Logger auf PROJECT_DIR/scanner.log.
# Diese Datei wird ggf. von einem laufenden Watchdog-Service genutzt – daher
# leiten wir die Test-Logs in eine wegwerfbare Temp-Datei um.
import utils  # noqa: E402  (Import mit beabsichtigtem Seiteneffekt)


def _isolate_logging():
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass
    tmp_log = os.path.join(tempfile.gettempdir(), "filescan_db_pytest.log")
    file_handler = logging.FileHandler(tmp_log, mode="w", encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter("%(levelname)s [%(filename)s] %(message)s")
    )
    root.addHandler(file_handler)
    root.setLevel(logging.WARNING)


_isolate_logging()


@pytest.fixture
def in_memory_db():
    """Frische, vollständig isolierte In-Memory-DBManager-Instanz.

    Verwendet ausdrücklich ':memory:' und NICHT das globale Singleton mit
    Default-Pfad – die produktive Dateien.db wird damit garantiert nie
    geöffnet. Die Verbindung wird nach jedem Test geschlossen.
    """
    from models import DBManager

    db = DBManager(":memory:")
    try:
        yield db
    finally:
        try:
            db.conn.close()
        except Exception:
            pass


@pytest.fixture
def sample_file_tree(tmp_path):
    """Temporärer Beispiel-Verzeichnisbaum mit genau 6 Dateien.

    Layout (Wurzel + Unterverzeichnisse, inkl. zweiter Ebene)::

        sample_drive/
            readme.txt
            docs/        report.pdf, notes.md
            images/      photo.jpg, icon.png
            code/src/    main.py
            empty_dir/

    Ergibt >=5 Verzeichnisse und genau 6 Dateien – passend zu den
    Erwartungen in tests/test_scanner_core.py.
    """
    root = tmp_path / "sample_drive"
    docs = root / "docs"
    images = root / "images"
    src = root / "code" / "src"
    empty = root / "empty_dir"
    for directory in (root, docs, images, src, empty):
        directory.mkdir(parents=True, exist_ok=True)

    (root / "readme.txt").write_text("hello filescan", encoding="utf-8")
    (docs / "report.pdf").write_bytes(b"%PDF-1.4 sample")
    (docs / "notes.md").write_text("# notes\n", encoding="utf-8")
    (images / "photo.jpg").write_bytes(b"\xff\xd8\xff\xe0JFIF-sample")
    (images / "icon.png").write_bytes(b"\x89PNG\r\n\x1a\n-sample")
    (src / "main.py").write_text("print('hi')\n", encoding="utf-8")

    return root
