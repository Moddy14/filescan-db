# -*- coding: utf-8 -*-
"""
dupe_finder.py – Gemeinsame, testbare Duplikat-Such-Engine.

Wiederverwendbarer Kern für die Duplikat-Tools in Dateien_Skripte/ (bisher mit
eigener, dupliziter Logik). Arbeitet read-only über DBManager.read_query, sodass
die Suche den Schreib-Lock von Scanner/Watchdog nicht blockiert.
"""


def find_hash_duplicates(db, min_count=2):
    """Findet Gruppen von Dateien mit identischem (nicht-leerem) SHA256-Hash.

    Returns:
        Liste von (hash, count) für Hashes, die mindestens ``min_count`` mal
        vorkommen, absteigend nach count.
    """
    return db.read_query(
        """
        SELECT hash, COUNT(*) AS c
        FROM files
        WHERE hash IS NOT NULL AND hash != ''
        GROUP BY hash
        HAVING c >= ?
        ORDER BY c DESC
        """,
        (min_count,),
    )


def find_duplicate_files(db, method="name_size", limit=1000):
    """Liefert Detail-Zeilen der Duplikat-Gruppen für die Anzeige in Tools.

    Args:
        db: DBManager-Instanz.
        method: 'hash' (identischer SHA256) oder 'name_size' (Name + Größe).
        limit: max. Anzahl Duplikat-Gruppen.

    Returns:
        Liste von (id, full_path, filename, extension, size, hash, dup_count).
        Das Platzhalter-Token '[none]' wird als leere Endung ('') geliefert.
    """
    if method == "hash":
        sql = """
            WITH dups AS (
                SELECT hash, COUNT(*) AS c FROM files
                WHERE hash IS NOT NULL AND hash != ''
                GROUP BY hash HAVING c > 1
                ORDER BY c DESC LIMIT ?
            )
            SELECT f.id, d.full_path, f.filename,
                   CASE WHEN e.name IS NULL OR e.name = '[none]' THEN '' ELSE e.name END,
                   f.size, f.hash, dups.c
            FROM dups
            JOIN files f ON f.hash = dups.hash
            JOIN directories d ON f.directory_id = d.id
            LEFT JOIN extensions e ON f.extension_id = e.id
            ORDER BY f.hash, d.full_path
        """
    else:
        sql = """
            WITH dups AS (
                SELECT filename, extension_id, size, COUNT(*) AS c FROM files
                WHERE size IS NOT NULL
                GROUP BY filename, extension_id, size HAVING c > 1
                ORDER BY c DESC LIMIT ?
            )
            SELECT f.id, d.full_path, f.filename,
                   CASE WHEN e.name IS NULL OR e.name = '[none]' THEN '' ELSE e.name END,
                   f.size, f.hash, dups.c
            FROM dups
            JOIN files f ON f.filename = dups.filename
                AND f.extension_id IS dups.extension_id
                AND f.size = dups.size
            JOIN directories d ON f.directory_id = d.id
            LEFT JOIN extensions e ON f.extension_id = e.id
            ORDER BY f.filename, d.full_path
        """
    return db.read_query(sql, (limit,))


def find_name_size_duplicates(db, min_count=2):
    """Findet Gruppen von Dateien mit gleichem Dateinamen UND gleicher Größe
    (Hash-unabhängige Heuristik).

    Returns:
        Liste von (filename, size, count), absteigend nach count.
    """
    return db.read_query(
        """
        SELECT filename, size, COUNT(*) AS c
        FROM files
        WHERE size IS NOT NULL
        GROUP BY filename, size
        HAVING c >= ?
        ORDER BY c DESC
        """,
        (min_count,),
    )
