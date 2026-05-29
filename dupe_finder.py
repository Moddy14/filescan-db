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
