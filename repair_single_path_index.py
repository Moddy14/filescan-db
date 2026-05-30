#!/usr/bin/env python3
"""
Backfill/repair index entries for one concrete file path.
Usage (Windows):
  py repair_single_path_index.py "D:\\...\\file.ext"
"""

import os
import sys
import sqlite3

from models import get_db_instance


def to_db_dir_and_name(win_path: str):
    norm = os.path.normpath(win_path)
    drive, rest = os.path.splitdrive(norm)
    if not drive:
        raise ValueError("Path must include drive letter (e.g. D:\\...)")
    drive_name = drive.upper() + "/"
    directory = os.path.dirname(norm).replace("\\", "/")
    filename = os.path.basename(norm)
    return drive_name, directory, filename


def run(file_path: str):
    if not os.path.isfile(file_path):
        raise FileNotFoundError(file_path)

    drive_name, directory, filename = to_db_dir_and_name(file_path)
    size = os.path.getsize(file_path)

    db = get_db_instance()
    db.conn.execute("PRAGMA busy_timeout = 120000")

    max_retries = 6
    for attempt in range(1, max_retries + 1):
        try:
            drive_id = db.get_or_create_drive(drive_name)
            dir_id = db.get_or_create_directory_optimized(drive_id, directory)
            file_id = db.insert_file_optimized(
                dir_id,
                filename,
                size,
                hash_val=None,
                created_date=None,
                modified_date=None,
            )
            db.conn.commit()

            stem, ext = os.path.splitext(filename)
            db.cursor.execute(
                """
                SELECT f.id, d.full_path, f.filename, COALESCE(e.name, '') AS ext, f.size
                FROM files f
                JOIN directories d ON d.id = f.directory_id
                LEFT JOIN extensions e ON e.id = f.extension_id
                WHERE d.full_path = ? AND f.filename = ? AND COALESCE(e.name, '') = ?
                """,
                (directory, stem, ext if ext else "[none]"),
            )
            rows = db.cursor.fetchall()
            print(f"OK: drive={drive_name} dir_id={dir_id} file_id={file_id} rows={len(rows)}")
            return 0
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if ("locked" in msg or "busy" in msg) and attempt < max_retries:
                wait_s = min(0.3 * (2 ** (attempt - 1)), 2.5)
                print(f"retry {attempt}/{max_retries} after lock: {e} (sleep {wait_s:.2f}s)")
                import time
                time.sleep(wait_s)
                continue
            raise


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('Usage: py repair_single_path_index.py "D:\\path\\to\\file.ext"')
        raise SystemExit(2)
    raise SystemExit(run(sys.argv[1]))
