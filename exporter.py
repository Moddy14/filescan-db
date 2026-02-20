import os
import sys
import json
import csv
import time
import html
from datetime import datetime
import logging

# Importiere zentrale Funktionen und Konstanten
from utils import DB_PATH, CONFIG, PROJECT_DIR, logger
from models import get_db_instance

# Exportverzeichnis definieren (relativ zum Projekt)
EXPORT_DIR = os.path.join(PROJECT_DIR, "exports")

# Stelle sicher, dass das Exportverzeichnis existiert
try:
    os.makedirs(EXPORT_DIR, exist_ok=True)
except OSError as e:
    logger.warning(f"[Exporter Warnung] Konnte Exportverzeichnis nicht erstellen: {EXPORT_DIR} - {e}")
    # Evtl. hier beenden? Oder versuchen, im Projektverzeichnis zu speichern?
    EXPORT_DIR = PROJECT_DIR # Fallback
    logger.warning(f"[Exporter Warnung] Exportiere stattdessen nach: {EXPORT_DIR}")

# Lade Exportformate aus der Konfiguration
EXPORT_FORMATS = CONFIG.get("export_formats", ["csv", "json", "html"])
if not isinstance(EXPORT_FORMATS, list):
    logger.warning("[Exporter Warnung] 'export_formats' in config.json ist keine Liste. Verwende Standardformate.")
    EXPORT_FORMATS = ["csv", "json", "html"]

def fetch_file_data(db_cursor, path_filter=None):
    """Holt Dateiinformationen aus der Datenbank, optional gefiltert nach Pfad."""
    # Erweiterte Query für optimierte Datenbankstruktur - rekonstruiert vollständigen Pfad
    query = '''
        SELECT 
            d.full_path || '/' || f.filename || COALESCE(e.name, '') as file_path,
            f.size, 
            f.hash, 
            d.full_path AS dir_path, 
            dr.name AS drive_name,
            e.name AS extension,
            e.category AS file_category
        FROM files f
        JOIN directories d ON f.directory_id = d.id
        JOIN drives dr ON d.drive_id = dr.id
        LEFT JOIN extensions e ON f.extension_id = e.id
    '''
    params = []
    if path_filter:
        # Filtert auf Dateien, deren Verzeichnis mit dem path_filter beginnt oder gleich ist
        normalized_filter = os.path.normpath(path_filter)
        query += " WHERE (d.full_path LIKE ? OR d.full_path = ?)"
        params.extend([normalized_filter + os.sep + '%', normalized_filter])

    query += " ORDER BY dr.name, d.full_path, f.filename" # Bessere Sortierung

    logger.info(f"[Exporter] Führe Abfrage aus: {query} mit Parametern: {params}")
    db_cursor.execute(query, params)
    # Gebe den Cursor zurück, um Daten in Chunks zu lesen, falls nötig
    # return db_cursor.fetchall() # Kann bei großen DBs viel Speicher brauchen
    return db_cursor

def export_csv(cursor, filepath):
    """Exportiert Daten aus einem DB-Cursor in eine CSV-Datei."""
    try:
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Dateipfad", "Größe (Bytes)", "SHA256", "Verzeichnis", "Laufwerk", "Extension", "Kategorie"])
            count = 0
            while True:
                rows = cursor.fetchmany(1000) # Lese in Chunks
                if not rows:
                    break
                # Normalisiere Pfade für CSV-Export
                normalized_rows = []
                for row in rows:
                    file_path = os.path.normpath(row[0].replace('/', os.sep)) if row[0] else ''
                    normalized_row = (file_path,) + row[1:]  # Ersetze ersten Wert
                    normalized_rows.append(normalized_row)
                writer.writerows(normalized_rows)
                count += len(rows)
            logger.info(f"[Exporter] {count} Einträge nach {filepath} (CSV) geschrieben.")
            return True
    except Exception as e:
        logger.error(f"[Exporter CSV-Fehler] {filepath}: {e}")
        return False

def export_json(cursor, filepath):
    """Exportiert Daten aus einem DB-Cursor in eine JSON-Datei."""
    records = []
    count = 0
    try:
        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                 file_path = os.path.normpath(row[0].replace('/', os.sep)) if row[0] else ''
                 records.append({
                    "file_path": file_path,
                    "size": row[1],
                    "hash": row[2],
                    "directory": row[3],
                    "drive": row[4],
                    "extension": row[5],
                    "category": row[6]
                })
            count += len(rows)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2) # Kleineres Indent
        logger.info(f"[Exporter] {count} Einträge nach {filepath} (JSON) geschrieben.")
        return True
    except Exception as e:
        logger.error(f"[Exporter JSON-Fehler] {filepath}: {e}")
        return False

def export_html(cursor, filepath):
    """Exportiert Daten aus einem DB-Cursor in eine HTML-Datei."""
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("<!DOCTYPE html>\n<html>\n<head>\n")
            f.write("  <meta charset=\"utf-8\">\n")
            f.write("  <title>Dateiexport</title>\n")
            # Einfaches CSS für bessere Lesbarkeit
            f.write("  <style>\n")
            f.write("    body { font-family: sans-serif; margin: 20px; }\n")
            f.write("    table { border-collapse: collapse; width: 100%; }\n")
            f.write("    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }\n")
            f.write("    th { background-color: #f2f2f2; font-weight: bold; }\n")
            f.write("    tr:nth-child(even) { background-color: #f9f9f9; }\n")
            f.write("    .number { text-align: right; }\n")
            f.write("  </style>\n</head>\n<body>\n")
            f.write("  <h2>Dateiübersicht</h2>\n")
            f.write("  <table>\n")
            f.write("    <thead><tr><th>Dateipfad</th><th>Größe (Bytes)</th><th>SHA256</th><th>Verzeichnis</th><th>Laufwerk</th><th>Extension</th><th>Kategorie</th></tr></thead>\n")
            f.write("    <tbody>\n")
            count = 0
            while True:
                 rows = cursor.fetchmany(1000)
                 if not rows:
                     break
                 for row in rows:
                     # Normalisiere Dateipfad für HTML
                     file_path = os.path.normpath(row[0].replace('/', os.sep)) if row[0] else ''
                     normalized_row = (file_path,) + row[1:]
                     
                     f.write("      <tr>")
                     for i, col in enumerate(normalized_row):
                         css_class = ' class="number"' if i == 1 else ''  # Size column
                         escaped_val = html.escape(str(col)) if col is not None else ''
                         f.write(f"<td{css_class}>{escaped_val}</td>")
                     f.write("</tr>\n")
                 count += len(rows)
            f.write("    </tbody>\n  </table>\n")
            f.write(f"  <p><small>Export erstellt am {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</small></p>\n")
            f.write("</body>\n</html>")
        logger.info(f"[Exporter] {count} Einträge nach {filepath} (HTML) geschrieben.")
        return True
    except Exception as e:
        logger.error(f"[Exporter HTML-Fehler] {filepath}: {e}")
        return False

def log_export(db, export_type, filepath):
    """Loggt einen erfolgreichen Export in die Datenbank."""
    try:
        # Zeitstempel direkt in SQL verwenden für Konsistenz
        db.cursor.execute(
            "INSERT INTO export_log (export_type, export_time, file_path) VALUES (?, datetime('now', 'localtime'), ?)",
            (export_type, filepath)
        )
        db.conn.commit()
    except Exception as e:
        logger.error(f"[Exporter DB-Log Fehler] Konnte Export für {filepath} nicht loggen: {e}")

def export_all(db, path_filter=None):
    """Führt den Export für alle konfigurierten Formate durch."""
    logger.info("[Exporter] Starte Exportvorgang...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    overall_success = True

    for fmt in EXPORT_FORMATS:
        export_func = None
        if fmt.lower() == "csv":
            export_func = export_csv
        elif fmt.lower() == "json":
            export_func = export_json
        elif fmt.lower() == "html":
            export_func = export_html
        else:
            logger.warning(f"[Exporter Warnung] Unbekanntes Exportformat übersprungen: {fmt}")
            continue

        filename = f"export_{timestamp}.{fmt.lower()}"
        filepath = os.path.join(EXPORT_DIR, filename)
        logger.info(f"[Exporter] Exportiere nach {filepath} ({fmt.upper()})...")

        try:
            # Hole Daten für jedes Format neu, da der Cursor verbraucht wird
            data_cursor = fetch_file_data(db.cursor, path_filter)
            success = export_func(data_cursor, filepath)

            if success:
                log_export(db, fmt.lower(), filepath)
                # Commit erfolgt in log_export
            else:
                overall_success = False # Markiere Gesamtprozess als nicht vollständig erfolgreich

        except Exception as e:
            logger.critical(f"[Exporter Kritischer Fehler] Bei Export nach {filepath} ({fmt.upper()}): {e}")
            import traceback
            logger.error(traceback.format_exc())
            overall_success = False

    if overall_success:
        write_log("[Exporter] Alle Exporte erfolgreich abgeschlossen.")
    else:
        write_log("[Exporter] Einige Exporte sind fehlgeschlagen.")
    return overall_success

def main():
    """Hauptfunktion: Ermittelt Filter und startet den Export."""
    path_filter = None
    if len(sys.argv) == 2:
        # Das Argument wird als Pfad-Prefix-Filter interpretiert
        path_filter = sys.argv[1]
        write_log(f"[Exporter] Verwende Pfadfilter aus Argument: {path_filter}")
    elif len(sys.argv) > 2:
        print("Verwendung: python exporter.py [optional: <Pfad-Filter>]")
        sys.exit(1)
    else:
         write_log("[Exporter] Kein Pfadfilter angegeben, exportiere alle Daten.")

    try:
        db = get_db_instance() # Holt die globale Instanz
        if not db:
             write_log("[Exporter Fehler] Konnte DB-Instanz nicht erhalten.")
             sys.exit(1)

        success = export_all(db, path_filter)

        if success:
             write_log("[Exporter] Programm erfolgreich beendet.")
             sys.exit(0)
        else:
             write_log("[Exporter] Programm mit Fehlern beendet.")
             sys.exit(1)

    except Exception as e:
        write_log(f"[Exporter Fehler] Unerwarteter Fehler im Hauptablauf: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
