#!/usr/bin/env python3
"""
Optimiert die Datenbank-Indizes für schnellere Duplikat-Suche.
WICHTIG: Einmalig ausführen nach dem Scan, um die Performance zu verbessern.
"""

import sqlite3
import time
from models import get_db_instance
from utils import logger

def create_optimized_indices():
    """Erstellt optimierte Indizes für Duplikat-Suche"""
    
    print("=" * 70)
    print("DATENBANK-OPTIMIERUNG FÜR DUPLIKAT-SUCHE")
    print("=" * 70)
    
    db = get_db_instance()
    
    # Liste der zu erstellenden Indizes
    indices = [
        # Zusammengesetzter Index für Duplikat-Suche (filename + size)
        ("idx_files_duplicate_search", "files(filename, size, extension_id)", 
         "Optimiert Duplikat-Suche nach Name+Größe"),
        
        # Index für Hash-basierte Duplikatsuche (falls Hashes vorhanden)
        ("idx_files_hash", "files(hash)", 
         "Optimiert Hash-basierte Duplikat-Suche"),
        
        # Größen-Index für schnelle Größenfilter
        ("idx_files_size_fast", "files(size, filename)", 
         "Optimiert Größen-basierte Suche"),
        
        # Verzeichnis-Index für Pfad-Filter
        ("idx_directories_path_search", "directories(full_path, drive_id)", 
         "Optimiert Pfad-basierte Suche"),
        
        # Extension-Index für Typ-Filter
        ("idx_files_extension", "files(extension_id, size)", 
         "Optimiert Dateiendungs-Filter"),
    ]
    
    for idx_name, idx_def, description in indices:
        print(f"\n[{idx_name}]")
        print(f"  Zweck: {description}")
        
        try:
            # Prüfe ob Index bereits existiert
            db.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name=?", 
                (idx_name,)
            )
            if db.cursor.fetchone():
                print(f"  Status: Bereits vorhanden [OK]")
                continue
            
            # Erstelle Index
            print(f"  Status: Wird erstellt...")
            start_time = time.time()
            
            sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {idx_def}"
            db.cursor.execute(sql)
            db.conn.commit()
            
            elapsed = time.time() - start_time
            print(f"  Status: Erstellt in {elapsed:.2f} Sekunden [OK]")
            
        except sqlite3.Error as e:
            print(f"  Status: FEHLER - {e}")
    
    # Analysiere Tabellen für Optimierung
    print("\n" + "=" * 70)
    print("ANALYSE DER TABELLEN...")
    print("=" * 70)
    
    try:
        db.cursor.execute("ANALYZE")
        db.conn.commit()
        print("Tabellenstatistiken aktualisiert [OK]")
    except sqlite3.Error as e:
        print(f"FEHLER bei ANALYZE: {e}")
    
    # Zeige Statistiken
    print("\n" + "=" * 70)
    print("DATENBANKSTATISTIKEN")
    print("=" * 70)
    
    # Dateien nach Laufwerk
    db.cursor.execute("""
        SELECT d.name, COUNT(f.id) as file_count
        FROM drives d
        LEFT JOIN directories dir ON dir.drive_id = d.id
        LEFT JOIN files f ON f.directory_id = dir.id
        GROUP BY d.id
        ORDER BY file_count DESC
    """)
    
    print("\nDateien je Laufwerk:")
    for row in db.cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,} Dateien")
    
    # Größte Duplikat-Kandidaten
    db.cursor.execute("""
        SELECT 
            files.filename || COALESCE(extensions.name, '') as name,
            files.size,
            COUNT(*) as count
        FROM files
        LEFT JOIN extensions ON files.extension_id = extensions.id
        WHERE files.size > 10485760  -- Nur Dateien > 10MB
        GROUP BY files.filename, COALESCE(extensions.name, ''), files.size
        HAVING COUNT(*) > 1
        ORDER BY files.size * COUNT(*) DESC
        LIMIT 10
    """)
    
    print("\nTop 10 Duplikat-Kandidaten (>10MB):")
    for row in db.cursor.fetchall():
        size_mb = row[1] / (1024*1024)
        potential_savings = size_mb * (row[2] - 1)
        print(f"  {row[0][:50]:50} | {size_mb:>8.1f} MB | {row[2]}x | Einsparung: {potential_savings:>8.1f} MB")
    
    print("\n" + "=" * 70)
    print("OPTIMIERUNG ABGESCHLOSSEN!")
    print("=" * 70)

if __name__ == "__main__":
    create_optimized_indices()