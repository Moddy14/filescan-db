#!/usr/bin/env python3
"""
Wartungsskript zur Überprüfung und Anpassung aller Skripte im Dateien_Skripte Verzeichnis
an die aktuelle Datenbankstruktur.
"""

import os
import sys
import re

def check_script_compatibility(script_path):
    """
    Überprüft ein Skript auf Kompatibilität mit der neuen DB-Struktur.
    Gibt Warnungen und Empfehlungen zurück.
    """
    issues = []
    
    try:
        with open(script_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        # Fallback auf latin-1 wenn UTF-8 fehlschlägt
        with open(script_path, 'r', encoding='latin-1') as f:
            content = f.read()
    
    # Prüfe auf alte Tabellennamen (die nicht mehr existieren sollten)
    old_tables = ['file_list', 'dir_list', 'files_old', 'directories_old']
    for table in old_tables:
        if re.search(rf'\b{table}\b', content, re.IGNORECASE):
            issues.append(f"WARNUNG: Verwendet alte Tabelle '{table}'")
    
    # Prüfe ob die neuen Tabellen verwendet werden
    new_tables = ['files', 'directories', 'drives', 'extensions']
    tables_found = []
    for table in new_tables:
        if re.search(rf'\bFROM\s+{table}\b', content, re.IGNORECASE):
            tables_found.append(table)
    
    # Prüfe auf JOIN-Statements mit korrekten Relationen
    if 'JOIN directories ON files.directory_id = directories.id' in content:
        pass  # Korrekt
    elif 'JOIN files' in content and 'directory_id' not in content:
        issues.append("WARNUNG: JOIN zwischen files und directories nutzt möglicherweise falsche Spalten")
    
    # Prüfe auf korrekte Spaltennutzung
    if 'files.file_path' in content:
        issues.append("VERALTET: 'files.file_path' existiert nicht mehr. Nutze directories.full_path + files.filename + extensions.name")
    
    if 'files.path' in content:
        issues.append("VERALTET: 'files.path' existiert nicht mehr. Nutze directories.full_path")
    
    # Prüfe Foreign Keys
    if 'PRAGMA foreign_keys' not in content and 'DELETE FROM' in content:
        issues.append("INFO: Skript nutzt DELETE ohne Foreign Keys zu aktivieren")
    
    return issues, tables_found

def main():
    script_dir = os.path.join(os.path.dirname(__file__), 'Dateien_Skripte')
    
    if not os.path.exists(script_dir):
        print(f"Verzeichnis {script_dir} nicht gefunden!")
        return
    
    print("=" * 70)
    print("ÜBERPRÜFUNG DER SKRIPTE AUF DATENBANK-KOMPATIBILITÄT")
    print("=" * 70)
    print()
    
    all_good = True
    
    for filename in os.listdir(script_dir):
        if filename.endswith('.py'):
            script_path = os.path.join(script_dir, filename)
            print(f"Prüfe: {filename}")
            print("-" * 40)
            
            try:
                issues, tables_found = check_script_compatibility(script_path)
                
                if tables_found:
                    print(f"  Nutzt Tabellen: {', '.join(tables_found)}")
                
                if issues:
                    all_good = False
                    print("  PROBLEME GEFUNDEN:")
                    for issue in issues:
                        print(f"    - {issue}")
                else:
                    print("  [OK] Keine Probleme gefunden")
                
            except Exception as e:
                print(f"  FEHLER beim Lesen: {e}")
                all_good = False
            
            print()
    
    print("=" * 70)
    if all_good:
        print("ERGEBNIS: Alle Skripte sind kompatibel mit der aktuellen DB-Struktur!")
    else:
        print("ERGEBNIS: Einige Skripte benötigen möglicherweise Anpassungen.")
        print("\nEMPFOHLENE STRUKTUR:")
        print("""
        Tabellen:
        - drives (id, name)
        - directories (id, drive_id, parent_id, directory_name, full_path, depth_level)
        - files (id, directory_id, filename, extension_id, size, hash, created_date, modified_date)
        - extensions (id, name, category, is_binary, mime_type)
        
        Korrekte JOINs:
        - files JOIN directories ON files.directory_id = directories.id
        - directories JOIN drives ON directories.drive_id = drives.id
        - files LEFT JOIN extensions ON files.extension_id = extensions.id
        
        Vollständiger Dateipfad:
        - directories.full_path || '/' || files.filename || COALESCE(extensions.name, '')
        """)
    print("=" * 70)

if __name__ == "__main__":
    main()