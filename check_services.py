#!/usr/bin/env python3
"""Prüft den Status der Scanner-Services ohne andere Prozesse zu stören"""

import subprocess
import os

print("=== Scanner Service Status ===\n")

# Prüfe laufende Prozesse
result = subprocess.run(['wmic', 'process', 'where', "name='python.exe' or name='pythonw.exe'", 'get', 'commandline'], 
                       capture_output=True, text=True)

services = {
    'Watchdog': False,
    'Tray': False,
    'Scanner': False,
    'GUI': False
}

for line in result.stdout.split('\n'):
    if 'watchdog' in line.lower():
        services['Watchdog'] = True
    if 'systray' in line.lower():
        services['Tray'] = True
    if 'scanner_core' in line.lower():
        services['Scanner'] = True
    if 'gui' in line.lower() or 'dateisuche' in line.lower():
        services['GUI'] = True

print("Service-Status:")
for service, running in services.items():
    status = "[OK] Läuft" if running else "[X] Gestoppt"
    print(f"  {service}: {status}")

# Prüfe Database-Status
import sqlite3
db_path = os.path.join(os.path.dirname(__file__), 'Dateien.db')
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Prüfe auf aktive Scans
    cursor.execute("SELECT * FROM scan_progress")
    scan_locks = cursor.fetchall()
    
    if scan_locks:
        print(f"\n⚠ WARNUNG: {len(scan_locks)} Scan-Locks gefunden!")
        for lock in scan_locks:
            print(f"  Drive ID {lock[0]}: {lock[1]}")
    else:
        print("\n[OK] Keine Scan-Locks - Datenbank bereit")
    
    conn.close()
else:
    print("\n[X] Datenbank nicht gefunden!")