#!/usr/bin/env python3
"""
Scan Status Monitor - Zeigt detaillierte Informationen über laufende Scans
"""

import os
import sys
import sqlite3
import datetime
import socket
import json
from pathlib import Path

# Projekt-Verzeichnis zur Python-Path hinzufügen
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_DIR)

from utils import DB_PATH, logger, setup_logging

def get_scan_status_details():
    """Holt detaillierte Informationen über alle aktiven und kürzlichen Scans."""
    
    scan_info = {
        'active_scans': [],
        'recent_scans': [],
        'scan_progress': [],
        'orphaned_locks': [],
        'status_summary': ''
    }
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # 1. Aktive Scans aus scan_lock Tabelle
        cursor.execute("""
            SELECT id, scan_type, start_time, pid, hostname, is_active
            FROM scan_lock 
            WHERE is_active = 1
            ORDER BY start_time DESC
        """)
        active_locks = cursor.fetchall()
        
        current_hostname = socket.gethostname()
        
        for lock in active_locks:
            lock_id, scan_type, start_time, pid, hostname, is_active = lock
            
            # Prüfe ob PID noch läuft (wenn auf diesem Host)
            pid_exists = False
            if hostname == current_hostname:
                try:
                    import psutil
                    pid_exists = psutil.pid_exists(pid)
                except ImportError:
                    # Fallback ohne psutil
                    try:
                        os.kill(pid, 0)
                        pid_exists = True
                    except OSError:
                        pid_exists = False
            else:
                # Auf anderem Host, nehmen wir an es läuft
                pid_exists = True
            
            # Berechne Laufzeit
            try:
                start_dt = datetime.datetime.fromisoformat(start_time)
                runtime = str(datetime.datetime.now() - start_dt).split('.')[0]
            except:
                runtime = "Unbekannt"
            
            scan_details = {
                'lock_id': lock_id,
                'scan_type': scan_type,
                'start_time': start_time,
                'runtime': runtime,
                'pid': pid,
                'hostname': hostname,
                'pid_exists': pid_exists,
                'status': 'Läuft' if pid_exists else 'Verwaist'
            }
            
            if pid_exists:
                scan_info['active_scans'].append(scan_details)
            else:
                scan_info['orphaned_locks'].append(scan_details)
        
        # 2. Scan-Progress aus scan_progress Tabelle
        cursor.execute("""
            SELECT drive_id, last_path, timestamp 
            FROM scan_progress
        """)
        progress_entries = cursor.fetchall()
        
        for drive_id, last_path, timestamp in progress_entries:
            # Hole Laufwerksinformationen
            cursor.execute("SELECT name FROM drives WHERE id = ?", (drive_id,))
            drive_info = cursor.fetchone()
            if drive_info:
                drive_name = drive_info[0]
                scan_info['scan_progress'].append({
                    'drive': drive_name,
                    'last_path': last_path if last_path else "Unbekannt",
                    'timestamp': timestamp,
                    'drive_id': drive_id
                })
        
        # 3. Kürzliche Scan-Aktivitäten (letzte 10)
        cursor.execute("""
            SELECT id, scan_type, start_time, pid, hostname 
            FROM scan_lock 
            WHERE is_active = 0
            ORDER BY start_time DESC
            LIMIT 10
        """)
        recent = cursor.fetchall()
        
        for scan in recent:
            lock_id, scan_type, start_time, pid, hostname = scan
            scan_info['recent_scans'].append({
                'lock_id': lock_id,
                'scan_type': scan_type,
                'start_time': start_time,
                'pid': pid,
                'hostname': hostname
            })
        
        # 4. Erstelle Zusammenfassung
        if scan_info['active_scans']:
            active_count = len(scan_info['active_scans'])
            scan_info['status_summary'] = f"[OK] {active_count} aktive(r) Scan(s) läuft/laufen"
            for scan in scan_info['active_scans']:
                scan_info['status_summary'] += f"\n  • {scan['scan_type']} (PID: {scan['pid']}, Laufzeit: {scan['runtime']})"
        elif scan_info['orphaned_locks']:
            orphan_count = len(scan_info['orphaned_locks'])
            scan_info['status_summary'] = f"[WARNUNG] {orphan_count} verwaiste(r) Scan-Lock(s) gefunden"
        else:
            scan_info['status_summary'] = "[OK] Kein Scan läuft aktuell - System bereit"
        
        # Scan-Progress hinzufügen
        if scan_info['scan_progress']:
            scan_info['status_summary'] += "\n\n[INFO] Scan-Progress Einträge:"
            for progress in scan_info['scan_progress']:
                scan_info['status_summary'] += f"\n  • {progress['drive']}: Letzter Pfad: {progress['last_path']}"
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Fehler beim Abrufen des Scan-Status: {e}")
        scan_info['status_summary'] = f"[FEHLER] Fehler beim Abrufen des Status: {e}"
    
    return scan_info

def clean_orphaned_locks():
    """Bereinigt verwaiste Scan-Locks."""
    cleaned = 0
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Hole alle aktiven Locks
        cursor.execute("""
            SELECT id, pid, hostname 
            FROM scan_lock 
            WHERE is_active = 1
        """)
        active_locks = cursor.fetchall()
        
        current_hostname = socket.gethostname()
        
        for lock_id, pid, hostname in active_locks:
            if hostname == current_hostname:
                # Prüfe ob PID noch existiert
                try:
                    import psutil
                    if not psutil.pid_exists(pid):
                        cursor.execute("UPDATE scan_lock SET is_active = 0 WHERE id = ?", (lock_id,))
                        cleaned += 1
                        logger.info(f"Bereinigt: Verwaister Lock {lock_id} (PID {pid} existiert nicht mehr)")
                except ImportError:
                    try:
                        os.kill(pid, 0)
                    except OSError:
                        cursor.execute("UPDATE scan_lock SET is_active = 0 WHERE id = ?", (lock_id,))
                        cleaned += 1
                        logger.info(f"Bereinigt: Verwaister Lock {lock_id} (PID {pid} existiert nicht mehr)")
        
        # Bereinige auch scan_progress Tabelle
        cursor.execute("DELETE FROM scan_progress")
        
        conn.commit()
        conn.close()
        
        return cleaned
        
    except Exception as e:
        logger.error(f"Fehler beim Bereinigen verwaister Locks: {e}")
        return -1

def format_scan_info_for_console(scan_info):
    """Formatiert Scan-Informationen für die Konsole."""
    
    output = []
    output.append("=" * 60)
    output.append("SCAN STATUS MONITOR")
    output.append("=" * 60)
    output.append("")
    
    # Zusammenfassung
    output.append(scan_info['status_summary'])
    output.append("")
    
    # Aktive Scans
    if scan_info['active_scans']:
        output.append("[AKTIVE SCANS]:")
        output.append("-" * 40)
        for scan in scan_info['active_scans']:
            output.append(f"  Typ: {scan['scan_type']}")
            output.append(f"  PID: {scan['pid']} @ {scan['hostname']}")
            output.append(f"  Start: {scan['start_time']}")
            output.append(f"  Laufzeit: {scan['runtime']}")
            output.append("-" * 40)
    
    # Verwaiste Locks
    if scan_info['orphaned_locks']:
        output.append("")
        output.append("[WARNUNG] VERWAISTE LOCKS:")
        output.append("-" * 40)
        for lock in scan_info['orphaned_locks']:
            output.append(f"  Lock ID: {lock['lock_id']}")
            output.append(f"  Typ: {lock['scan_type']}")
            output.append(f"  PID: {lock['pid']} (existiert nicht mehr)")
            output.append(f"  Start: {lock['start_time']}")
            output.append("-" * 40)
    
    # Kürzliche Scans
    if scan_info['recent_scans']:
        output.append("")
        output.append("[HISTORIE] KÜRZLICHE SCANS (abgeschlossen):")
        output.append("-" * 40)
        for i, scan in enumerate(scan_info['recent_scans'][:5], 1):
            output.append(f"  {i}. {scan['scan_type']} - {scan['start_time']}")
    
    output.append("")
    output.append("=" * 60)
    
    return "\n".join(output)

def main():
    """Hauptfunktion."""
    setup_logging("ScanStatusMonitor")
    
    import argparse
    parser = argparse.ArgumentParser(description="Scan Status Monitor")
    parser.add_argument("--clean", action="store_true", help="Bereinige verwaiste Locks")
    parser.add_argument("--json", action="store_true", help="Ausgabe als JSON")
    parser.add_argument("--watch", action="store_true", help="Kontinuierliche Überwachung")
    args = parser.parse_args()
    
    if args.clean:
        cleaned = clean_orphaned_locks()
        if cleaned >= 0:
            print(f"[OK] {cleaned} verwaiste Locks bereinigt")
        else:
            print("[FEHLER] Fehler beim Bereinigen")
        return
    
    if args.watch:
        # Kontinuierliche Überwachung
        import time
        print("Starte kontinuierliche Überwachung (Strg+C zum Beenden)...")
        try:
            while True:
                os.system('cls' if os.name == 'nt' else 'clear')
                scan_info = get_scan_status_details()
                print(format_scan_info_for_console(scan_info))
                time.sleep(2)  # Aktualisiere alle 2 Sekunden
        except KeyboardInterrupt:
            print("\nÜberwachung beendet.")
    else:
        # Einmalige Ausgabe
        scan_info = get_scan_status_details()
        
        if args.json:
            print(json.dumps(scan_info, indent=2, ensure_ascii=False))
        else:
            print(format_scan_info_for_console(scan_info))

if __name__ == "__main__":
    main()