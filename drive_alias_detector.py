#!/usr/bin/env python3
r"""
Drive Alias Detection System
Erkennt gemappte Laufwerke (wie T: -> C:\Laufwerk T\USB16GB) um Duplikate zu vermeiden.
"""

import os
import subprocess
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def get_drive_mapping():
    """
    Ermittelt alle Laufwerk-Mappings auf dem System.
    Gibt ein Dictionary zurück: {mapped_drive: real_path}
    
    Z.B.: {'T:': 'C:\\Laufwerk T\\USB16GB', 'U:': '\\\\wsl$\\Ubuntu\\'}
    """
    drive_mappings = {}
    
    try:
        # Verwende subst um gemappte Laufwerke zu finden
        result = subprocess.run(['subst'], capture_output=True, text=True, encoding='cp1252', errors='replace')
        if result.returncode == 0:
            for line in result.stdout.strip().split('\n'):
                if line and ': => ' in line:
                    parts = line.split(': => ')
                    if len(parts) == 2:
                        mapped_drive = parts[0].strip()
                        real_path = parts[1].strip()
                        # Normalisiere Laufwerksbuchstabe: T:\ -> T:
                        drive_letter = mapped_drive.rstrip('\\:') + ':'
                        drive_mappings[drive_letter] = real_path
                        logger.info(f"Gefunden subst-Mapping: {mapped_drive}: => {real_path}")
    except Exception as e:
        logger.warning(f"Fehler beim Ermitteln von subst-Mappings: {e}")
    
    # Zusätzlich: Prüfe Netzlaufwerk-Mappings
    try:
        result = subprocess.run(['net', 'use'], capture_output=True, text=True, encoding='cp1252', errors='replace')
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                # Format: "OK           U:        \\server\share"
                parts = [p.strip() for p in line.split() if p.strip()]
                if len(parts) >= 3 and parts[1].endswith(':'):
                    drive_letter = parts[1]
                    network_path = parts[2]
                    drive_mappings[drive_letter] = network_path
                    logger.info(f"Gefunden Netzlaufwerk-Mapping: {drive_letter} => {network_path}")
    except Exception as e:
        logger.warning(f"Fehler beim Ermitteln von Netzlaufwerk-Mappings: {e}")
    
    return drive_mappings

def normalize_path_with_aliases(path, drive_mappings):
    """
    Normalisiert einen Pfad unter Berücksichtigung von Laufwerk-Aliassen.
    Gibt den "echten" Pfad zurück.
    
    Args:
        path: Original-Pfad (z.B. "T:\\Programme\\...")
        drive_mappings: Dictionary von get_drive_mapping()
    
    Returns:
        Tuple: (normalized_path, is_alias, original_drive, real_drive)
        
    Beispiel: 
        Input: "T:\\Programme\\test"
        Output: ("C:\\Laufwerk T\\USB16GB\\Programme\\test", True, "T:", "C:")
    """
    normalized_path = os.path.normpath(path)
    drive_letter, rest_of_path = os.path.splitdrive(normalized_path)
    
    if drive_letter in drive_mappings:
        real_path = drive_mappings[drive_letter]
        # Kombiniere echten Pfad mit Rest
        if rest_of_path:
            full_real_path = os.path.normpath(os.path.join(real_path, rest_of_path.lstrip(os.sep)))
        else:
            full_real_path = real_path
        
        real_drive = os.path.splitdrive(full_real_path)[0]
        return (full_real_path, True, drive_letter, real_drive)
    else:
        # Kein Alias
        return (normalized_path, False, drive_letter, drive_letter)

def get_canonical_drive_list():
    """
    Ermittelt eine Liste der kanonischen (nicht-gemappten) Laufwerke.
    Vermeidet Duplikate durch Alias-Laufwerke.
    
    Returns:
        List[str]: Liste der echten Laufwerke (ohne Aliases)
    """
    from utils import get_available_drives  # Import hier um Zirkularität zu vermeiden
    
    all_drives = get_available_drives()
    drive_mappings = get_drive_mapping()
    
    canonical_drives = []
    real_paths_seen = set()
    
    for drive in all_drives:
        # Normalize drive letter: 'T:\\' -> 'T:'
        drive_letter = drive.rstrip('/\\')
        if not drive_letter.endswith(':'):
            drive_letter += ':'
        logger.info(f"Prüfe Laufwerk: {drive} -> {drive_letter}")
        
        if drive_letter in drive_mappings:
            # Es ist ein gemapptes Laufwerk - überspringe es komplett
            real_path = drive_mappings[drive_letter]
            real_drive = os.path.splitdrive(real_path)[0]
            logger.info(f"Alias {drive_letter} zeigt auf {real_drive} - überspringe Alias-Laufwerk")
            continue
        else:
            # Es ist ein echtes Laufwerk
            drive_letter_clean = drive_letter.rstrip(':') + ':'
            if drive_letter_clean not in real_paths_seen:
                real_paths_seen.add(drive_letter_clean)
                canonical_drives.append(drive)
                logger.info(f"Echtes Laufwerk: {drive}")
    
    return canonical_drives

def is_path_alias_of(path1, path2):
    """
    Prüft, ob zwei Pfade auf dieselbe physische Stelle zeigen (über Aliases).
    
    Args:
        path1, path2: Pfade zum Vergleichen
        
    Returns:
        bool: True wenn sie auf dieselben Daten zeigen
    """
    drive_mappings = get_drive_mapping()
    
    norm1, is_alias1, orig1, real1 = normalize_path_with_aliases(path1, drive_mappings)
    norm2, is_alias2, orig2, real2 = normalize_path_with_aliases(path2, drive_mappings)
    
    return norm1 == norm2

if __name__ == "__main__":
    # Test-Code
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    print("=== Drive Alias Detection Test ===")
    mappings = get_drive_mapping()
    print(f"Gefundene Mappings: {mappings}")
    
    print(f"\nKanonische Laufwerke:")
    canonical = get_canonical_drive_list()
    print(f"Kanonische Laufwerke: {canonical}")
    for drive in canonical:
        print(f"  {drive}")
    
    # Test path normalization
    test_paths = [
        r"T:\Programme\test",
        r"C:\Laufwerk T\USB16GB\Programme\test",
        r"U:\home\user\test"
    ]
    
    print(f"\nPfad-Normalisierung:")
    for path in test_paths:
        norm, is_alias, orig, real = normalize_path_with_aliases(path, mappings)
        print(f"  {path}")
        print(f"    -> {norm} (Alias: {is_alias}, {orig} -> {real})")