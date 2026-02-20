#!/usr/bin/env python3
"""
Vollständiger System-Tray Launcher mit allen Funktionen
Reparierte Version mit besserer Stabilität und Optik
"""

import os
import sys
import subprocess
import threading
from PIL import Image, ImageDraw, ImageFont
import ctypes
import ctypes.wintypes
import pystray
from pystray import MenuItem as item
import logging

# Projekt-Konfiguration
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(PROJECT_DIR, "Dateien.db")
SCRIPTS_DIR = os.path.join(PROJECT_DIR, "Dateien_Skripte")

# Setup logging: Rotierender File-Logger (pythonw.exe hat kein stdout)
try:
    from utils import logger, CONFIG, load_config, save_config
except ImportError:
    # Fallback: Eigener File-Logger wenn utils nicht verfügbar
    from logging.handlers import RotatingFileHandler
    logger = logging.getLogger("systray")
    logger.setLevel(logging.INFO)
    _log_path = os.path.join(PROJECT_DIR, "systray.log")
    _handler = RotatingFileHandler(_log_path, maxBytes=5*1024*1024, backupCount=2, encoding='utf-8')
    _handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(_handler)
    CONFIG = {}
    def load_config(): pass
    def save_config(path): pass

# Watchdog-Steuerung: NSSM-Dienst bevorzugt, watchdog_control.py als Fallback
try:
    from watchdog_control import find_watchdog_pid, stop_watchdog, start_watchdog
except ImportError:
    logger.warning("watchdog_control nicht verfügbar")
    def find_watchdog_pid(): return None
    def stop_watchdog(): return False
    def start_watchdog(): return False

def _extract_exe_icon(exe_path, icon_index=0, size=64):
    """Extrahiert ein Icon aus einer EXE/DLL als PIL Image."""
    import win32gui
    import win32ui
    import win32con

    # Lade Icon-Handle aus der Datei
    large_icons, small_icons = win32gui.ExtractIconEx(exe_path, icon_index, 1)
    if not large_icons:
        return None

    hicon = large_icons[0]
    try:
        # Device Context erstellen
        hdc = win32ui.CreateDCFromHandle(win32gui.GetDC(0))
        hbmp = win32ui.CreateBitmap()
        hbmp.CreateCompatibleBitmap(hdc, size, size)

        hdc_mem = hdc.CreateCompatibleDC()
        hdc_mem.SelectObject(hbmp)

        # Hintergrund transparent (schwarz) füllen
        hdc_mem.FillSolidRect((0, 0, size, size), 0)

        # Icon zeichnen
        win32gui.DrawIconEx(
            hdc_mem.GetSafeHdc(), 0, 0, hicon, size, size, 0, None,
            win32con.DI_NORMAL
        )

        # Bitmap-Bits auslesen
        bmpinfo = hbmp.GetInfo()
        bmpstr = hbmp.GetBitmapBits(True)

        # In PIL Image konvertieren (BGRX → RGBA)
        img = Image.frombuffer('RGBA', (bmpinfo['bmWidth'], bmpinfo['bmHeight']),
                               bmpstr, 'raw', 'BGRA', 0, 1)

        # Cleanup
        hdc_mem.DeleteDC()
        win32gui.ReleaseDC(0, hdc.GetSafeHdc())
        return img
    finally:
        # Icon-Handles freigeben
        for h in large_icons + small_icons:
            win32gui.DestroyIcon(h)


def create_icon():
    """Erstellt das Tray-Icon (Windows Explorer Icon bevorzugt)."""
    # 1. Windows Explorer Icon
    try:
        explorer_path = os.path.join(os.environ.get('WINDIR', r'C:\Windows'), 'explorer.exe')
        img = _extract_exe_icon(explorer_path, icon_index=0, size=64)
        if img:
            logger.info("Explorer-Icon erfolgreich extrahiert")
            return img
    except Exception as e:
        logger.warning(f"Explorer-Icon Extraktion fehlgeschlagen: {e}")

    # 2. Fallback: eigenes generiertes Icon
    icon_path = os.path.join(PROJECT_DIR, 'icons', 'systray.ico')
    if os.path.exists(icon_path):
        try:
            img = Image.open(icon_path)
            img = img.resize((64, 64), Image.LANCZOS)
            logger.info("Eigenes Systray-Icon geladen")
            return img
        except Exception as e:
            logger.warning(f"Eigenes Icon konnte nicht geladen werden: {e}")

    # 3. Fallback: einfaches Icon
    logger.info("Verwende Fallback-Icon")
    image = Image.new('RGBA', (64, 64), (0, 120, 215, 255))
    dc = ImageDraw.Draw(image)
    dc.ellipse([4, 4, 59, 59], fill=(0, 120, 215, 255), outline='white', width=2)
    try:
        font = ImageFont.truetype("arial.ttf", 28)
        dc.text((32, 32), "D", fill="white", anchor="mm", font=font)
    except Exception:
        dc.text((24, 18), "D", fill="white")
    return image

def run_script(script_name, args=None):
    """Startet ein Python-Skript als separaten Prozess."""
    try:
        script_path = os.path.join(PROJECT_DIR, script_name)

        if not os.path.exists(script_path):
            logger.error(f"Script nicht gefunden: {script_path}")
            return False
        
        # Python-Interpreter verwenden
        command = [sys.executable, script_path]
        
        # Argumente hinzufügen
        if args:
            if isinstance(args, str):
                command.append(args)
            else:
                command.extend(args)
        
        # Script im Hintergrund starten
        process = subprocess.Popen(
            command, 
            cwd=PROJECT_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
        )
        
        logger.info(f"Gestartet: {script_name} (PID: {process.pid})")
        return True
        
    except Exception as e:
        logger.error(f"Fehler beim Starten von {script_name}: {e}")
        return False

# === Menü-Aktionen ===

def on_gui(icon, item):
    """Öffnet die GUI/Hauptanwendung."""
    logger.info("Öffne GUI...")
    if os.path.exists(os.path.join(PROJECT_DIR, "gui_launcher.py")):
        run_script("gui_launcher.py")
    else:
        # Fallback: Öffne Dateisuche direkt
        on_search(icon, item)

def on_search(icon, item):
    """Öffnet die erweiterte Dateisuche."""
    logger.info("Öffne Dateisuche...")
    enhanced_search = os.path.join(SCRIPTS_DIR, "Enhanced_Dateisuche.py")
    normal_search = os.path.join(SCRIPTS_DIR, "Dateisuche.py")
    
    if os.path.exists(enhanced_search):
        run_script(os.path.join("Dateien_Skripte", "Enhanced_Dateisuche.py"), DB_PATH)
    elif os.path.exists(normal_search):
        run_script(os.path.join("Dateien_Skripte", "Dateisuche.py"), DB_PATH)
    else:
        logger.error("Keine Dateisuche gefunden!")
        icon.notify("Dateisuche nicht gefunden!", "Fehler")

def on_duplicate_manager(icon, item):
    """Öffnet den Duplikat-Ordner-Manager."""
    logger.info("Öffne Duplikat-Manager...")
    dup_manager = os.path.join(SCRIPTS_DIR, "Duplikat_Ordner_Manager.py")
    if os.path.exists(dup_manager):
        run_script(os.path.join("Dateien_Skripte", "Duplikat_Ordner_Manager.py"), DB_PATH)
    else:
        logger.error("Duplikat-Manager nicht gefunden!")
        icon.notify("Duplikat-Manager nicht gefunden!", "Fehler")

def on_scan_all(icon, item):
    """Startet Scan aller Laufwerke."""
    logger.info("Starte Scan aller Laufwerke...")
    if run_script("scan_all_drives.py"):
        icon.notify("Scan aller Laufwerke gestartet", "Scanner")

def on_scan_current(icon, item):
    """Scannt das konfigurierte Laufwerk."""
    logger.info("Starte Scan des konfigurierten Pfads...")
    base_path = CONFIG.get('base_path', 'C:\\')
    if run_script("scanner_core.py", [base_path, "--restart"]):
        icon.notify(f"Scan gestartet: {base_path}", "Scanner")

def on_integrity_check(icon, item):
    """Startet Integritätsprüfung."""
    logger.info("Starte Integritätsprüfung...")
    if os.path.exists(os.path.join(PROJECT_DIR, "integrity_checker.py")):
        run_script("integrity_checker.py")
        icon.notify("Integritätsprüfung gestartet", "Scanner")
    else:
        logger.warning("Integritätsprüfung nicht verfügbar")

def on_export(icon, item):
    """Exportiert die Datenbank."""
    logger.info("Starte Export...")
    if os.path.exists(os.path.join(PROJECT_DIR, "exporter.py")):
        run_script("exporter.py")
        icon.notify("Export gestartet", "Scanner")
    else:
        logger.warning("Export nicht verfügbar")

def on_watchdog_start(icon, item):
    """Startet den Watchdog-Service (delegiert an watchdog_control.py)."""
    def _start():
        try:
            if find_watchdog_pid() is not None:
                icon.notify("Watchdog läuft bereits", "Scanner")
                return
            logger.info("Starte Watchdog-Service...")
            if start_watchdog():
                icon.notify("Watchdog-Service gestartet", "Scanner")
            else:
                icon.notify("Watchdog-Start fehlgeschlagen", "Fehler")
        except Exception as e:
            logger.error(f"Fehler beim Watchdog-Start: {e}")
            icon.notify("Fehler beim Watchdog-Start", "Fehler")
    threading.Thread(target=_start, daemon=True).start()

def on_watchdog_stop(icon, item):
    """Stoppt den Watchdog-Service (delegiert an watchdog_control.py)."""
    def _stop():
        try:
            if find_watchdog_pid() is None:
                icon.notify("Watchdog ist bereits gestoppt", "Scanner")
                return
            logger.info("Stoppe Watchdog-Service...")
            if stop_watchdog():
                icon.notify("Watchdog-Service gestoppt", "Scanner")
            else:
                icon.notify("Watchdog-Stop fehlgeschlagen", "Fehler")
        except Exception as e:
            logger.error(f"Fehler beim Watchdog-Stop: {e}")
            icon.notify("Fehler beim Watchdog-Stop", "Fehler")
    threading.Thread(target=_stop, daemon=True).start()

def on_status(icon, item):
    """Zeigt Status der Datenbank."""
    try:
        import sqlite3
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM files")
        file_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM directories")
        dir_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT drive_id) FROM directories")
        drive_count = cursor.fetchone()[0]
        
        conn.close()
        
        status_msg = f"Dateien: {file_count:,}\nVerzeichnisse: {dir_count:,}\nLaufwerke: {drive_count}"
        icon.notify(status_msg, "Datenbank Status")
        logger.info(f"Status: {file_count} Dateien, {dir_count} Verzeichnisse, {drive_count} Laufwerke")
        
    except Exception as e:
        logger.error(f"Fehler beim Status abrufen: {e}")
        icon.notify("Fehler beim Status abrufen", "Fehler")

def on_settings(icon, item):
    """Öffnet Einstellungen (nicht-blockierend)."""
    logger.info("Öffne Einstellungen...")
    config_file = os.path.join(PROJECT_DIR, "config.json")
    try:
        if sys.platform == 'win32':
            subprocess.Popen(['notepad', config_file])
        else:
            subprocess.Popen(['xdg-open', config_file])
    except Exception as e:
        logger.error(f"Fehler beim Öffnen der Einstellungen: {e}")

def on_restart(icon, item):
    """Startet das Tray-Icon neu (sauber: erst stoppen, dann neuen Prozess starten)."""
    logger.info("Starte Tray-Anwendung neu...")

    python_exe = sys.executable
    script_path = os.path.abspath(__file__)

    # Erst Icon stoppen, dann neuen Prozess starten
    icon.stop()
    subprocess.Popen(
        [python_exe, script_path],
        cwd=PROJECT_DIR,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
    )

def on_quit(icon, item):
    """Beendet das Tray-Icon."""
    logger.info("Beende Tray-Anwendung...")
    icon.stop()

def create_custom_scripts_menu():
    """Erstellt Menüeinträge für benutzerdefinierte Scripts."""
    menu_items = []
    
    if os.path.isdir(SCRIPTS_DIR):
        try:
            for filename in sorted(os.listdir(SCRIPTS_DIR)):
                if filename.endswith('.py') and not filename.startswith('_'):
                    script_name = filename[:-3]  # Ohne .py
                    script_path = os.path.join("Dateien_Skripte", filename)
                    
                    # Schöne Namen für bekannte Scripts
                    display_names = {
                        'Dateisuche': '🔍 Dateisuche',
                        'Enhanced_Dateisuche': '🔎 Erweiterte Suche',
                        'Duplikat_Ordner_Manager': '📂 Duplikat-Manager',
                        'Speicherverbrauch.je.Ordner': '💾 Speicheranalyse',
                        'Sex.Songs': '🎵 Musik-Manager'
                    }
                    
                    display_name = display_names.get(script_name, f"▶ {script_name}")
                    
                    # Closure-Funktion für korrekte Bindung erstellen
                    def make_script_runner(script_path_local):
                        def runner(icon, item):
                            return run_script(script_path_local, DB_PATH)
                        return runner
                    
                    menu_items.append(
                        item(display_name, make_script_runner(script_path))
                    )
        except Exception as e:
            logger.error(f"Fehler beim Laden der benutzerdefinierten Scripts: {e}")
    
    return menu_items

def main():
    """Hauptfunktion - erstellt und startet das System-Tray."""
    logger.info("Starte System-Tray...")
    
    # Lade Konfiguration
    load_config()
    
    # Icon erstellen
    icon_image = create_icon()
    if not icon_image:
        logger.error("Konnte Icon nicht erstellen!")
        return
    
    # Benutzerdefinierte Scripts laden
    custom_scripts = create_custom_scripts_menu()
    
    # Haupt-Menü erstellen
    menu_items = [
        item('🏠 GUI öffnen', on_gui, default=True),  # Doppelklick-Aktion
        item('🔍 Dateisuche', on_search),
        pystray.Menu.SEPARATOR,
    ]
    
    # Scan-Menü
    scan_menu = pystray.Menu(
        item('🔄 Alle Laufwerke scannen', on_scan_all),
        item('📁 Aktuellen Pfad scannen', on_scan_current),
    )
    menu_items.append(item('💾 Scan', scan_menu))
    
    # Watchdog-Menü
    watchdog_menu = pystray.Menu(
        item('▶ Watchdog starten', on_watchdog_start),
        item('⏸ Watchdog stoppen', on_watchdog_stop),
    )
    menu_items.append(item('👁 Watchdog', watchdog_menu))
    
    # Tools-Menü
    tools_menu = pystray.Menu(
        item('🔍 Integritätsprüfung', on_integrity_check),
        item('📤 Export', on_export),
        item('📊 Status anzeigen', on_status),
    )
    menu_items.append(item('🔧 Tools', tools_menu))
    
    # Benutzerdefinierte Scripts
    if custom_scripts:
        menu_items.append(pystray.Menu.SEPARATOR)
        menu_items.append(item('📜 Scripts', pystray.Menu(*custom_scripts)))
    
    # Einstellungen und Beenden
    menu_items.extend([
        pystray.Menu.SEPARATOR,
        item('⚙ Einstellungen', on_settings),
        item('🔄 Tray neu starten', on_restart),
        item('❌ Beenden', on_quit),
    ])
    
    # Menü zusammenbauen
    menu = pystray.Menu(*menu_items)
    
    # Tray-Icon erstellen
    icon = pystray.Icon(
        name="DateiScanner",
        icon=icon_image,
        title="Datei Scanner System\n(Doppelklick für GUI)",
        menu=menu
    )
    
    # Icon starten
    logger.info("System-Tray bereit")
    try:
        icon.run()
    except Exception as e:
        logger.error(f"Fehler beim Starten des Tray-Icons: {e}")

if __name__ == "__main__":
    main()