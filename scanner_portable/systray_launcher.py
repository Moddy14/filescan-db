import os
import sys
import subprocess
import threading
from PIL import Image, ImageDraw
import pystray
import logging

# Importiere den globalen Logger aus utils
from utils import logger, CONFIG, PROJECT_DIR, load_config, save_config

def run_script(script_name, use_config_path=False, path_arg=None):
    """Startet ein Python-Skript als Subprozess.

    Args:
        script_name: Der Dateiname des Skripts im Projektverzeichnis.
        use_config_path: Wenn True, wird der 'base_path' aus der Konfiguration
                         als Argument übergeben (falls vorhanden).
        path_arg: Ein spezifischer Pfad, der als Argument übergeben wird
                  (hat Vorrang vor use_config_path).
    """
    script_path = os.path.join(PROJECT_DIR, script_name)
    if not os.path.exists(script_path):
        logger.error(f"[Tray Fehler] Skript nicht gefunden: {script_path}")
        return

    command = [sys.executable, script_path]
    final_path_arg = None

    if path_arg:
        final_path_arg = path_arg
    elif use_config_path:
        config_base_path = CONFIG.get('base_path')
        if config_base_path and os.path.exists(config_base_path):
            final_path_arg = config_base_path
        else:
             logger.warning(f"[Tray Warnung] Konfigurierter Basispfad für {script_name} fehlt oder ist ungültig.")
             # Optional: GUI starten, um Pfad zu setzen?
             # run_script("gui_launcher.py")
             # Aktuell: Skript ohne Pfad starten (falls möglich) oder nichts tun
             if script_name not in ["gui_launcher.py", "exporter.py", "integrity_checker.py"]:
                 # Scan und Watchdog benötigen einen Pfad
                 logger.error(f"[Tray Fehler] {script_name} erfordert einen gültigen Basispfad.")
                 # Optional: User benachrichtigen (schwierig aus Tray)
                 return

    if final_path_arg:
         command.append(final_path_arg)

    try:
        # Starte den Prozess
        subprocess.Popen(command)
        logger.info(f"[Tray Info] Starte: {' '.join(command)}")
    except Exception as e:
        logger.error(f"[Tray Fehler] Start von {' '.join(command)} fehlgeschlagen: {e}")

def create_icon_image(color="blue"):
    """Erstellt ein einfaches Icon-Bild für das Tray."""
    try:
        image = Image.new("RGBA", (64, 64), (0, 0, 0, 0)) # Transparenter Hintergrund
        dc = ImageDraw.Draw(image)
        # Zeichne einen Kreis
        dc.ellipse((8, 8, 56, 56), fill=color, outline="black", width=2)
        # Optional: Buchstabe oder Symbol hinzufügen
        # font = ImageFont.truetype("arial.ttf", 32)
        # dc.text((32, 32), "S", fill="white", anchor="mm", font=font)
        return image
    except Exception as e:
        logger.error(f"[Tray Fehler] Konnte Icon nicht erstellen: {e}")
        # Fallback: Kein Icon (pystray sollte Standard verwenden)
        return None

# --- Menü-Aktionen --- 
def on_gui(icon, item):
    logger.info("[Tray Action] Öffne GUI...")
    run_script("gui_launcher.py")

def on_scan(icon, item):
    logger.info("[Tray Action] Starte Scan (via Core)...")
    run_script("scanner_core.py", use_config_path=True)

def on_integrity(icon, item):
    # Führt globale Prüfung durch (ohne Pfad)
    logger.info("[Tray Action] Starte globale Integritätsprüfung...")
    run_script("integrity_checker.py")

def on_export(icon, item):
    # Führt globalen Export durch (ohne Filter)
    logger.info("[Tray Action] Starte globalen Export...")
    run_script("exporter.py")

def on_exit(icon, item):
    logger.info("[Tray Info] Beende Tray-Anwendung...")
    icon.stop()

# --- Hauptfunktion --- 
def start_systray():
    """Erstellt und startet das System-Tray-Icon."""
    icon_image = create_icon_image()
    icon = pystray.Icon("DateiScanner", icon=icon_image, title="Datei Scanner")

    # Dynamisches Menü basierend auf Konfiguration?
    # Z.B.: Scan/Watchdog deaktivieren, wenn kein base_path gesetzt ist?
    base_path_valid = bool(CONFIG.get('base_path') and os.path.isdir(CONFIG.get('base_path')))

    icon.menu = pystray.Menu(
        pystray.MenuItem("🖥 GUI öffnen", on_gui),
        pystray.MenuItem("📂 Scan starten (Basis-Pfad)", on_scan, enabled=base_path_valid),
        pystray.MenuItem("--- Scanner ---", None), # Separator
        pystray.MenuItem("🧪 Globale Integritätsprüfung", on_integrity),
        pystray.MenuItem("📤 Globaler Export", on_export),
        pystray.MenuItem("--- Anwendung ---", None), # Separator
        pystray.MenuItem("❌ Beenden", on_exit)
    )

    logger.info("[Tray Info] Tray-Anwendung wird gestartet...")
    # Starte pystray in einem eigenen Thread, um den Hauptthread nicht zu blockieren,
    # falls dies aus einem anderen Skript aufgerufen wird.
    # Wenn dies das Hauptskript ist, ist icon.run() blockierend.
    icon.run() # Blockiert, bis on_exit aufgerufen wird
    logger.info("[Tray Info] Tray-Anwendung beendet.")

if __name__ == "__main__":
    # Stelle sicher, dass die DB initialisiert wird (falls noch nicht geschehen)
    # Dies kann helfen, frühe Fehler abzufangen, bevor das Tray startet.
    try:
        from models import get_db_instance
        get_db_instance()
    except Exception as e:
        logger.error(f"[Tray Kritischer Fehler] DB konnte nicht initialisiert werden: {e}. Tray startet möglicherweise nicht korrekt.")
        # Nicht abbrechen, da Tray evtl. nur GUI starten soll.

    # Starte das Tray in einem separaten Thread, damit das Hauptprogramm
    # (falls es eines gäbe) weiterlaufen könnte. Für den reinen Tray-Start
    # ist es nicht zwingend nötig, aber schadet auch nicht.
    tray_thread = threading.Thread(target=start_systray, daemon=True)
    tray_thread.start()

    # Halte den Hauptthread am Leben, solange der Tray-Thread läuft
    # (Nötig, wenn daemon=True gesetzt ist, sonst würde das Programm sofort enden)
    try:
        while tray_thread.is_alive():
            tray_thread.join(timeout=1.0) # Warte auf Thread-Ende oder Timeout
    except KeyboardInterrupt:
        logger.info("[Tray Info] Beenden durch Benutzer (KeyboardInterrupt im Hauptthread).")
        # Versuche, das Icon zu stoppen (funktioniert evtl. nicht zuverlässig)
        # Besser ist es, das 'Beenden'-Menü im Tray zu verwenden.
