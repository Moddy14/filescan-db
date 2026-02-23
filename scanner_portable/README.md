## ✅ `README.md` – vollständige Dokumentation für Dein Projekt

Wird automatisch im Projektverzeichnis `scanner_project/` abgelegt.

```markdown
# 🧠 Intelligenter Datei-Scanner für Windows

Dieses Projekt bietet ein **lokales, robustes Datei-Überwachungs- und Analysesystem** mit:

- 🔍 Fortsetzbarem Dateiscan mit optionalem SHA256
- 🧪 Integritätsprüfung der Dateistruktur
- 📡 Echtzeitüberwachung über watchdog
- 💾 SQLite-Datenbank mit Historie gelöschter Elemente
- 📤 Exportfunktionen: CSV, JSON, HTML
- 🖥 GUI zur Steuerung & Loganzeige

---

## 📁 Projektstruktur

```
scanner_project/
├── Dateien.db                 # SQLite-Datenbank mit Triggern & Tabellen
├── scanner_core.py           # Batch-Scanner mit Restart-Funktion
├── watchdog_monitor.py       # Überwachung für Live-Dateiänderungen
├── integrity_checker.py      # Täglicher Datenbank-FS-Abgleich
├── gui_launcher.py           # Benutzerfreundliche GUI
├── exporter.py               # Exportmodul (csv/json/html)
├── models.py                 # Gemeinsame DB-Schicht
├── config.json               # Konfigurierbare Parameter
├── scanner.log               # Laufende Logausgabe
├── exports/                  # Exportierte Dateilisten
└── run_all.bat               # Automatisierungs-Skript (siehe unten)
```

---

## ⚙ Konfiguration (`config.json`)

```json
{
  "log_file": "scanner.log",
  "hashing": true,
  "include_network_drives": true,
  "export_formats": ["csv", "json", "html"],
  "default_drive": "C:/",
  "rescan_hour": 3,
  "batch_size": 1000
}
```

---

## 🚀 Anwendung

### 1. Setup starten

```bash
python setup_scanner_project.py
```

### 2. Scan ausführen

```bash
python scanner_core.py "C:/BeispielOrdner"
```

### 3. Überwachung starten

```bash
python watchdog_monitor.py "C:/BeispielOrdner"
```

### 4. Integritätsprüfung starten

```bash
python integrity_checker.py
```

### 5. Export starten

```bash
python exporter.py
```

### 6. GUI starten

```bash
python gui_launcher.py
```

---

## 🗓 Geplante Automatisierung

Verwende den Taskplaner oder die folgende Batch-Datei für tägliche Abläufe:

---

## ✅ `run_all.bat` – Automatisierung via Taskplaner

```bat
@echo off
SET BASEDIR=%~dp0
cd /d %BASEDIR%

REM -- Integritätsprüfung um 3:00 Uhr --
echo [INFO] Starte Integritätsprüfung...
python integrity_checker.py

REM -- Export --
echo [INFO] Exportiere Dateidaten...
python exporter.py

REM -- Überwachung starten (optional dauerhaft) --
REM start "" python watchdog_monitor.py "C:\DeinVerzeichnis"
```

> 💡 Hinweis: Diese Datei kannst Du im Windows-Taskplaner täglich ausführen lassen.

---

## ✅ Empfehlung

- `scanner_core.py` nur 1× initial oder manuell ausführen
- `watchdog_monitor.py` dauerhaft laufen lassen (Taskplaner bei Login starten)
- `integrity_checker.py` 1× täglich per Zeitplan
- `exporter.py` nach Bedarf oder automatisch nach dem Rescan

---

## 🧰 Voraussetzungen

Die folgenden Änderungen wurden vorgenommen:
- Verbesserte Fehlerbehandlung in Batch-Dateien
- Erweiterte Logik für den Systray-Launcher

- Python 3.9+
- Pakete: `pip install watchdog PyQt5`

---

Wenn Du möchtest, liefere ich Dir als Nächstes:

- Installations-Wrapper (`install_requirements.bat`)
- Systray-Anwendung mit Tray-Icon

Sag einfach Bescheid.