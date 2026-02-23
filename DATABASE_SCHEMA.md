# DateiScanner - Vollständige Datenbankbeschreibung

## Übersicht

Die DateiScanner-Anwendung verwendet eine SQLite-Datenbank (`Dateien.db`) mit **8 Tabellen** zur Verwaltung von Dateisystem-Informationen, Scan-Fortschritt und Lösch-Historie.

**Aktuelle Datensätze:** 4,6 Millionen Dateien und Verzeichnisse über 12 Laufwerke

---

## Tabellen-Schema

### 1. **drives** - Laufwerksverwaltung
Zentrale Tabelle für alle überwachten Laufwerke.

```sql
CREATE TABLE drives (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);
```

**Spalten:**
- `id`: Auto-increment Primary Key
- `name`: Eindeutiger Laufwerksname (z.B. "C:/", "D:/")

**Constraints:**
- `UNIQUE(name)`: Jedes Laufwerk nur einmal
- `sqlite_autoindex_drives_1`: Automatischer Index auf name

**Aktuelle Daten:** 12 Laufwerke (D:/, T:/, M:/, etc.)

---

### 2. **directories** - Verzeichnisstruktur
Hierarchische Verzeichnisstruktur pro Laufwerk.

```sql
CREATE TABLE directories (
    id INTEGER PRIMARY KEY,
    drive_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    FOREIGN KEY (drive_id) REFERENCES drives (id) ON DELETE CASCADE,
    UNIQUE (drive_id, path)
);
```

**Spalten:**
- `id`: Auto-increment Primary Key
- `drive_id`: Referenz zu drives.id
- `path`: Vollständiger Verzeichnispfad

**Constraints:**
- `FOREIGN KEY`: Referentielle Integrität zu drives mit CASCADE
- `UNIQUE(drive_id, path)`: Ein Pfad pro Laufwerk nur einmal
- `idx_directories_drive_path`: Index für Performance

**Aktuelle Daten:** 323.564 Verzeichnisse

---

### 3. **files** - Dateiverwaltung
Alle gescannten Dateien mit Metadaten.

```sql
CREATE TABLE files (
    id INTEGER PRIMARY KEY,
    directory_id INTEGER NOT NULL,
    file_path TEXT UNIQUE NOT NULL,
    size INTEGER,
    hash TEXT,
    FOREIGN KEY (directory_id) REFERENCES directories (id) ON DELETE CASCADE
);
```

**Spalten:**
- `id`: Auto-increment Primary Key
- `directory_id`: Referenz zu directories.id
- `file_path`: Eindeutiger vollständiger Dateipfad
- `size`: Dateigröße in Bytes (optional)
- `hash`: SHA256-Hash (optional, konfigurierbar)

**Constraints:**
- `FOREIGN KEY`: Referentielle Integrität zu directories mit CASCADE
- `UNIQUE(file_path)`: Jeder Dateipfad systemweit eindeutig
- `idx_files_filepath`: Index auf file_path für Suchen
- `idx_files_directory_id`: Index auf directory_id für Joins

**Aktuelle Daten:** 3.118.873 Dateien

---

### 4. **scan_progress** - Scan-Fortschrittsverfolgung
Speichert Fortsetzungspunkte für unterbrochene Scans.

```sql
CREATE TABLE scan_progress (
    id INTEGER PRIMARY KEY,
    drive_id INTEGER UNIQUE NOT NULL,
    last_path TEXT,
    timestamp TEXT,
    FOREIGN KEY (drive_id) REFERENCES drives (id) ON DELETE CASCADE
);
```

**Spalten:**
- `id`: Auto-increment Primary Key
- `drive_id`: Eindeutige Referenz zu drives.id
- `last_path`: Letzter gescannter Pfad (für Resume-Funktion)
- `timestamp`: ISO-Format Zeitstempel

**Constraints:**
- `UNIQUE(drive_id)`: Ein Fortschrittseintrag pro Laufwerk
- `FOREIGN KEY`: Referentielle Integrität mit CASCADE

**Aktuelle Daten:** 12 Einträge (ein pro Laufwerk)

---

### 5. **scan_lock** - Scan-Koordination
Verhindert gleichzeitige Scans und verfolgt aktive Prozesse.

```sql
CREATE TABLE scan_lock (
    id INTEGER PRIMARY KEY,
    scan_type TEXT NOT NULL,
    start_time TEXT NOT NULL,
    pid INTEGER NOT NULL,
    hostname TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1
);
```

**Spalten:**
- `id`: Auto-increment Primary Key
- `scan_type`: Typ des Scans ("manual", "scheduled", etc.)
- `start_time`: ISO-Format Startzeitpunkt
- `pid`: Prozess-ID des Scanners
- `hostname`: Computer-Name
- `is_active`: Boolean (1=aktiv, 0=beendet)

**Verwendung:**
- Mutex-Mechanismus für Scanner
- Dead-Lock Detection über PID-Prüfung
- Multi-Host Support

**Aktuelle Daten:** 39 historische Lock-Einträge

---

### 6. **export_log** - Export-Protokoll
Protokolliert alle Datenexporte.

```sql
CREATE TABLE export_log (
    id INTEGER PRIMARY KEY,
    export_type TEXT,
    export_time TEXT,
    file_path TEXT
);
```

**Spalten:**
- `id`: Auto-increment Primary Key
- `export_type`: Format-Typ ("CSV", "JSON", "HTML")
- `export_time`: ISO-Format Zeitstempel
- `file_path`: Pfad der generierten Export-Datei

**Aktuelle Daten:** 0 Einträge (noch keine Exporte)

---

### 7. **deleted_directories** - Lösch-Historie Verzeichnisse
Auditlog für gelöschte Verzeichnisse.

```sql
CREATE TABLE deleted_directories (
    id INTEGER PRIMARY KEY,
    drive_id INTEGER,
    path TEXT NOT NULL,
    deleted_date TEXT NOT NULL
);
```

**Spalten:**
- `id`: Auto-increment Primary Key
- `drive_id`: Ursprüngliche Laufwerks-ID (optional)
- `path`: Vollständiger Pfad des gelöschten Verzeichnisses
- `deleted_date`: ISO-Format Löschzeitpunkt

**Zweck:**
- Audit-Trail für Integritätsprüfungen
- Recovery-Informationen
- Cleanup-Historie

**Aktuelle Daten:** 360.731 gelöschte Verzeichnisse

---

### 8. **deleted_files** - Lösch-Historie Dateien
Auditlog für gelöschte Dateien.

```sql
CREATE TABLE deleted_files (
    id INTEGER PRIMARY KEY,
    file_path TEXT NOT NULL,
    deleted_date TEXT NOT NULL
);
```

**Spalten:**
- `id`: Auto-increment Primary Key
- `file_path`: Vollständiger Pfad der gelöschten Datei
- `deleted_date`: ISO-Format Löschzeitpunkt

**Zweck:**
- Vollständige Lösch-Historie
- Forensische Analyse
- Recovery-Support

**Aktuelle Daten:** 1.168.850 gelöschte Dateien

---

## Referentielle Integrität

### CASCADE-Beziehungen:
```
drives (id) 
├── directories (drive_id) → ON DELETE CASCADE
│   └── files (directory_id) → ON DELETE CASCADE
└── scan_progress (drive_id) → ON DELETE CASCADE
```

**Bedeutung:**
- Löschen eines Laufwerks → Alle Verzeichnisse und Dateien werden gelöscht
- Löschen eines Verzeichnisses → Alle Dateien darin werden gelöscht
- Automatische Konsistenz ohne Waisen-Datensätze

---

## Performance-Optimierungen

### Indizes:
- **drives**: Automatischer Index auf `name` (UNIQUE)
- **directories**: 
  - `idx_directories_drive_path` (drive_id, path)
  - Automatischer UNIQUE-Index
- **files**:
  - `idx_files_filepath` (file_path) - für Suchen
  - `idx_files_directory_id` (directory_id) - für Joins
  - Automatischer UNIQUE-Index
- **scan_progress**: Automatischer UNIQUE-Index auf `drive_id`

### SQLite-Optimierungen:
- **WAL-Modus**: Write-Ahead Logging für bessere Concurrency
- **Batch-Inserts**: 1000 Dateien pro Transaction
- **Thread-Safe**: RLock-Mechanismus für Multi-Threading
- **Connection Pooling**: Singleton-Pattern mit globaler DB-Instanz

---

## Datenkonsistenz

### Trigger-System:
Die Anwendung nutzt vermutlich Trigger für:
- Automatisches Befüllen von `deleted_directories` bei DELETE auf `directories`
- Automatisches Befüllen von `deleted_files` bei DELETE auf `files`
- Zeitstempel-Verwaltung

### Transaktionale Sicherheit:
- Alle kritischen Operationen in Transaktionen
- Rollback bei Fehlern
- Isolation Level für Concurrency

---

## Speicherverbrauch

**Geschätzte Datenbankgröße:** ~500MB - 2GB (abhängig von Hash-Berechnung)

**Verteilung:**
- **files**: ~80% (3,1M Datensätze + Hashes)
- **directories**: ~15% (323k Datensätze)
- **deleted_files**: ~4% (1,2M historische Einträge)
- **deleted_directories**: ~1% (361k historische Einträge)
- **Sonstige**: <1%

---

## Wartung & Monitoring

### Automatische Bereinigung:
- Integritätsprüfer entfernt nicht-existente Dateien/Verzeichnisse
- Alte `scan_lock` Einträge werden bei Restart bereinigt
- Dead-PID Detection für verwaiste Locks

### Überwachung:
- Watchdog-Service für Echtzeit-Updates
- Scan-Progress für Resume-Funktionalität
- Export-Log für Audit-Zwecke

---

*Letzte Aktualisierung: $(Get-Date)*
*Datenbasis: SQLite 3.x mit Python sqlite3-Modul*