# DateiScanner – Datenbankschema

> **Maßgeblich ist `models.py` (`DBManager.ensure_schema`).** Dieses Dokument
> beschreibt das tatsächlich erzeugte, normalisierte Schema (Stand 2026-05-29).
> Frühere Fassungen dieses Dokuments beschrieben ein veraltetes Schema
> (`directories.path`, `files.file_path`) – das ist nicht mehr aktuell.

## Übersicht

SQLite-Datenbank (`Dateien.db`, WAL-Modus) mit **9 Tabellen** und **1 View** zur
Verwaltung von Dateisystem-Metadaten, Scan-Fortschritt und Lösch-Historie.
Aktuelle Datenmengen lassen sich jederzeit über `db_health_probe.py` ermitteln.

---

## Tabellen

### 1. `drives` – Laufwerke
```sql
CREATE TABLE drives (
    id   INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL        -- z. B. 'C:/', 'D:/'
);
```

### 2. `extensions` – Dateiendungen mit Kategorisierung
```sql
CREATE TABLE extensions (
    id        INTEGER PRIMARY KEY,
    name      TEXT UNIQUE NOT NULL,  -- '.txt', '.pdf', ... bzw. '[none]'
    category  TEXT,                  -- 'document' | 'image' | 'video' | 'audio'
                                     -- 'archive' | 'executable' | 'code' | 'other'
    is_binary BOOLEAN DEFAULT 0,
    mime_type TEXT
);
```
Wird beim Schema-Aufbau mit Standard-Endungen vorbefüllt
(`_populate_standard_extensions`). Dateien ohne Endung verwenden `'[none]'`.

### 3. `directories` – Verzeichnisbaum (hierarchisch)
```sql
CREATE TABLE directories (
    id             INTEGER PRIMARY KEY,
    drive_id       INTEGER NOT NULL,
    parent_id      INTEGER,              -- Hierarchie (self-reference)
    directory_name TEXT NOT NULL,        -- nur der Name, nicht der ganze Pfad
    full_path      TEXT NOT NULL,        -- vollständiger Pfad (Forward-Slashes)
    depth_level    INTEGER DEFAULT 0,
    FOREIGN KEY (drive_id)  REFERENCES drives (id)      ON DELETE CASCADE,
    FOREIGN KEY (parent_id) REFERENCES directories (id) ON DELETE CASCADE,
    UNIQUE (drive_id, full_path)
);
```

### 4. `files` – Dateien
```sql
CREATE TABLE files (
    id            INTEGER PRIMARY KEY,
    directory_id  INTEGER NOT NULL,
    filename      TEXT NOT NULL,         -- Dateiname OHNE Endung
    extension_id  INTEGER,               -- Referenz auf extensions
    size          INTEGER,
    hash          TEXT,                  -- SHA256, optional (konfigurierbar)
    created_date  TEXT,
    modified_date TEXT,
    attributes    INTEGER DEFAULT 0,
    FOREIGN KEY (directory_id) REFERENCES directories (id) ON DELETE CASCADE,
    FOREIGN KEY (extension_id) REFERENCES extensions  (id)
);
```
Eindeutigkeit über den UNIQUE-Index `idx_files_directory_filename
(directory_id, filename)`. Der vollständige Dateipfad ist **nicht** gespeichert,
sondern wird bei Bedarf aus `directories.full_path + filename + extension`
rekonstruiert (siehe View `files_legacy`).

### 5. `scan_progress` – Fortsetzungspunkte
```sql
CREATE TABLE scan_progress (
    id        INTEGER PRIMARY KEY,
    drive_id  INTEGER UNIQUE NOT NULL,
    last_path TEXT,
    timestamp TEXT,
    FOREIGN KEY (drive_id) REFERENCES drives (id) ON DELETE CASCADE
);
```

### 6. `scan_lock` – Scan-Koordination (Mutex)
```sql
CREATE TABLE scan_lock (
    id         INTEGER PRIMARY KEY,
    scan_type  TEXT NOT NULL,           -- 'manual' | 'scheduled' | ...
    start_time TEXT NOT NULL,
    pid        INTEGER NOT NULL,
    hostname   TEXT NOT NULL,
    is_active  INTEGER NOT NULL DEFAULT 1
);
```
Dead-Lock-Erkennung über PID-/Hostname-Validierung (`acquire_scan_lock`).

### 7. `export_log` – Export-Protokoll
```sql
CREATE TABLE export_log (
    id          INTEGER PRIMARY KEY,
    export_type TEXT,                   -- 'CSV' | 'JSON' | 'HTML'
    export_time TEXT,
    file_path   TEXT
);
```

### 8. `deleted_directories` – Audit-Trail gelöschter Verzeichnisse
```sql
CREATE TABLE deleted_directories (
    id           INTEGER PRIMARY KEY,
    drive_id     INTEGER,
    full_path    TEXT NOT NULL,
    deleted_date TEXT NOT NULL
);
```

### 9. `deleted_files` – Audit-Trail gelöschter Dateien
```sql
CREATE TABLE deleted_files (
    id           INTEGER PRIMARY KEY,
    directory_id INTEGER,
    filename     TEXT NOT NULL,
    extension_id INTEGER,
    deleted_date TEXT NOT NULL
);
```

---

## View

### `files_legacy` – Kompatibilitäts-View (rekonstruierter Pfad)
```sql
CREATE VIEW files_legacy AS
SELECT f.id,
       f.directory_id,
       d.full_path || '/' || f.filename || COALESCE(e.name, '') AS file_path,
       f.size,
       f.hash
FROM files f
JOIN directories d ON f.directory_id = d.id
LEFT JOIN extensions e ON f.extension_id = e.id;
```
> ⚠️ Hinweis: Für Dateien ohne Endung hängt die View das Platzhalter-Token
> `'[none]'` an den Pfad an. Code, der echte Pfade benötigt (z. B.
> `cleanup_removed_files`), behandelt `'[none]'` daher gesondert.

---

## Indizes (`ensure_schema`)

| Index | Spalten | Zweck |
|-------|---------|-------|
| `idx_directories_drive_path`      | `(drive_id, full_path)`            | Verzeichnis-Lookup |
| `idx_directories_parent`          | `(parent_id)`                      | Hierarchie-Traversal |
| `idx_files_filename`              | `(filename)`                       | Namenssuche |
| `idx_files_extension`             | `(extension_id)`                   | Filter nach Typ |
| `idx_files_directory`             | `(directory_id)`                   | Join files↔directories |
| `idx_files_size`                  | `(size)`                           | Größen-Sortierung/-Filter |
| `idx_extensions_name` *(UNIQUE)*  | `(name)`                           | Extension-Lookup |
| `idx_files_directory_filename` *(UNIQUE)* | `(directory_id, filename)` | Eindeutigkeit / Upsert |
| `idx_files_hash`                  | `(hash)`                           | Duplikat-Erkennung |
| `idx_extensions_category`         | `(category)`                       | Kategorie-Filter |
| `idx_files_name_ext_size`         | `(filename, extension_id, size)`   | Duplikat-Heuristik |

---

## Referentielle Integrität

```
drives (id)
├── directories (drive_id)        → ON DELETE CASCADE
│   ├── directories (parent_id)   → ON DELETE CASCADE (self-reference)
│   └── files (directory_id)      → ON DELETE CASCADE
└── scan_progress (drive_id)      → ON DELETE CASCADE

files (extension_id) → extensions (id)   [kein CASCADE]
```

`PRAGMA foreign_keys = ON` wird in `DBManager.__init__` gesetzt; CASCADE-Deletes
sind damit aktiv.

---

## SQLite-Konfiguration

- **WAL-Modus** (`PRAGMA journal_mode = WAL`) für bessere Nebenläufigkeit
- **`busy_timeout = 60000`** (60 s) gegen `database is locked`
- **Thread-Sicherheit** über `threading.RLock()` (globaler `_db_lock`)
- **Singleton-DBManager** (`get_db_instance`) mit einer geteilten Verbindung
- **Batch-Inserts** mit In-Memory-`FileCache` zur Reduktion von DB-Lookups

*Letzte Aktualisierung: 2026-05-29 – abgeglichen mit `models.py`.*
