# DateiScanner - Datenbank-Optimierungsanalyse

## 🚨 Identifizierte Probleme der aktuellen Struktur

### **1. Massive Redundanz in file_path**
```
Aktuell: "C:/Users/Name/Documents/file.txt" (35+ Zeichen pro Datei)
Problem: Bei 3.118.873 Dateien = ~109 MB nur für Pfade!
```

### **2. Ineffiziente Extension-Suchen**
```sql
-- Aktuell: Volltext-Scan über 3M Dateien
SELECT * FROM files WHERE file_path LIKE '%.pdf';

-- Benötigt: Index-basierte Suche
SELECT * FROM files WHERE extension_id = (SELECT id FROM extensions WHERE name = '.pdf');
```

### **3. Keine gezielte Dateinamen-Suche**
```sql
-- Aktuell: Unmöglich ohne LIKE-Pattern
SELECT * FROM files WHERE file_path LIKE '%/config.%';

-- Optimal: Direkte Index-Suche
SELECT * FROM files WHERE filename = 'config';
```

---

## ✅ Optimierte Datenbankstruktur

### **Neues Schema-Design:**

```sql
-- 1. DRIVES (bereits optimal)
CREATE TABLE drives (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL  -- C:/, D:/, etc.
);

-- 2. EXTENSIONS (NEU)
CREATE TABLE extensions (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,  -- .txt, .pdf, .jpg, etc.
    category TEXT,              -- 'document', 'image', 'executable', etc.
    is_binary BOOLEAN DEFAULT 0
);

-- 3. DIRECTORIES (verbessert)
CREATE TABLE directories (
    id INTEGER PRIMARY KEY,
    drive_id INTEGER NOT NULL,
    parent_id INTEGER,          -- Hierarchie-Support
    directory_name TEXT NOT NULL,  -- Nur der Name, nicht der vollständige Pfad
    full_path TEXT NOT NULL,    -- Cache für Performance (optional)
    depth_level INTEGER,        -- Verzeichnistiefe für Optimierungen
    FOREIGN KEY (drive_id) REFERENCES drives (id) ON DELETE CASCADE,
    FOREIGN KEY (parent_id) REFERENCES directories (id) ON DELETE CASCADE,
    UNIQUE (drive_id, full_path)
);

-- 4. FILES (komplett überarbeitet)
CREATE TABLE files (
    id INTEGER PRIMARY KEY,
    directory_id INTEGER NOT NULL,
    filename TEXT NOT NULL,        -- Dateiname OHNE Extension
    extension_id INTEGER,          -- Referenz zu extensions
    size INTEGER,
    hash TEXT,
    created_date TEXT,            -- Erstellungsdatum
    modified_date TEXT,           -- Änderungsdatum
    attributes INTEGER DEFAULT 0,  -- Dateiattribute (Hidden, System, etc.)
    FOREIGN KEY (directory_id) REFERENCES directories (id) ON DELETE CASCADE,
    FOREIGN KEY (extension_id) REFERENCES extensions (id),
    INDEX idx_files_filename (filename),
    INDEX idx_files_extension (extension_id),
    INDEX idx_files_size (size),
    INDEX idx_files_directory (directory_id)
);

-- 5. FILE_PATHS (Materialized View für Kompatibilität)
CREATE VIEW file_paths AS
SELECT 
    f.id,
    d.full_path || '/' || f.filename || COALESCE(e.name, '') as full_path,
    f.size,
    f.hash,
    f.created_date,
    f.modified_date
FROM files f
JOIN directories d ON f.directory_id = d.id
LEFT JOIN extensions e ON f.extension_id = e.id;

-- 6. SCAN_PROGRESS (unverändert)
-- 7. SCAN_LOCK (unverändert)  
-- 8. EXPORT_LOG (unverändert)
-- 9. DELETED_* (erweitert mit normalized structure)
```

---

## 📊 Performance-Vergleich

### **Speicherverbrauch:**

| Component | Aktuell | Optimiert | Ersparnis |
|-----------|---------|-----------|-----------|
| **Pfade** | ~109 MB | ~25 MB | **77%** |
| **Extensions** | In Pfaden | ~50 KB | **99.9%** |
| **Filenames** | In Pfaden | ~40 MB | **Deduplizierung** |
| **Indizes** | 3 | 8+ | **Bessere Performance** |
| **TOTAL** | ~500 MB | ~280 MB | **44% kleiner** |

### **Query-Performance:**

```sql
-- VORHER: Volltext-Scan (3.118.873 Zeilen)
SELECT COUNT(*) FROM files WHERE file_path LIKE '%.pdf';
-- Geschätzt: 2-5 Sekunden

-- NACHHER: Index-Lookup
SELECT COUNT(*) FROM files f 
JOIN extensions e ON f.extension_id = e.id 
WHERE e.name = '.pdf';
-- Geschätzt: 0.01-0.05 Sekunden (100x schneller!)
```

### **Erweiterte Suchmöglichkeiten:**

```sql
-- 1. Alle großen PDF-Dateien
SELECT filename, size FROM files f
JOIN extensions e ON f.extension_id = e.id
WHERE e.name = '.pdf' AND size > 10000000;

-- 2. Duplicate Dateinamen mit verschiedenen Extensions
SELECT filename, COUNT(*) FROM files 
GROUP BY filename HAVING COUNT(*) > 1;

-- 3. Verzeichnisse nach Anzahl Dateien
SELECT d.full_path, COUNT(f.id) as file_count
FROM directories d
LEFT JOIN files f ON d.id = f.directory_id
GROUP BY d.id ORDER BY file_count DESC;

-- 4. Extension-Statistiken
SELECT e.name, e.category, COUNT(f.id) as count, AVG(f.size) as avg_size
FROM extensions e
LEFT JOIN files f ON e.id = f.extension_id
GROUP BY e.id ORDER BY count DESC;

-- 5. Dateien nach Verzeichnistiefe
SELECT d.depth_level, COUNT(f.id) as file_count
FROM files f
JOIN directories d ON f.directory_id = d.id
GROUP BY d.depth_level;
```

---

## 🔄 Migrationsstrategie

### **Phase 1: Schema erweitern (ohne Downtime)**
```sql
-- Neue Tabellen erstellen
CREATE TABLE extensions (...);
CREATE TABLE files_new (...);

-- Extensions populated
INSERT INTO extensions (name, category) 
SELECT DISTINCT 
    CASE 
        WHEN instr(file_path, '.') > 0 
        THEN substr(file_path, instr(file_path, '.', -1))
        ELSE ''
    END as ext,
    CASE 
        WHEN ext IN ('.pdf', '.doc', '.docx', '.txt') THEN 'document'
        WHEN ext IN ('.jpg', '.png', '.gif', '.bmp') THEN 'image'
        WHEN ext IN ('.exe', '.dll', '.sys') THEN 'executable'
        ELSE 'other'
    END as category
FROM files;
```

### **Phase 2: Daten migrieren**
```sql
-- Batch-Migration (100k Dateien pro Batch)
INSERT INTO files_new (directory_id, filename, extension_id, size, hash)
SELECT 
    f.directory_id,
    CASE 
        WHEN instr(basename, '.') > 0 
        THEN substr(basename, 1, instr(basename, '.', -1) - 1)
        ELSE basename
    END as filename,
    e.id as extension_id,
    f.size,
    f.hash
FROM files f
JOIN (
    SELECT file_path, 
           substr(file_path, instr(file_path, '/', -1) + 1) as basename
    FROM files
) path_parts ON f.file_path = path_parts.file_path
LEFT JOIN extensions e ON e.name = substr(basename, instr(basename, '.', -1))
LIMIT 100000 OFFSET ?;
```

### **Phase 3: Cutover**
```sql
-- Atomarer Tabellenwechsel
BEGIN TRANSACTION;
ALTER TABLE files RENAME TO files_old;
ALTER TABLE files_new RENAME TO files;
-- Views und Indizes aktualisieren
COMMIT;
```

### **Phase 4: Cleanup**
```sql
-- Nach Verifikation
DROP TABLE files_old;
VACUUM; -- Speicher zurückgewinnen
```

---

## 🎯 Implementierungs-Prioritäten

### **Quick Wins (niedrige Komplexität):**
1. **Extensions-Tabelle** hinzufügen
2. **Filename/Extension-Split** in files-Tabelle
3. **Grundlegende Indizes** auf neue Felder

### **Medium-Term (mittlere Komplexität):**
1. **Directory-Hierarchie** mit parent_id
2. **Materialized Views** für Kompatibilität
3. **Migration-Scripts** entwickeln

### **Long-Term (hohe Komplexität):**
1. **Vollständige Migration** der bestehenden Daten
2. **Application-Code Updates** für neue Struktur
3. **Advanced Features** (Kategorien, Metadaten)

---

## 🔧 Anpassungen am Application Code

### **Modifikationen in models.py:**

```python
class OptimizedDBManager(DBManager):
    def ensure_schema(self):
        # Erweiterte Tabellen-Definitionen
        self.create_extensions_table()
        self.create_optimized_files_table()
        # ... etc
    
    def get_or_create_extension(self, ext_name):
        cursor.execute("SELECT id FROM extensions WHERE name = ?", (ext_name,))
        # ... implementation
    
    def insert_file_optimized(self, dir_id, full_filename, size, hash_val):
        filename, ext = os.path.splitext(full_filename)
        ext_id = self.get_or_create_extension(ext) if ext else None
        cursor.execute(
            "INSERT INTO files (directory_id, filename, extension_id, size, hash) VALUES (?,?,?,?,?)",
            (dir_id, filename, ext_id, size, hash_val)
        )
```

---

## 📈 Erwartete Verbesserungen

### **Performance:**
- **100x schnellere** Extension-Suchen
- **50x schnellere** Filename-Suchen
- **10x schnellere** Verzeichnis-Analysen

### **Speicher:**
- **44% weniger** Datenbankgröße
- **Bessere Kompression** durch Normalisierung
- **Effizientere Indizes**

### **Funktionalität:**
- **Erweiterte Statistiken** (Extensions, Größen, etc.)
- **Datei-Kategorisierung** (Document, Image, etc.)
- **Hierarchie-Analysen** (Verzeichnistiefe, etc.)
- **Duplicate-Detection** (gleiche Dateinamen)

### **Wartbarkeit:**
- **Sauberere Code-Struktur**
- **Bessere Testbarkeit**
- **Einfachere Feature-Erweiterungen**

---

**Fazit:** Die vorgeschlagene Optimierung würde die Datenbank erheblich verbessern und ist den Aufwand definitiv wert!