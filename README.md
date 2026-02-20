<p align="center">
  <img src="https://img.shields.io/badge/python-3.10%2B-blue?style=flat-square&logo=python&logoColor=white" alt="Python 3.10+">
  <img src="https://img.shields.io/badge/platform-Windows-0078D6?style=flat-square&logo=windows&logoColor=white" alt="Windows">
  <img src="https://img.shields.io/badge/database-SQLite-003B57?style=flat-square&logo=sqlite&logoColor=white" alt="SQLite">
  <img src="https://img.shields.io/badge/license-MIT-green?style=flat-square" alt="MIT License">
</p>

# filescan-db

A high-performance Windows filesystem scanner that builds and maintains a normalized SQLite database of all file metadata across multiple drives. Combines scheduled batch scanning with real-time watchdog monitoring for an always-current filesystem index.

---

## Features

- **Batch Scanner** with checkpoint-based resume — interrupted scans continue where they left off
- **Real-Time Watchdog** monitors filesystem events (create, modify, move, delete) and updates the database instantly
- **Multi-Drive Support** with automatic network alias detection (prevents duplicate entries when the same share is mapped to multiple drive letters)
- **PyQt5 Control Panel** with tabs for scanning, log viewing, settings, and statistics
- **System Tray Integration** for background operation with quick access to all functions
- **Integrity Checker** verifies database-filesystem consistency and detects drift
- **Multi-Format Export** to CSV, JSON, and HTML
- **Scheduled Scans** via configurable cron-like schedule (integrity checks, full rescans)
- **Windows Service** support via NSSM for production deployments
- **Portable** — runs from USB drives, no installation required

## Architecture

```
                    +-----------------+
                    |  gui_launcher   |  PyQt5 Control Panel
                    +--------+--------+
                             |
              +--------------+--------------+
              |              |              |
     +--------v---+  +------v------+  +----v--------+
     | scanner_core|  |  watchdog   |  |  exporter   |
     | (batch scan)|  |  (realtime) |  | (csv/json)  |
     +--------+---+  +------+------+  +----+--------+
              |              |              |
              +--------------+--------------+
                             |
                    +--------v--------+
                    |    models.py    |  Thread-safe SQLite
                    |   (DBManager)   |  WAL mode, FK, RLock
                    +-----------------+
```

**Core Modules:**

| Module | Purpose |
|--------|---------|
| `models.py` | Thread-safe singleton DBManager with SQLite WAL mode, foreign keys, and in-memory LRU file cache |
| `scanner_core.py` | Batch scanner with resume capability, optional SHA256 hashing, and transaction-based inserts |
| `watchdog_monitor.py` | Real-time filesystem event handler with thread-safe DB updates and auto-reconnect |
| `watchdog_service.py` | Long-running service wrapper with heartbeat monitoring and retry logic |
| `watchdog_control.py` | Process management for starting/stopping the watchdog safely |
| `drive_alias_detector.py` | Detects network drive aliases to prevent duplicate database entries |
| `gui_launcher.py` | PyQt5 GUI with worker threads communicating via Qt signals |
| `systray_launcher_full.py` | System tray with NSSM service integration and Explorer icon |
| `integrity_checker.py` | Database-filesystem consistency verification |
| `exporter.py` | Export to CSV, JSON, and HTML with path filtering |

## Quick Start

### Prerequisites

- Python 3.10+ on Windows
- Install dependencies:
  ```bash
  pip install -r requirements.txt
  ```

### First Scan

```bash
# Scan a single drive
python scanner_core.py "C:/" --restart

# Scan all available drives
python scan_all_drives.py

# Launch the GUI
python gui_launcher.py
```

### Real-Time Monitoring

```bash
# Start watchdog for a specific path
python watchdog_monitor.py "C:/"

# Or run as background service (recommended)
python watchdog_service.py
```

### System Tray (Background Operation)

```bash
pythonw systray_launcher_full.py
```

### Windows Service (Production)

```bash
# Install watchdog as Windows service (requires Admin + NSSM in tools/)
install_autostart.bat

# Repair existing service configuration
fix_watchdog_autostart.bat
```

## Database

Normalized SQLite schema with 8 tables:

| Table | Purpose |
|-------|---------|
| `drives` | Drive letters (C:/, D:/, ...) |
| `directories` | Hierarchical directory tree with parent references |
| `files` | File metadata (name, size, mtime, optional SHA256 hash) |
| `extensions` | File extensions with category classification |
| `scan_progress` | Checkpoint tracking for scan resume |
| `scan_lock` | Concurrency control (PID + hostname based) |
| `deleted_files` | Audit trail of removed files |
| `deleted_directories` | Audit trail of removed directories |

**Key guarantees:**
- Foreign keys with CASCADE deletes
- UNIQUE constraint on `(directory_id, filename)` prevents duplicates
- WAL mode with 60s busy timeout for concurrent access
- All operations protected by `threading.RLock()`

## Configuration

Settings are stored in `config.json`:

```json
{
    "base_path": "C:/",
    "hashing": false,
    "export_formats": ["csv", "json", "html"],
    "log_level": "WARNING",
    "resume_scan": true,
    "scheduled_scans": [
        {
            "scan_type": "integrity",
            "time": "06:00",
            "enabled": true
        }
    ]
}
```

## Utility Scripts

Located in `Dateien_Skripte/`:

- **Duplicate Finder** — detect files with identical hashes
- **File Search** — fast filename/path search across the database
- **Disk Usage Analysis** — directory size breakdown
- **Playlist Management** — generate playlists from indexed media files

## Dependencies

```
watchdog          # Filesystem event monitoring
PyQt5             # GUI framework
pystray           # System tray integration
Pillow            # Image processing
psutil            # Process management
pyinstaller       # Building portable executables
```

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
