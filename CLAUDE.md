# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
Windows file scanner and monitoring system that maintains a SQLite database of filesystem information. Features batch scanning with resume capability, real-time monitoring via watchdog, integrity checking, and export functionality.

The system tracks file metadata including paths, sizes, modification times, and optional SHA256 hashes. All data is stored in a normalized SQLite database with foreign key constraints and transaction safety.

## Key Commands

### Core Operations
```bash
# Initial full scan with database reset
python scanner_core.py "C:/Path/To/Scan" --restart

# Resume interrupted scan from checkpoint
python scanner_core.py "C:/Path/To/Scan"

# Scan all available drives
python scan_all_drives.py

# Real-time file monitoring
python watchdog_monitor.py "C:/Path/To/Monitor"

# Database integrity check
python integrity_checker.py

# Export database (CSV/JSON/HTML)
python exporter.py
```

### GUI and Services
```bash
# Main GUI control panel (PyQt5-based)
python gui_launcher.py

# System tray with all features
python systray_launcher_full.py

# Quick system test (foreign keys, integrity)
quick_test.bat

# Full system test (comprehensive validation)
full_system_test.bat

# Critical bug tests (regression testing)
run_critical_tests.bat

# Stop all running services
stop_all_services.bat
```

### Build and Deployment
```bash
# Build portable executable
python build_portable.py

# Build release package
python build_release_package.py

# Update database schema
python update_db_schema.py
```

## Architecture

### Core Components
- **models.py**: Thread-safe singleton DBManager with SQLite WAL mode
  - Tables: drives, extensions, directories, files, scan_progress, scan_lock, deleted_files, deleted_directories
  - Foreign keys enforced with CASCADE deletes
  - Scan locking prevents concurrent operations (PID + hostname based dead-lock detection)
  - Singleton pattern via `get_db_instance()` — can reconnect to different DB file mid-execution
  - `FileCache` class: in-memory LRU cache (100k entries) of `(directory_id, filename)` tuples to optimize INSERT-vs-UPDATE decisions
  - `@with_lock` decorator wraps all public methods with `threading.RLock()`

- **scanner_core.py**: Main batch scanner
  - Resume capability via scan_progress table (last_path set to NULL signals completion)
  - Optional SHA256 hashing (configurable globally or per-directory)
  - Transaction-based batch inserts (1000 files per batch)
  - --restart flag for clean rescan (only affects the scanned drive)
  - Skips problematic Windows directories (servicing/LCU, WinSxS/Backup, SoftwareDistribution, etc.)
  - Uses `watchdog_control.pause_watchdog_for_scan()` to prevent conflicts

- **watchdog_monitor.py**: Real-time filesystem monitoring
  - `FSHandler(FileSystemEventHandler)` — one per watched path
  - Filters 15+ system directories (Windows, AppData, $RECYCLE.BIN, etc.)
  - Ignores temp files and SQLite journals (.tmp, .log, .db-wal, .db-shm)
  - Uses `drive_alias_detector.py` to normalize network drive aliases (e.g., T:\ → O:\)
  - Thread-safe database updates, auto-reconnect on errors

- **watchdog_service.py**: Long-running service wrapper
  - Starts Observer threads for all canonical drives
  - Heartbeat monitoring (logs every 10 minutes)
  - Retry mechanism: 3 attempts with 30-second delays
  - Can run via Windows Scheduler or NSSM (Tools/nssm.exe)

- **watchdog_control.py**: Service controller
  - `stop_watchdog()` / `start_watchdog()` — process management via psutil
  - `pause_watchdog_for_scan()` — decorator to pause watchdog during manual scans
  - Launches via pythonw.exe (no console window)

- **drive_alias_detector.py**: Network drive alias normalization
  - Detects when multiple drive letters point to the same physical location
  - `normalize_path_with_aliases()` and `get_canonical_drive_list()` prevent duplicate DB entries

- **utils.py**: Shared utilities
  - Global logger with rotation (10MB x 5 backups)
  - `CONFIG` global dict — loaded once at import time, persisted via `save_config()`
  - Hash calculation (SHA256), drive enumeration

- **gui_launcher.py**: PyQt5 control panel with tabs (Scan, Logs, Settings, Statistics)
  - Worker threads (ScanWorker, LogUpdater) communicate via Qt Signal-Slot pattern
  - Controls watchdog via watchdog_control.py

- **scheduled_scanner.py**: Scheduled scan execution based on config.json schedule entries

- **scan_status_monitor.py**: Monitors and displays current scan progress

### Subdirectories
- **Dateien_Skripte/**: Utility scripts (duplicate finder, file search, disk usage analysis, playlist management)
- **Dateien_Anweisungen/**: Reference documentation files
- **Tools/**: Contains nssm.exe for Windows service management
- **exports/**: Output directory for CSV/JSON/HTML exports
- **test_scan_dir/**: Sample files for testing scan logic

### Configuration (config.json)
```json
{
    "base_path": "O:/",
    "hashing": false,
    "export_formats": ["csv", "json", "html"],
    "hash_directories": [],
    "log_level": "WARNING",
    "resume_scan": true,
    "scheduled_scans": [
        {
            "scan_type": "integrity",
            "path": null,
            "time": "06:00",
            "enabled": true,
            "restart": false
        }
    ],
    "watchdog_auto_paths": ["C:/Laufwerk T/USB16GB/Programme"]
}
```

### Cross-File Dependencies
```
scanner_core.py → models.py, utils.py, watchdog_control.py
gui_launcher.py → models.py, utils.py, watchdog_control.py, PyQt5
watchdog_monitor.py → models.py, utils.py, drive_alias_detector.py, watchdog
watchdog_service.py → watchdog_monitor.py, models.py, utils.py, drive_alias_detector.py
watchdog_control.py → psutil, subprocess
exporter.py → models.py, utils.py
integrity_checker.py → models.py, utils.py
```

## Critical Implementation Notes

1. **Database Locking**: All operations use `threading.RLock()` via `@with_lock` decorator
2. **WAL Mode**: SQLite uses Write-Ahead Logging; 60-second busy_timeout for lock contention
3. **Foreign Keys**: Always enabled via `PRAGMA foreign_keys = ON`, verified twice on startup with retry
4. **Scan Locking**: scan_lock table uses PID + hostname; dead locks auto-released via `psutil.pid_exists()`
5. **Resume Logic**: scan_progress tracks last scanned path per drive; NULL = scan completed
6. **Singleton Pattern**: `get_db_instance()` — single connection, can reconnect if DB path changes
7. **Transaction Safety**: Batch operations use UPDATE-then-INSERT (not INSERT OR REPLACE) with IntegrityError fallback for race conditions
8. **Path Normalization**: Drive names standardized to "X:/" format; network aliases resolved
9. **UNIQUE INDEX**: `idx_files_directory_filename` prevents duplicate files (same directory + filename)
10. **Config Reload Caveat**: `CONFIG` loaded at import time — changes via `save_config()` write to disk but require module reload to affect other running modules
11. **Cascade Deletes**: Deleting a drive cascades to all its directories and files atomically
12. **Watchdog Pause**: Manual scans should pause watchdog via `pause_watchdog_for_scan()` to prevent concurrent DB access

## Important Safety Features
- NEVER automatically deletes database entries
- Preserves deleted file history in deleted_files/deleted_directories tables
- Drive isolation (operations on one drive never affect others)
- Scan abort safety (interrupted scans don't corrupt data, resume via checkpoint)

## Testing Approach
No formal test framework. Testing via:
- `run_critical_tests.bat`: Quick foreign key and integrity checks
- `full_system_test.bat`: Comprehensive system test
- `test_critical_bugs.py`: Detailed bug verification
- `test_unique_index.py`: Verifies UNIQUE constraint functionality
- `fix_duplicates_fast.py`: Removes duplicate entries and creates UNIQUE INDEX
- `integrity_checker.py`: Database-filesystem consistency

## Logging
- Main: scanner.log (rotating, max 10MB x 5)
- Scan all: scan_all.log
- Schema updates: schema_update.log
- Build process: build.log

## Dependencies
```
watchdog      # Filesystem monitoring
PyQt5         # GUI framework
pystray       # System tray
Pillow        # Image processing
psutil        # Process management
pyinstaller   # Building executables
```

## Debugging Scan Issues
1. Check scanner.log for detailed error messages
2. Run `python integrity_checker.py` to verify database consistency
3. Use `run_critical_tests.bat` for quick validation
4. Check scan_progress table for stuck scans (NULL last_path = completed)
