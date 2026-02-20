@echo off
echo ============================================================
echo KRITISCHE BUG-TESTS - SCHNELLCHECK
echo ============================================================
echo.

echo [TEST 1] Foreign Keys Status pruefen...
python -c "from models import get_db_instance; db = get_db_instance(); db.cursor.execute('PRAGMA foreign_keys'); result = db.cursor.fetchone()[0]; print(f'Foreign Keys: {result}'); exit(0 if result == 1 else 1)" 2>nul
if %errorlevel% equ 0 (
    echo [OK] Foreign Keys sind aktiviert
) else (
    echo [FEHLER] Foreign Keys sind NICHT aktiviert!
)
echo.

echo [TEST 2] Scanner-Core Sicherheit pruefen...
findstr /C:"NIEMALS automatisch" scanner_core.py >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] Automatische Loeschung deaktiviert
) else (
    echo [FEHLER] Automatische Loeschung moeglich!
)
echo.

echo [TEST 3] Datenbank-Integritaet pruefen...
python -c "import sqlite3; conn = sqlite3.connect('Dateien.db'); conn.execute('PRAGMA foreign_keys = ON'); cursor = conn.cursor(); cursor.execute('SELECT d.name, COUNT(f.id) FROM drives d LEFT JOIN directories dir ON dir.drive_id = d.id LEFT JOIN files f ON f.directory_id = dir.id GROUP BY d.id'); results = cursor.fetchall(); print('Laufwerke:'); [print(f'  {r[0]}: {r[1]} Dateien') for r in results]; conn.close()" 2>nul
echo.

echo [TEST 4] Verwaiste Eintraege suchen...
python -c "import sqlite3; conn = sqlite3.connect('Dateien.db'); conn.execute('PRAGMA foreign_keys = ON'); cursor = conn.cursor(); cursor.execute('SELECT COUNT(*) FROM directories d LEFT JOIN drives dr ON d.drive_id = dr.id WHERE dr.id IS NULL'); orphans = cursor.fetchone()[0]; print(f'Verwaiste Verzeichnisse: {orphans}'); exit(0 if orphans == 0 else 1); conn.close()" 2>nul
if %errorlevel% equ 0 (
    echo [OK] Keine verwaisten Eintraege
) else (
    echo [FEHLER] Verwaiste Eintraege gefunden!
)
echo.

echo ============================================================
echo SCHNELLTEST ABGESCHLOSSEN
echo ============================================================
echo.
echo Fuer detaillierte Tests: python test_critical_bugs.py
echo.
pause