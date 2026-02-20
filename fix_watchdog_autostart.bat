@echo off
chcp 65001 >nul 2>&1
echo ============================================================
echo WATCHDOG AUTOSTART REPARATUR
echo ============================================================
echo.

REM Pruefe Admin-Rechte
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [FEHLER] Dieses Script benoetigt Administrator-Rechte!
    echo.
    echo Bitte mit Rechtsklick "Als Administrator ausfuehren" starten.
    pause
    exit /b 1
)

set CURRENT_DIR=%~dp0
REM WICHTIG: Trailing Backslash entfernen, sonst wird \" als Escape interpretiert
if "%CURRENT_DIR:~-1%"=="\" set CURRENT_DIR=%CURRENT_DIR:~0,-1%

set NSSM=%CURRENT_DIR%\tools\nssm.exe
set PYTHON_EXE=D:\Python313\pythonw.exe
set SERVICE_NAME=DateiScannerWatchdog

REM Pruefe ob NSSM existiert
if not exist "%NSSM%" (
    echo [FEHLER] NSSM nicht gefunden: %NSSM%
    pause
    exit /b 1
)

REM Pruefe ob pythonw.exe existiert
if not exist "%PYTHON_EXE%" (
    echo [INFO] pythonw.exe nicht unter %PYTHON_EXE% gefunden, suche im PATH...
    for /f "delims=" %%i in ('where pythonw.exe 2^>nul') do set PYTHON_EXE=%%i
    if not defined PYTHON_EXE (
        echo [FEHLER] pythonw.exe nicht gefunden!
        pause
        exit /b 1
    )
)
echo Python: %PYTHON_EXE%
echo Projektverzeichnis: %CURRENT_DIR%
echo.

echo [1/4] Stoppe bestehenden Dienst...
"%NSSM%" stop %SERVICE_NAME% >nul 2>&1
timeout /t 3 /nobreak >nul

echo [2/4] Repariere NSSM-Konfiguration...
echo.

REM KERN-FIX 1: Application ohne Trailing-Backslash-Problem
"%NSSM%" set %SERVICE_NAME% Application "%PYTHON_EXE%"

REM KERN-FIX 2: AppParameters MIT inneren Anfuehrungszeichen fuer Leerzeichen im Pfad
"%NSSM%" set %SERVICE_NAME% AppParameters "\"%CURRENT_DIR%\watchdog_service.py\""

REM KERN-FIX 3: AppDirectory OHNE trailing backslash (sonst \" = escaped quote)
"%NSSM%" set %SERVICE_NAME% AppDirectory "%CURRENT_DIR%"

REM Logging aktivieren (war deaktiviert - Fehler waren unsichtbar!)
"%NSSM%" set %SERVICE_NAME% AppStdout "%CURRENT_DIR%\nssm_stdout.log"
"%NSSM%" set %SERVICE_NAME% AppStderr "%CURRENT_DIR%\nssm_stderr.log"

REM Starttyp und Neustart-Verhalten
"%NSSM%" set %SERVICE_NAME% Start SERVICE_AUTO_START
"%NSSM%" set %SERVICE_NAME% AppExit Default Restart
"%NSSM%" set %SERVICE_NAME% AppRestartDelay 5000
"%NSSM%" set %SERVICE_NAME% AppThrottle 5000

echo.
echo [OK] NSSM-Konfiguration repariert
echo.

echo [3/4] Starte Dienst neu...
"%NSSM%" start %SERVICE_NAME%
timeout /t 5 /nobreak >nul

REM Pruefe ob Dienst laeuft
sc query %SERVICE_NAME% | findstr /C:"RUNNING" >nul
if %errorlevel% equ 0 (
    echo [OK] Dienst laeuft!
) else (
    sc query %SERVICE_NAME% | findstr /C:"STATE"
    echo [WARNUNG] Dienst laeuft moeglicherweise nicht. Pruefe nssm_stderr.log
)
echo.

echo [4/4] Pruefe Registry-Autostart fuer System-Tray...

REM System-Tray Autostart (fuer aktuellen Benutzer)
REM reg add braucht den Wert OHNE aeussere Quotes um die Quotes im Wert korrekt zu speichern
reg add "HKCU\Software\Microsoft\Windows\CurrentVersion\Run" /v "DateiScannerTray" /t REG_SZ /d "\"%PYTHON_EXE%\" \"%CURRENT_DIR%\systray_launcher_full.py\"" /f >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] System-Tray Autostart eingerichtet
) else (
    echo [WARNUNG] Konnte System-Tray Autostart nicht einrichten
)

echo.
echo ============================================================
echo REPARATUR ABGESCHLOSSEN
echo ============================================================
echo.
echo Zusammenfassung:
echo   - NSSM AppParameters: Pfad-Quoting fuer Leerzeichen repariert
echo   - NSSM AppDirectory: Trailing-Backslash-Bug behoben
echo   - NSSM Logging: stdout/stderr aktiviert
echo   - Dienst: Neugestartet
echo   - System-Tray: Autostart in Registry eingetragen
echo.
echo Falls Probleme auftreten, pruefe:
echo   - %CURRENT_DIR%\nssm_stderr.log
echo   - %CURRENT_DIR%\scanner.log
echo   - Windows Ereignisanzeige (Anwendung, Quelle: nssm)
echo.
pause
