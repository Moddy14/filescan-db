@echo off
echo ============================================================
echo DATEI-SCANNER AUTOSTART INSTALLATION
echo ============================================================
echo.

REM Prüfe Admin-Rechte
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [FEHLER] Dieses Script benötigt Administrator-Rechte!
    echo.
    echo Bitte mit Rechtsklick "Als Administrator ausführen" starten.
    pause
    exit /b 1
)

set CURRENT_DIR=%~dp0
REM WICHTIG: Trailing Backslash entfernen, sonst wird \" als Escape interpretiert
if "%CURRENT_DIR:~-1%"=="\" set CURRENT_DIR=%CURRENT_DIR:~0,-1%
set PYTHON_EXE=pythonw.exe

echo [1/3] Erstelle Autostart-Einträge...
echo.

REM System-Tray in Autostart (für aktuellen Benutzer)
echo Installiere System-Tray Autostart...
set TRAY_PATH=%CURRENT_DIR%\systray_launcher_full.py
set TRAY_START_CMD="%PYTHON_EXE%" "%TRAY_PATH%"

REM Registry-Eintrag für System-Tray
reg add "HKCU\Software\Microsoft\Windows\CurrentVersion\Run" /v "DateiScannerTray" /t REG_SZ /d "%TRAY_START_CMD%" /f >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] System-Tray wird beim Windows-Start geladen
) else (
    echo [FEHLER] Konnte System-Tray Autostart nicht einrichten
)

echo.
echo [2/3] Erstelle Windows-Dienst für Watchdog...
echo.

REM Watchdog als Windows-Dienst (benötigt NSSM oder sc)
REM Prüfe ob NSSM verfügbar ist (zuerst im tools-Ordner, dann im PATH)
set NSSM_EXE=%CURRENT_DIR%\tools\nssm.exe
if not exist "%NSSM_EXE%" (
    where nssm >nul 2>&1
    if %errorlevel% equ 0 (
        for /f "delims=" %%i in ('where nssm') do set NSSM_EXE=%%i
    )
)
if exist "%NSSM_EXE%" (
    echo NSSM gefunden: %NSSM_EXE% - installiere Watchdog-Dienst...

    REM Entferne alten Dienst falls vorhanden
    "%NSSM_EXE%" stop DateiScannerWatchdog >nul 2>&1
    "%NSSM_EXE%" remove DateiScannerWatchdog confirm >nul 2>&1

    REM Finde vollständigen Pfad zu pythonw.exe
    for /f "delims=" %%i in ('where pythonw.exe 2^>nul') do set PYTHONW_FULL=%%i
    if not defined PYTHONW_FULL set PYTHONW_FULL=%PYTHON_EXE%

    REM Installiere neuen Dienst
    REM WICHTIG: AppParameters MUSS gequotet werden wegen Leerzeichen im Pfad!
    "%NSSM_EXE%" install DateiScannerWatchdog "%PYTHONW_FULL%"
    "%NSSM_EXE%" set DateiScannerWatchdog AppParameters "\"%CURRENT_DIR%\watchdog_service.py\""
    "%NSSM_EXE%" set DateiScannerWatchdog AppDirectory "%CURRENT_DIR%"
    "%NSSM_EXE%" set DateiScannerWatchdog DisplayName "Datei Scanner Watchdog Service"
    "%NSSM_EXE%" set DateiScannerWatchdog Description "Überwacht Dateisystem-Änderungen in Echtzeit"
    "%NSSM_EXE%" set DateiScannerWatchdog Start SERVICE_AUTO_START
    "%NSSM_EXE%" set DateiScannerWatchdog AppExit Default Restart
    "%NSSM_EXE%" set DateiScannerWatchdog AppRestartDelay 5000

    REM Logging aktivieren für Fehlerdiagnose
    "%NSSM_EXE%" set DateiScannerWatchdog AppStdout "%CURRENT_DIR%\nssm_stdout.log"
    "%NSSM_EXE%" set DateiScannerWatchdog AppStderr "%CURRENT_DIR%\nssm_stderr.log"
    "%NSSM_EXE%" set DateiScannerWatchdog AppStdoutCreationDisposition 4
    "%NSSM_EXE%" set DateiScannerWatchdog AppStderrCreationDisposition 4

    REM Starte den Dienst
    "%NSSM_EXE%" start DateiScannerWatchdog

    echo [OK] Watchdog-Dienst installiert und gestartet
) else (
    echo [INFO] NSSM nicht gefunden (weder im tools-Ordner noch im PATH^) - verwende Task Scheduler als Alternative
    
    REM Alternative: Task Scheduler für Watchdog
    set TASK_NAME=DateiScannerWatchdog
    
    REM Lösche alte Task falls vorhanden
    schtasks /delete /tn "%TASK_NAME%" /f >nul 2>&1
    
    REM Erstelle neue Task
    schtasks /create /tn "%TASK_NAME%" ^
        /tr "\"%PYTHON_EXE%\" \"%CURRENT_DIR%\watchdog_monitor.py\" \"C:/\"" ^
        /sc onstart ^
        /ru SYSTEM ^
        /rl highest ^
        /f >nul 2>&1
        
    if %errorlevel% equ 0 (
        echo [OK] Watchdog als geplante Aufgabe eingerichtet
    ) else (
        echo [WARNUNG] Watchdog konnte nicht automatisch eingerichtet werden
    )
)

echo.
echo [3/3] Erstelle Desktop-Verknüpfungen...
echo.

REM PowerShell-Script für Verknüpfung erstellen
echo $WshShell = New-Object -ComObject WScript.Shell > temp_create_shortcut.ps1
echo $Shortcut = $WshShell.CreateShortcut("$env:USERPROFILE\Desktop\Datei Scanner.lnk") >> temp_create_shortcut.ps1
echo $Shortcut.TargetPath = "%PYTHON_EXE%" >> temp_create_shortcut.ps1
echo $Shortcut.Arguments = '"%CURRENT_DIR%\gui_launcher.py"' >> temp_create_shortcut.ps1
echo $Shortcut.WorkingDirectory = '"%CURRENT_DIR%"' >> temp_create_shortcut.ps1
echo $Shortcut.IconLocation = "shell32.dll,3" >> temp_create_shortcut.ps1
echo $Shortcut.Description = "Datei Scanner GUI" >> temp_create_shortcut.ps1
echo $Shortcut.Save() >> temp_create_shortcut.ps1

REM Verknüpfung erstellen
powershell -ExecutionPolicy Bypass -File temp_create_shortcut.ps1 >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] Desktop-Verknüpfung erstellt
) else (
    echo [INFO] Desktop-Verknüpfung konnte nicht erstellt werden
)

REM Aufräumen
del temp_create_shortcut.ps1 >nul 2>&1

echo.
echo ============================================================
echo INSTALLATION ABGESCHLOSSEN
echo ============================================================
echo.
echo Installierte Komponenten:
echo   - System-Tray: Startet automatisch beim Windows-Start
echo   - Watchdog: Läuft als Dienst/Task im Hintergrund
echo   - Desktop-Verknüpfung: Für schnellen GUI-Zugriff
echo.
echo Nächste Schritte:
echo   1. Windows neu starten für automatischen Start
echo   2. Oder System-Tray manuell starten: 
echo      pythonw systray_launcher_full.py
echo.
pause