@echo off
echo ============================================================
echo DATEI-SCANNER AUTOSTART DEINSTALLATION
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

echo [1/3] Entferne Autostart-Einträge...
echo.

REM Entferne System-Tray aus Autostart
reg delete "HKCU\Software\Microsoft\Windows\CurrentVersion\Run" /v "DateiScannerTray" /f >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] System-Tray Autostart entfernt
) else (
    echo [INFO] System-Tray Autostart war nicht vorhanden
)

echo.
echo [2/3] Entferne Watchdog-Dienst/Task...
echo.

REM Versuche NSSM-Dienst zu entfernen
where nssm >nul 2>&1
if %errorlevel% equ 0 (
    nssm stop DateiScannerWatchdog >nul 2>&1
    nssm remove DateiScannerWatchdog confirm >nul 2>&1
    echo [OK] Watchdog-Dienst entfernt (falls vorhanden)
)

REM Entferne geplante Aufgabe
schtasks /delete /tn "DateiScannerWatchdog" /f >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] Watchdog Task entfernt
) else (
    echo [INFO] Watchdog Task war nicht vorhanden
)

echo.
echo [3/3] Stoppe laufende Prozesse...
echo.

REM Beende laufende Prozesse
taskkill /F /IM pythonw.exe /FI "WINDOWTITLE eq systray_launcher*" >nul 2>&1
taskkill /F /IM python.exe /FI "WINDOWTITLE eq watchdog*" >nul 2>&1

echo [OK] Laufende Prozesse beendet
echo.

echo ============================================================
echo DEINSTALLATION ABGESCHLOSSEN
echo ============================================================
echo.
echo Entfernte Komponenten:
echo   - System-Tray Autostart
echo   - Watchdog Dienst/Task
echo   - Laufende Prozesse
echo.
echo Die Desktop-Verknüpfung kann manuell gelöscht werden.
echo.
pause