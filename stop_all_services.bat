@echo off
REM =============================================================================
REM Stop All Services Script
REM Beendet sauber alle laufenden Services (Tray, Watchdog, Scanner)
REM =============================================================================

setlocal enabledelayedexpansion

echo =============================================================================
echo Stop All Services - Scanner Portable System  
echo =============================================================================
echo Beende alle laufenden Services...
echo Zeit: %date% %time%
echo =============================================================================

REM Log-Datei
set LOG_FILE=%~dp0stop_services.log

REM =============================================================================
REM System Tray beenden
REM =============================================================================
echo.
echo [1/3] Beende System Tray...

REM Versuche zuerst, über Fenstertitel zu beenden (sanftere Methode)
tasklist | findstr /i "systray" >nul 2>&1
if not errorlevel 1 (
    echo   • System Tray Prozess gefunden - beende sanft...
    taskkill /im python.exe /fi "WINDOWTITLE eq *systray*" >nul 2>&1
    timeout /t 2 /nobreak >nul
)

REM Prüfe ob noch System Tray läuft
tasklist | findstr /i python | findstr /i systray >nul 2>&1
if not errorlevel 1 (
    echo   • Erzwinge Beendigung von System Tray...
    taskkill /f /im python.exe /fi "WINDOWTITLE eq *systray*" >nul 2>&1
    echo   ✓ System Tray beendet
) else (
    echo   ✓ System Tray bereits beendet oder nicht aktiv
)

REM =============================================================================
REM Watchdog-Service beenden
REM =============================================================================
echo.
echo [2/3] Beende Watchdog-Service...

REM Suche nach Watchdog-Prozessen
tasklist | findstr /i python | findstr /i watchdog >nul 2>&1
if not errorlevel 1 (
    echo   • Watchdog-Service gefunden - beende sanft...
    taskkill /im python.exe /fi "WINDOWTITLE eq *Watchdog*" >nul 2>&1
    timeout /t 2 /nobreak >nul
    
    REM Prüfe ob noch läuft
    tasklist | findstr /i python | findstr /i watchdog >nul 2>&1
    if not errorlevel 1 (
        echo   • Erzwinge Beendigung von Watchdog-Service...
        taskkill /f /im python.exe /fi "WINDOWTITLE eq *Watchdog*" >nul 2>&1
    )
    echo   ✓ Watchdog-Service beendet
) else (
    echo   ✓ Watchdog-Service bereits beendet oder nicht aktiv
)

REM =============================================================================
REM Weitere Scanner-Prozesse beenden
REM =============================================================================
echo.
echo [3/3] Prüfe weitere Scanner-Prozesse...

REM Beende eventuelle Scan-Prozesse
tasklist | findstr /i python >nul 2>&1
if not errorlevel 1 (
    echo   • Python-Prozesse gefunden - prüfe Scanner-Prozesse...
    
    REM Suche nach Scanner-spezifischen Prozessen
    wmic process where "name='python.exe'" get processid,commandline /format:csv | findstr /i "scanner\|scan_all\|dateisuche" >nul 2>&1
    if not errorlevel 1 (
        echo   • Scanner-Prozesse gefunden - beende sie...
        REM Beende Scanner-Prozesse (vorsichtig, um System nicht zu beeinträchtigen)
        for /f "tokens=2 delims=," %%i in ('wmic process where "name='python.exe'" get processid^,commandline /format:csv ^| findstr /i "scanner\|scan_all"') do (
            if not "%%i"=="" (
                echo     Beende Prozess-ID: %%i
                taskkill /pid %%i >nul 2>&1
            )
        )
        echo   ✓ Scanner-Prozesse beendet
    ) else (
        echo   ✓ Keine Scanner-Prozesse gefunden
    )
) else (
    echo   ✓ Keine Python-Prozesse gefunden
)

REM =============================================================================
REM Aufräumen und Status
REM =============================================================================
echo.
echo =============================================================================
echo Services erfolgreich beendet!
echo.

REM Finale Prüfung
echo Finale Status-Prüfung:
tasklist | findstr /i python >nul 2>&1
if not errorlevel 1 (
    echo   • Verbleibende Python-Prozesse:
    tasklist | findstr /i python | findstr /v "System\|Services"
) else (
    echo   ✓ Alle Scanner-bezogenen Python-Prozesse beendet
)

echo.
echo System ist bereit für einen Neustart der Services.
echo Verwende: full_system_test.bat zum Neustarten aller Services
echo =============================================================================

REM Log-Eintrag
echo [%date% %time%] Alle Services beendet >> "%LOG_FILE%"

echo.
echo Drücke eine Taste zum Beenden...
pause >nul

goto :eof