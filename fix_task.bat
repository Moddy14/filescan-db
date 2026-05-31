@echo off
chcp 65001 >nul

echo ============================================
echo  DateiScanner Task - Reparatur
echo ============================================
echo.

set TASK_PYTHON=T:\Programme\Entwicklung\Python\Projekte\DateiDB\scanner_portable\venv\Scripts\python.exe
set TASK_SCRIPT=T:\Programme\Entwicklung\Python\Projekte\DateiDB\scanner_portable\scan_all_drives.py
set TASK_WORKDIR=T:\Programme\Entwicklung\Python\Projekte\DateiDB\scanner_portable

echo [1/3] Alten Task loeschen...
schtasks /Delete /TN "DateiScanner-Taeglicher Scan" /F 2>nul
schtasks /Delete /TN "DateiScanner-TaegligerScan" /F 2>nul
schtasks /Delete /TN "DateiScanner-T?gligerScan" /F 2>nul

REM Alle DateiScanner Tasks loeschen (egal wie der Name lautet)
for /f "tokens=*" %%i in ('schtasks /Query /FO LIST 2^>nul ^| findstr /i "DateiScanner"') do (
    echo Gefunden: %%i
)

echo.
echo [2/3] Neuen Task erstellen (03:00 Uhr taeglich)...
schtasks /Create /TN "DateiScanner-TaegligerScan" /TR "\"%TASK_PYTHON%\" \"%TASK_SCRIPT%\"" /SC DAILY /ST 03:00 /F /RL HIGHEST

echo.
echo [3/3] Task pruefen...
schtasks /Query /TN "DateiScanner-TaegligerScan" /FO LIST

echo.
echo ============================================
echo  Fertig! Druecke eine Taste zum Beenden.
echo ============================================
pause
