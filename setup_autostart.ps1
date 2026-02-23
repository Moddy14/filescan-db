# PowerShell Script für Autostart-Installation
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "DATEI-SCANNER AUTOSTART SETUP" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
$trayScript = Join-Path $scriptPath "systray_launcher_full.py"
$pythonw = "pythonw.exe"

# System-Tray Autostart
Write-Host "[1/2] Installiere System-Tray Autostart..." -ForegroundColor Yellow

# Erstelle Autostart-Verknüpfung im Startup-Ordner
$startupFolder = [Environment]::GetFolderPath("Startup")
$shortcutPath = Join-Path $startupFolder "DateiScanner-Tray.lnk"

$WshShell = New-Object -ComObject WScript.Shell
$Shortcut = $WshShell.CreateShortcut($shortcutPath)
$Shortcut.TargetPath = $pythonw
$Shortcut.Arguments = "`"$trayScript`""
$Shortcut.WorkingDirectory = $scriptPath
$Shortcut.WindowStyle = 7  # Minimiert
$Shortcut.IconLocation = "shell32.dll,3"
$Shortcut.Description = "Datei Scanner System Tray"
$Shortcut.Save()

if (Test-Path $shortcutPath) {
    Write-Host "[OK] System-Tray Autostart eingerichtet" -ForegroundColor Green
    Write-Host "     Pfad: $shortcutPath" -ForegroundColor Gray
} else {
    Write-Host "[FEHLER] Konnte Autostart nicht einrichten" -ForegroundColor Red
}

# Desktop-Verknüpfung
Write-Host ""
Write-Host "[2/2] Erstelle Desktop-Verknüpfung..." -ForegroundColor Yellow

$desktopPath = [Environment]::GetFolderPath("Desktop")
$desktopShortcut = Join-Path $desktopPath "Datei Scanner.lnk"
$guiScript = Join-Path $scriptPath "gui_launcher.py"

$Shortcut2 = $WshShell.CreateShortcut($desktopShortcut)
$Shortcut2.TargetPath = "python.exe"
$Shortcut2.Arguments = "`"$guiScript`""
$Shortcut2.WorkingDirectory = $scriptPath
$Shortcut2.IconLocation = "shell32.dll,3"
$Shortcut2.Description = "Datei Scanner GUI"
$Shortcut2.Save()

if (Test-Path $desktopShortcut) {
    Write-Host "[OK] Desktop-Verknüpfung erstellt" -ForegroundColor Green
} else {
    Write-Host "[INFO] Desktop-Verknüpfung konnte nicht erstellt werden" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "INSTALLATION ABGESCHLOSSEN" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Installierte Komponenten:" -ForegroundColor White
Write-Host "  - System-Tray: Startet beim nächsten Windows-Start" -ForegroundColor Gray
Write-Host "  - Desktop-Verknüpfung: Für schnellen GUI-Zugriff" -ForegroundColor Gray
Write-Host ""
Write-Host "System-Tray jetzt starten? (J/N): " -NoNewline -ForegroundColor Yellow
$response = Read-Host

if ($response -eq "J" -or $response -eq "j") {
    Write-Host "Starte System-Tray..." -ForegroundColor Yellow
    Start-Process $pythonw -ArgumentList "`"$trayScript`"" -WindowStyle Hidden
    Write-Host "[OK] System-Tray gestartet" -ForegroundColor Green
}

Write-Host ""
Write-Host "Drücken Sie eine beliebige Taste zum Beenden..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")