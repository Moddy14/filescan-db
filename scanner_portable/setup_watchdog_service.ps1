<#
.SYNOPSIS
Installiert und konfiguriert den DateiScanner Watchdog als Windows-Dienst mithilfe von NSSM.

.DESCRIPTION
Dieses Skript automatisiert die Einrichtung von watchdog_service.py als Windows-Dienst.
Es setzt voraus, dass nssm.exe im selben Verzeichnis wie dieses Skript liegt
und dass das Skript als Administrator ausgeführt wird.

.PARAMETER Force
Wenn dieser Switch gesetzt ist, wird ein eventuell vorhandener Dienst ohne Rückfrage
 gestoppt und neu konfiguriert.

.PREREQUISITES
- nssm.exe muss im selben Verzeichnis wie dieses Skript liegen.
- PowerShell muss als Administrator ausgeführt werden.
#>

param(
    [string]$ServiceName = "DateiScannerWatchdog",
    [string]$DisplayName = "DateiScanner Watchdog Service",
    [string]$Description = "Überwacht Dateisystemänderungen für DateiScanner.",
    [string]$PythonScriptName = "watchdog_service.py",
    [switch]$Force
)

# --- Prüfe Administratorrechte ---
Write-Host "Prüfe Administratorrechte..."
if (-NOT ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Error "Dieses Skript muss als Administrator ausgeführt werden. Bitte PowerShell als Administrator starten und das Skript erneut ausführen."
    Exit 1
}
Write-Host "Administratorrechte vorhanden."

# --- Finde Skriptverzeichnis und Pfade ---
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
# Verwende den vom Benutzer angegebenen Pfad zu nssm.exe
$NssmPath = Join-Path $ScriptDir "tools\nssm.exe"
$PythonScriptPath = Join-Path $ScriptDir $PythonScriptName

# --- Prüfe, ob nssm.exe existiert ---
Write-Host "Suche nach nssm.exe unter $NssmPath..."
if (-NOT (Test-Path $NssmPath -PathType Leaf)) {
    Write-Error "nssm.exe nicht im tools-Unterverzeichnis gefunden: $NssmPath. Stelle sicher, dass nssm.exe im 'tools'-Ordner neben diesem Skript liegt."
    Exit 1
}
Write-Host "nssm.exe gefunden: $NssmPath"

# --- Prüfe, ob Python-Skript existiert ---
Write-Host "Suche nach Python-Skript: $PythonScriptPath..."
if (-NOT (Test-Path $PythonScriptPath -PathType Leaf)) {
    Write-Error "$PythonScriptName nicht gefunden in $ScriptDir."
    Exit 1
}
Write-Host "Python-Skript gefunden."

# --- Finde Python Interpreter (priorisiere pythonw.exe) ---
Write-Host "Suche nach Python Interpreter..."
$PythonPathW = Get-Command pythonw.exe -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source
$PythonPath = Get-Command python.exe -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source

if ($PythonPathW) {
    $PythonInterpreter = $PythonPathW
    Write-Host "Bevorzugter Python-Interpreter gefunden: $PythonInterpreter"
} elseif ($PythonPath) {
    $PythonInterpreter = $PythonPath
    Write-Host "Standard Python-Interpreter gefunden: $PythonInterpreter"
} else {
    Write-Error "Python-Interpreter (pythonw.exe oder python.exe) nicht im PATH gefunden. Bitte installieren Sie Python und stellen Sie sicher, dass es zum PATH hinzugefügt wird."
    pause
    exit 1
}

# --- Prüfe, ob Dienst bereits existiert ---
$ExistingService = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($ExistingService) {
    Write-Warning "Ein Dienst mit dem Namen '$ServiceName' existiert bereits."
    if (-not $Force) {
        $choice = Read-Host "Möchtest du versuchen, den vorhandenen Dienst zu stoppen und neu zu konfigurieren? (J/N)"
        if ($choice -ne 'J' -and $choice -ne 'j') {
            Write-Host "Aktion abgebrochen."
            Exit 0
        }
    } else {
        Write-Host "Parameter -Force angegeben. Vorhandener Dienst wird gestoppt und entfernt..."
    }
    Write-Host "Versuche vorhandenen Dienst zu stoppen..."
    Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 3 # Kurz warten
    Write-Host "Versuche vorhandenen Dienst zu entfernen..."
    & $NssmPath remove $ServiceName confirm # Versuche alten Dienst zu entfernen
    Start-Sleep -Seconds 2
}

# --- Installiere und konfiguriere den Dienst mit NSSM ---
Write-Host "Installiere Dienst '$ServiceName'..."

$WrapperScriptPath = Join-Path $ScriptDir "run_watchdog_service.bat"

# Neue Version: Wrapper-Skript installieren
& $NssmPath install $ServiceName "$WrapperScriptPath"

if ($LASTEXITCODE -ne 0) {
    Write-Error "Fehler bei der Installation des Dienstes mit NSSM (Install)."
    Exit 1
}

Write-Host "Konfiguriere Dienstparameter..."

# Entferne AppParameters, da der Wrapper es direkt aufruft
& $NssmPath set $ServiceName AppParameters ""
if ($LASTEXITCODE -ne 0) { Write-Warning "Warnung: Fehler beim Zurücksetzen von AppParameters." }

& $NssmPath set $ServiceName AppDirectory $ScriptDir
if ($LASTEXITCODE -ne 0) { Write-Error "Fehler beim Setzen von AppDirectory."; Exit 1 }

& $NssmPath set $ServiceName DisplayName $DisplayName
if ($LASTEXITCODE -ne 0) { Write-Error "Fehler beim Setzen von DisplayName."; Exit 1 }

& $NssmPath set $ServiceName Description $Description
if ($LASTEXITCODE -ne 0) { Write-Error "Fehler beim Setzen von Description."; Exit 1 }

Write-Host "Setze Starttyp auf Automatisch..."
& $NssmPath set $ServiceName Start SERVICE_AUTO_START
if ($LASTEXITCODE -ne 0) { Write-Error "Fehler beim Setzen des Starttyps."; Exit 1 }

Write-Host "Konfiguriere automatischen Neustart bei Fehler..."
& $NssmPath set $ServiceName AppExit Default Restart
if ($LASTEXITCODE -ne 0) { Write-Error "Fehler beim Setzen der Neustart-Aktion."; Exit 1 }

& $NssmPath set $ServiceName AppRestartDelay 5000 # 5 Sekunden Verzögerung
if ($LASTEXITCODE -ne 0) { Write-Warning "Warnung: Fehler beim Setzen der Neustart-Verzögerung (AppRestartDelay)." }

# --- NEU: Konfiguriere stdout und stderr Logging ---
# --- DEAKTIVIERT: Verursachte Probleme ---
# $StdOutLog = Join-Path $ScriptDir "minimal_stdout.log"
# $StdErrLog = Join-Path $ScriptDir "minimal_stderr.log"
# Write-Host "Konfiguriere stdout-Umleitung nach: $StdOutLog"
# & $NssmPath set $ServiceName AppStdout $StdOutLog
# if ($LASTEXITCODE -ne 0) { Write-Warning "Warnung: Fehler beim Setzen von AppStdout." }
#
# Write-Host "Konfiguriere stderr-Umleitung nach: $StdErrLog"
# & $NssmPath set $ServiceName AppStderr $StdErrLog
# if ($LASTEXITCODE -ne 0) { Write-Warning "Warnung: Fehler beim Setzen von AppStderr." }
# --- Ende NEU ---

Write-Host "Dienst '$ServiceName' erfolgreich installiert und konfiguriert."

# --- Starte den Dienst ---
Write-Host "Starte Dienst '$ServiceName'..."
Start-Service -Name $ServiceName
if ($LASTEXITCODE -ne 0) {
    Write-Error "Fehler beim Starten des Dienstes '$ServiceName'. Überprüfe die Windows-Ereignisanzeige und die Logs (`watchdog_service.log`) für Details."
    Write-Host "Du kannst versuchen, ihn manuell über 'services.msc' zu starten."
    Exit 1
}

# Kurze Prüfung, ob der Dienst läuft
Start-Sleep -Seconds 5
$ServiceStatus = Get-Service -Name $ServiceName
if ($ServiceStatus.Status -eq "Running") {
    Write-Host "Dienst '$ServiceName' wurde erfolgreich gestartet und läuft."
} else {
    Write-Warning "Dienst '$ServiceName' konnte gestartet werden, läuft aber anscheinend nicht (Status: $($ServiceStatus.Status)). Bitte überprüfe Logs oder die Windows Ereignisanzeige."
}

# Überprüfe, ob die Log-Datei erstellt wurde (Hinweis auf erfolgreichen Python-Start)
$LogFileName = "watchdog_service.log"
$LogFilePath = Join-Path $ScriptDir $LogFileName
if (Test-Path $LogFilePath) {
    Write-Host "Die Log-Datei '$LogFilePath' wurde gefunden."
    # Optional: Log-Datei öffnen
    # Write-Host "Versuche, die Log-Datei zu öffnen..."
    # try {
    #     Invoke-Item $LogFilePath -ErrorAction Stop
    # } catch {
    #     Write-Warning "Konnte die Log-Datei '$LogFilePath' nicht automatisch öffnen. Bitte überprüfen Sie sie manuell."
    # }
} else {
    Write-Warning "WARNUNG: Die Log-Datei '$LogFilePath' wurde NICHT gefunden."
    Write-Warning "Dies deutet darauf hin, dass das Python-Skript '$PythonScriptName' möglicherweise nicht korrekt gestartet wurde oder sehr früh abgestürzt ist."
    Write-Warning "Bitte überprüfen Sie die Windows-Ereignisanzeige (Anwendung) auf Fehler von '$ServiceName' oder 'nssm'."
}

Write-Host "Setup abgeschlossen."
if (-not $Force) {
    pause # Pause am Ende, um die Ausgabe zu sehen
} 