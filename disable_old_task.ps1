try {
    $task = Get-ScheduledTask | Where-Object { $_.TaskName -like "DateiScanner*" }

    if ($task) {
        Write-Host "Gefunden: $($task.TaskName)"
        Disable-ScheduledTask -TaskName $task.TaskName
        Write-Host "OK - Task deaktiviert."
        $info = Get-ScheduledTaskInfo -TaskName $task.TaskName
        Write-Host "Status: $($task.State)"
        Write-Host "Naechster Lauf: $($info.NextRunTime)"
    } else {
        Write-Host "Kein DateiScanner-Task gefunden."
    }
} catch {
    Write-Host "FEHLER: $_"
}

Write-Host ""
Read-Host "Druecke Enter zum Beenden"
