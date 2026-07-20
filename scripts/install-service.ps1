<#
.SYNOPSIS
  Register (or remove) the Migration Studio as an auto-start Windows
  scheduled task - a lightweight "service" that launches the Studio at
  logon and restarts it on failure. Runs the deployed install's own venv
  (no Node needed) via run.ps1 -NoBuild.

.EXAMPLE
  .\scripts\install-service.ps1 -Dest C:\PDI-Airflow -Port 5012
  .\scripts\install-service.ps1 -Remove
#>
param(
    [string]$Dest = 'C:\PDI-Airflow',
    [int]$Port = 5012,
    [switch]$Remove
)

$ErrorActionPreference = 'Stop'
$taskName = 'PDI-AirFlow Studio'

if ($Remove) {
    if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
        Write-Host "Removed scheduled task '$taskName'."
    } else {
        Write-Host "No scheduled task '$taskName' to remove."
    }
    return
}

$run = Join-Path $Dest 'run.ps1'
if (-not (Test-Path $run)) {
    throw "$run not found - deploy the install first (install.ps1)."
}

$action = New-ScheduledTaskAction -Execute 'powershell.exe' `
    -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$run`" -NoBuild -Port $Port" `
    -WorkingDirectory $Dest
$trigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
    -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero)

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Settings $settings -Force `
    -Description 'PDI -> Airflow Migration Studio (auto-start at logon)' | Out-Null

Write-Host "Registered scheduled task '$taskName' - starts at logon on port $Port."
Write-Host "Start it now:  Start-ScheduledTask -TaskName '$taskName'"
Write-Host "Stop it:       Stop-ScheduledTask  -TaskName '$taskName'"
