<#
.SYNOPSIS
  Remove a PDI-AirFlow install: unregister the auto-start service and
  delete the deployed copy. Use -KeepData to preserve your DAGs, file
  repository and settings.

.EXAMPLE
  .\uninstall.ps1                 # remove C:\PDI-Airflow + the service
  .\uninstall.ps1 -KeepData       # ... but keep DAGs, repositories, .kettle
  .\uninstall.ps1 -Dest D:\Apps\PDI-Airflow
#>
param(
    [string]$Dest = 'C:\PDI-Airflow',
    [switch]$KeepData
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot

# 1. Remove the auto-start service (best-effort).
& (Join-Path $root 'scripts\install-service.ps1') -Remove

# 2. Remove the deployed copy.
if (-not (Test-Path $Dest)) {
    Write-Host "$Dest not found - nothing to remove."
    return
}

$keep = @('DAGs', 'repositories', '.kettle')
if ($KeepData) {
    Get-ChildItem -LiteralPath $Dest -Force |
        Where-Object { $keep -notcontains $_.Name } |
        Remove-Item -Recurse -Force
    Write-Host "Removed $Dest (kept: $($keep -join ', '))."
} else {
    Remove-Item -LiteralPath $Dest -Recurse -Force
    Write-Host "Removed $Dest."
}
