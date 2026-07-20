<#
.SYNOPSIS
  Start Carte for the lab using the consolidated config + file repository
  under this folder. KETTLE_HOME is set to this folder so Carte reads its
  own .kettle\repositories.xml - your global ~/.kettle is left untouched.

.DESCRIPTION
  Works in a deployed install (C:\PDI-Airflow, where deploy.ps1 staged
  carte\, repositories\ and .kettle\) and, as a fallback, from the source
  repo (uses lab\carte\ and your global ~/.kettle).

.EXAMPLE
  .\run-carte.ps1
  .\run-carte.ps1 -PdiHome "C:\Pentaho\design-tools\data-integration"
#>
param(
    [string]$PdiHome
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot

# Locate PDI (Carte.bat) if not supplied.
if (-not $PdiHome) {
    $PdiHome = @(
        'C:\Pentaho\design-tools\data-integration',
        'C:\Pentaho\data-integration'
    ) | Where-Object { Test-Path (Join-Path $_ 'Carte.bat') } | Select-Object -First 1
}
if (-not $PdiHome -or -not (Test-Path (Join-Path $PdiHome 'Carte.bat'))) {
    throw "Carte.bat not found. Pass -PdiHome <your PDI 'data-integration' folder>."
}

# Carte config: deployed layout first, then the source-repo layout.
$config = Join-Path $root 'carte\carte-config.xml'
if (-not (Test-Path $config)) { $config = Join-Path $root 'lab\carte\carte-config.xml' }
if (-not (Test-Path $config)) { throw "carte-config.xml not found under $root." }

# Self-contained repository: if this folder has its own .kettle (deployed
# install), point KETTLE_HOME at it so the global ~/.kettle is untouched.
if (Test-Path (Join-Path $root '.kettle\repositories.xml')) {
    $env:KETTLE_HOME = $root
    $repoNote = "$root\repositories (KETTLE_HOME=$root)"
} else {
    $repoNote = "global ~/.kettle repository 'Default'"
}

Write-Host "Starting Carte on :8081" -ForegroundColor Cyan
Write-Host "  PDI:        $PdiHome"
Write-Host "  config:     $config"
Write-Host "  repository: $repoNote"
Write-Host "  auth:       cluster / cluster    (Ctrl+C to stop)"
Write-Host ""
& (Join-Path $PdiHome 'Carte.bat') $config
