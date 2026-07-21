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
# The authoritative definition lives in repositories\repositories.xml -
# sync it into .kettle\ (what Carte reads) so editing one file is enough.
$repoDef = Join-Path $root 'repositories\repositories.xml'
$kettleDef = Join-Path $root '.kettle\repositories.xml'
if (Test-Path $repoDef) {
    New-Item -ItemType Directory -Force (Split-Path $kettleDef) | Out-Null
    Copy-Item $repoDef $kettleDef -Force
}
if (Test-Path $kettleDef) {
    $env:KETTLE_HOME = $root
    $repoNote = "$root\pipelines (KETTLE_HOME=$root)"

    # Shared database connections live in .kettle\shared.xml. Spoon writes
    # them to your GLOBAL ~/.kettle, but KETTLE_HOME points Carte at the
    # install - so without this sync every shared connection fails with
    # "!BaseDatabaseStep.Init.ConnectionMissing!". Copy the global file in
    # so Carte resolves the same connections you defined in Spoon.
    $globalShared = Join-Path $env:USERPROFILE '.kettle\shared.xml'
    $installShared = Join-Path $root '.kettle\shared.xml'
    if (Test-Path $globalShared) {
        Copy-Item $globalShared $installShared -Force
        $sharedNote = "synced from $globalShared"
    } elseif (Test-Path $installShared) {
        $sharedNote = 'using the install copy'
    } else {
        $sharedNote = 'NONE - shared DB connections will not resolve'
    }

    # VFS connections (Amazon S3/MinIO/HCP) live in the Pentaho metastore,
    # which ALSO follows KETTLE_HOME - so the same gotcha applies. Without
    # this an s3:// path silently resolves to nothing: with "file required"
    # off the step reports Finished with 0 rows and no error at all.
    $globalMeta = Join-Path $env:USERPROFILE '.pentaho\metastore'
    $installMeta = Join-Path $root '.pentaho\metastore'
    if (Test-Path $globalMeta) {
        New-Item -ItemType Directory -Force (Split-Path $installMeta) | Out-Null
        Copy-Item $globalMeta $installMeta -Recurse -Force
        $metaNote = "synced from $globalMeta"
    } elseif (Test-Path $installMeta) {
        $metaNote = 'using the install copy'
    } else {
        $metaNote = 'NONE - VFS (s3://) connections will not resolve'
    }
} else {
    $repoNote = "global ~/.kettle repository 'Default'"
    $sharedNote = 'global ~/.kettle\shared.xml'
    $metaNote = 'global ~/.pentaho\metastore'
}

Write-Host "Starting Carte on :8081" -ForegroundColor Cyan
Write-Host "  PDI:        $PdiHome"
Write-Host "  config:     $config"
Write-Host "  repository: $repoNote"
Write-Host "  shared.xml: $sharedNote"
Write-Host "  metastore:  $metaNote"
Write-Host "  auth:       cluster / cluster    (Ctrl+C to stop)"
Write-Host ""
# Run from the PDI folder: Carte.bat calls Spoon.bat relative to the
# working directory, so invoking it by full path from elsewhere fails
# with "'Spoon.bat' is not recognized". Note Push-Location alone is NOT
# enough - it changes PowerShell's location but not the *process*
# working directory that child processes inherit, so set that too.
$prevCwd = [Environment]::CurrentDirectory
Push-Location $PdiHome
try {
    [Environment]::CurrentDirectory = $PdiHome
    & '.\Carte.bat' $config
} finally {
    Pop-Location
    [Environment]::CurrentDirectory = $prevCwd
}
