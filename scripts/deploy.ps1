<#
.SYNOPSIS
  Deploy a runnable copy of PDI-AirFlow to a target directory
  (default C:\PDI-Airflow) — everything needed to run the Studio and
  the lab, without the dev venv / node_modules / caches.

.EXAMPLE
  .\scripts\deploy.ps1                 # -> C:\PDI-Airflow
  .\scripts\deploy.ps1 -Dest D:\Apps\PDI-Airflow
#>
param(
    [string]$Dest = 'C:\PDI-Airflow'
)

$ErrorActionPreference = 'Stop'
$src = Split-Path $PSScriptRoot -Parent   # repo root

Write-Host "==> Building the UI before deploy..." -ForegroundColor Cyan
Push-Location (Join-Path $src 'webapp\frontend')
if (-not (Test-Path 'node_modules')) { npm install --no-audit --no-fund }
npm run build
Pop-Location

Write-Host "==> Deploying $src -> $Dest" -ForegroundColor Cyan
# robocopy mirrors the tree, excluding the dirs/files a fresh install
# rebuilds. /XD = exclude dirs, /XF = exclude files. dist IS copied so
# the target serves the UI without needing npm.
$exclDirs = @('.git', '.venv', 'node_modules', '__pycache__',
              '.pytest_cache', '_pdc_share', '.airflow') |
            ForEach-Object { Join-Path $src "*\$_" }
robocopy $src $Dest /MIR `
    /XD (Join-Path $src '.git') (Join-Path $src '.venv') `
        (Join-Path $src '_pdc_share') (Join-Path $src '.airflow') `
        node_modules __pycache__ .pytest_cache .idea .vscode `
    /XF '*.pyc' 'settings.json' '*.openlineage.json' `
    /NFL /NDL /NJH /NP | Out-Null

# robocopy exit codes 0-7 are success; 8+ are errors.
if ($LASTEXITCODE -ge 8) { throw "robocopy failed ($LASTEXITCODE)" }

Write-Host ""
Write-Host "  Deployed to $Dest" -ForegroundColor Green
Write-Host "  Run it there:" -ForegroundColor Green
Write-Host "     cd $Dest ; .\run.ps1 -NoBuild"
Write-Host "     (or  make run  /  make lab-up)"
