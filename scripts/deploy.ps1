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

# DAGs folder the Windows lab (Airflow 2.10) mounts. Seed it with the
# whole workshop tree (workshop/ + examples/ + deploy-target/) so the
# example DAGs load and the Studio has a deploy-target/ to deploy into.
$dags = Join-Path $Dest 'DAGs'
$target = Join-Path $dags 'deploy-target'
New-Item -ItemType Directory -Force $target | Out-Null
Copy-Item (Join-Path $Dest 'workshop\dags\*') $dags -Recurse -Force -ErrorAction SilentlyContinue

# Carte lab artifacts, consolidated under $Dest so the install is
# self-contained:
#   $Dest\carte\carte-config.xml   - the Carte slave config
#   $Dest\repositories\            - the file-repository base_directory
#                                    (home\bi\hello_world.ktr) + a copy
#                                    of repositories.xml
#   $Dest\.kettle\repositories.xml - what Carte actually reads, via
#                                    KETTLE_HOME=$Dest (run-carte.ps1),
#                                    so your global ~/.kettle is untouched
$carteDir  = Join-Path $Dest 'carte'
$repoDir   = Join-Path $Dest 'repositories'
$kettleDir = Join-Path $Dest '.kettle'
New-Item -ItemType Directory -Force $carteDir, $repoDir, $kettleDir | Out-Null

Copy-Item (Join-Path $src 'lab\carte\carte-config.xml') $carteDir -Force
Copy-Item (Join-Path $src 'lab\carte\repository\*') $repoDir -Recurse -Force

# repositories.xml with base_directory pointed at the deployed repo dir;
# written both under repositories\ and under .kettle\ (Carte reads the
# latter via KETTLE_HOME).
$reposXml = (Get-Content (Join-Path $src 'lab\carte\repositories.xml') -Raw) `
    -replace '<base_directory>[^<]*</base_directory>', "<base_directory>$repoDir</base_directory>"
Set-Content -LiteralPath (Join-Path $repoDir 'repositories.xml')  -Value $reposXml -Encoding utf8
Set-Content -LiteralPath (Join-Path $kettleDir 'repositories.xml') -Value $reposXml -Encoding utf8

Write-Host ""
Write-Host "  Deployed to $Dest  (DAGs folder: $dags)" -ForegroundColor Green
Write-Host "  Run the Studio:   cd $Dest ; .\run.ps1 -NoBuild"
Write-Host "  Start Carte:      cd $Dest ; .\run-carte.ps1"
Write-Host "                    (config: $carteDir\carte-config.xml, repo: $repoDir)"
Write-Host "  Windows lab:      cd $Dest\lab\docker ; docker compose -f docker-compose.win.yml up -d --build"
Write-Host "  In the Studio Settings, set DAGs folder = $target"
