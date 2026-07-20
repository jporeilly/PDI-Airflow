<#
.SYNOPSIS
  One-shot installer for PDI-AirFlow: prereq checks -> venv + packages ->
  build the Studio UI -> deploy a self-contained copy to C:\PDI-Airflow
  (its own venv, so it runs without Node) -> optionally register an
  auto-start service.

.EXAMPLE
  .\install.ps1                 # full install to C:\PDI-Airflow
  .\install.ps1 -Service        # ... and auto-start the Studio at logon
  .\install.ps1 -NoDeploy       # dev only: venv + UI in the source repo
  .\install.ps1 -Dest D:\Apps\PDI-Airflow -Port 5555
#>
param(
    [string]$Dest = 'C:\PDI-Airflow',
    [int]$Port = 5012,
    [switch]$NoDeploy,
    [switch]$Service
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
function Step($m) { Write-Host "==> $m" -ForegroundColor Cyan }
function Warn($m) { Write-Host "  ! $m" -ForegroundColor Yellow }

# 1. Prerequisites -------------------------------------------------------
Step 'Checking prerequisites'
$pyExe = $null
if (Get-Command py -ErrorAction SilentlyContinue) {
    foreach ($v in '3.12', '3.11', '3.10') {
        $exe = (& py "-$v" -c "import struct,sys;print(sys.executable if struct.calcsize('P')==8 else '')" 2>$null)
        if ($exe) { $pyExe = $exe.Trim(); break }
    }
}
if (-not $pyExe) {
    throw "No 64-bit Python 3.10-3.12 found. Airflow's msgspec dependency has no 32-bit Windows build - install a 64-bit Python and retry."
}
Write-Host "  Python (64-bit): $pyExe"
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
    throw 'Node.js not found (needed to build the Studio UI). Install Node 18+ and retry.'
}
Write-Host "  Node: $(node --version)"
if (Get-Command docker -ErrorAction SilentlyContinue) {
    Write-Host '  Docker: present (Windows Airflow lab available)'
} else {
    Warn 'Docker not found - the Windows Airflow lab (docker compose) will be unavailable.'
}

# 2. Source venv + packages (enables tests + the build) ------------------
Step 'Creating the source venv and installing packages'
$venv = Join-Path $root '.venv'
if (-not (Test-Path (Join-Path $venv 'Scripts\python.exe'))) { & $pyExe -m venv $venv }
$pip = Join-Path $venv 'Scripts\pip.exe'
& $pip install --upgrade pip | Out-Null
& $pip install -e "$root[dev,webapp]"
& $pip install -e "$root\airflow-pentaho-provider"

# 3. Build the Studio UI -------------------------------------------------
Step 'Building the Studio UI'
Push-Location (Join-Path $root 'webapp\frontend')
try {
    if (-not (Test-Path 'node_modules')) { npm ci }
    npm run build
} finally { Pop-Location }

if ($NoDeploy) {
    Step 'Dev setup complete (no deploy)'
    Write-Host "  Run:   .\run.ps1                      -> http://localhost:$Port"
    Write-Host '  Tests: .\.venv\Scripts\python -m pytest'
    return
}

# 4. Deploy a runnable copy to $Dest ------------------------------------
Step "Deploying to $Dest"
& (Join-Path $root 'scripts\deploy.ps1') -Dest $Dest

# 5. Self-contained venv inside $Dest (so the install runs without Node
#    or the source repo) ------------------------------------------------
Step "Creating the install venv at $Dest\.venv"
$destVenv = Join-Path $Dest '.venv'
if (-not (Test-Path (Join-Path $destVenv 'Scripts\python.exe'))) { & $pyExe -m venv $destVenv }
$destPip = Join-Path $destVenv 'Scripts\pip.exe'
& $destPip install --upgrade pip | Out-Null
& $destPip install "$Dest[webapp]"

# 6. Optional auto-start service ----------------------------------------
if ($Service) {
    Step 'Registering the auto-start Studio service'
    & (Join-Path $root 'scripts\install-service.ps1') -Dest $Dest -Port $Port
}

Step 'Install complete'
Write-Host ""
Write-Host "  Studio:      cd $Dest ; .\run.ps1 -NoBuild -Port $Port   ->  http://localhost:$Port" -ForegroundColor Green
Write-Host "  Carte:       cd $Dest ; .\run-carte.ps1        (cluster: .\run-carte-cluster.ps1)"
Write-Host "  Windows lab: cd $Dest\lab\docker ; docker compose -f docker-compose.win.yml up -d --build"
if ($Service) {
    Write-Host "  Service:     'PDI-AirFlow Studio' registered - starts at logon on port $Port."
    Write-Host "               Start now: Start-ScheduledTask -TaskName 'PDI-AirFlow Studio'"
}
Write-Host "  Uninstall:   .\uninstall.ps1"
