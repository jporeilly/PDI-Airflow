<#
.SYNOPSIS
  Launch the PDI -> Airflow Migration Studio (FastAPI backend serving the
  built React/Vite UI).

.EXAMPLE
  .\run.ps1                 # build UI, serve everything at http://localhost:5012
  .\run.ps1 -Port 5555      # use a different port
  .\run.ps1 -Dev            # hot-reload dev: Vite (:5173) + backend (:5012)
  .\run.ps1 -NoBuild        # serve the existing build without rebuilding
#>
param(
    [int]$Port = 5012,
    [switch]$Dev,
    [switch]$NoBuild
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$venv = Join-Path $root '.venv'
$py   = Join-Path $venv 'Scripts\python.exe'

# 1. Python venv + app dependencies (pdi2dag + FastAPI/uvicorn) -----------
if (-not (Test-Path $py)) {
    Write-Host '==> Creating Python venv and installing dependencies...' -ForegroundColor Cyan
    # Prefer a 64-bit Python - Airflow's deps (msgspec) have no 32-bit
    # Windows build. Fall back to whatever is available (fine for the
    # app itself, which doesn't need Airflow).
    $made = $false
    if (Get-Command py -ErrorAction SilentlyContinue) {
        foreach ($v in '3.12', '3.11', '3.10') {
            $bits = (& py "-$v" -c "import struct;print(struct.calcsize('P')*8)" 2>$null)
            if ($bits -eq '64') { & py "-$v" -m venv $venv; $made = $true; break }
        }
        if (-not $made) { & py -3 -m venv $venv; $made = $true }
    }
    if (-not $made) { & python -m venv $venv }
    & $py -m pip install --quiet --upgrade pip
    & $py -m pip install --quiet -e "$root[webapp]"
}

# 2. Frontend --------------------------------------------------------------
# -NoBuild serves the existing build and needs NO Node - this is how a
# deployed install (dist shipped, no node_modules) runs. Dev/build paths
# do need Node.
$frontend = Join-Path $root 'webapp\frontend'
$dist = Join-Path $frontend 'dist'
if ($Dev) {
    if (-not (Test-Path (Join-Path $frontend 'node_modules'))) {
        Write-Host '==> Installing frontend packages...' -ForegroundColor Cyan
        Push-Location $frontend; npm install --no-audit --no-fund; Pop-Location
    }
    Write-Host '==> Starting Vite dev server (http://localhost:5173)...' -ForegroundColor Cyan
    Start-Process -FilePath 'npm' -ArgumentList 'run', 'dev' -WorkingDirectory $frontend
} elseif ($NoBuild) {
    if (-not (Test-Path $dist)) {
        throw "No UI build found at $dist. Run without -NoBuild (needs Node) or deploy a built copy."
    }
} else {
    if (-not (Test-Path (Join-Path $frontend 'node_modules'))) {
        Write-Host '==> Installing frontend packages...' -ForegroundColor Cyan
        Push-Location $frontend; npm install --no-audit --no-fund; Pop-Location
    }
    Write-Host '==> Building the UI...' -ForegroundColor Cyan
    Push-Location $frontend; npm run build; Pop-Location
}

# 3. Backend (serves the built UI, or the API for the dev server) ----------
$backend = Join-Path $root 'webapp\backend'
if ($Dev) {
    Write-Host "==> Backend API on http://localhost:$Port  (open the UI at http://localhost:5173)" -ForegroundColor Green
} else {
    Write-Host "==> Migration Studio: http://localhost:$Port   (API docs: /docs)" -ForegroundColor Green
}
Push-Location $backend
try {
    & $py -m uvicorn main:app --port $Port
} finally {
    Pop-Location
}
