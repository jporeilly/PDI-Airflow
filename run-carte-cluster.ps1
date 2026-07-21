<#
.SYNOPSIS
  Start a local Carte CLUSTER for the lab: one master (:8081) and two
  slaves (:8082, :8083), each in its own window. The slaves register with
  the master, so a transformation carrying a cluster schema is distributed
  across them.

.DESCRIPTION
  KETTLE_HOME is set to this folder (deployed install) so all three nodes
  share the install's own .kettle repository and your global ~/.kettle is
  left untouched. Falls back to lab\carte\cluster + the global .kettle
  when run from the source repo.

  Point the `pdi_cluster` Airflow connection at this master (host:8081)
  and select it in the Studio (or `pdi2dag --conn-id pdi_cluster`) to run
  clustered. Stop the cluster by closing the three windows.

.EXAMPLE
  .\run-carte-cluster.ps1
  .\run-carte-cluster.ps1 -PdiHome "C:\Pentaho\design-tools\data-integration"
#>
param(
    [string]$PdiHome
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot

if (-not $PdiHome) {
    $PdiHome = @(
        'C:\Pentaho\design-tools\data-integration',
        'C:\Pentaho\data-integration'
    ) | Where-Object { Test-Path (Join-Path $_ 'Carte.bat') } | Select-Object -First 1
}
if (-not $PdiHome -or -not (Test-Path (Join-Path $PdiHome 'Carte.bat'))) {
    throw "Carte.bat not found. Pass -PdiHome <your PDI 'data-integration' folder>."
}
$carteBat = Join-Path $PdiHome 'Carte.bat'

# Config dir: deployed layout first, then the source-repo layout.
$cfgDir = Join-Path $root 'carte\cluster'
if (-not (Test-Path $cfgDir)) { $cfgDir = Join-Path $root 'lab\carte\cluster' }
if (-not (Test-Path $cfgDir)) { throw "carte\cluster configs not found under $root." }

if (Test-Path (Join-Path $root '.kettle\repositories.xml')) {
    $env:KETTLE_HOME = $root
    # Same shared-connection sync as run-carte.ps1: Spoon writes shared DB
    # connections to the GLOBAL ~/.kettle\shared.xml, but KETTLE_HOME points
    # the cluster nodes at the install - without this every shared connection
    # fails with "!BaseDatabaseStep.Init.ConnectionMissing!".
    $globalShared = Join-Path $env:USERPROFILE '.kettle\shared.xml'
    if (Test-Path $globalShared) {
        Copy-Item $globalShared (Join-Path $root '.kettle\shared.xml') -Force
        Write-Host "Synced shared.xml from $globalShared" -ForegroundColor DarkGray
    }
}

# Master first, then the slaves (give the master a moment to accept
# registrations). Each node runs in its own window.
$nodes = @(
    @{ name = 'master'; cfg = 'master.xml'; port = 8081 },
    @{ name = 'slave1'; cfg = 'slave1.xml'; port = 8082 },
    @{ name = 'slave2'; cfg = 'slave2.xml'; port = 8083 }
)
foreach ($n in $nodes) {
    $cfg = Join-Path $cfgDir $n.cfg
    Write-Host ("Starting cluster-{0} on :{1}  ({2})" -f $n.name, $n.port, $cfg) -ForegroundColor Cyan
    Start-Process -FilePath $carteBat -ArgumentList "`"$cfg`"" -WorkingDirectory $PdiHome
    if ($n.name -eq 'master') { Start-Sleep -Seconds 5 } else { Start-Sleep -Seconds 2 }
}

Write-Host ""
Write-Host "Carte cluster starting: master :8081, slaves :8082 / :8083 (auth cluster/cluster)." -ForegroundColor Green
Write-Host "Check registrations: http://localhost:8081/kettle/getSlaves/ (the master lists its slaves)."
Write-Host "Point the pdi_cluster connection at this master and run a transformation with a cluster schema."
