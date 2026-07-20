# Runs the unit test suite on Windows.
$venv = Join-Path $PSScriptRoot "..\.venv\Scripts\python.exe"
& $venv -m pytest tests --tb=short @args
exit $LASTEXITCODE
