#!/usr/bin/env bash
# Runs the unit test suite on Linux/macOS.
set -e
cd "$(dirname "$0")/.."
PYTHON=".venv/bin/python"
[ -x "$PYTHON" ] || PYTHON="python"
"$PYTHON" -m pytest tests --tb=short "$@"
