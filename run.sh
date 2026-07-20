#!/usr/bin/env bash
# Launch the PDI -> Airflow Migration Studio (FastAPI backend serving the
# built React/Vite UI).
#   ./run.sh            build UI, serve at http://localhost:5012
#   ./run.sh --dev      hot-reload: Vite (:5173) + backend (:5012)
#   ./run.sh --no-build serve the existing build
#   PORT=5555 ./run.sh  use a different port
set -e
root="$(cd "$(dirname "$0")" && pwd)"
venv="$root/.venv"
port="${PORT:-5012}"
dev=false; nobuild=false
for a in "$@"; do
  case "$a" in
    --dev) dev=true ;;
    --no-build) nobuild=true ;;
  esac
done

py="$venv/bin/python"
[ -x "$py" ] || py="$venv/Scripts/python.exe"   # Git Bash on Windows
if [ ! -x "$py" ]; then
  echo "==> Creating venv and installing dependencies..."
  python3 -m venv "$venv" 2>/dev/null || python -m venv "$venv"
  py="$venv/bin/python"; [ -x "$py" ] || py="$venv/Scripts/python.exe"
  "$py" -m pip install --quiet --upgrade pip
  "$py" -m pip install --quiet -e "$root[webapp]"
fi

frontend="$root/webapp/frontend"
[ -d "$frontend/node_modules" ] || (cd "$frontend" && npm install --no-audit --no-fund)

if $dev; then
  echo "==> Starting Vite dev server (http://localhost:5173)..."
  (cd "$frontend" && npm run dev &)
elif ! $nobuild; then
  echo "==> Building the UI..."
  (cd "$frontend" && npm run build)
fi

if $dev; then
  echo "==> Backend API on :$port  (open the UI at http://localhost:5173)"
else
  echo "==> Migration Studio: http://localhost:$port   (API docs: /docs)"
fi
cd "$root/webapp/backend"
exec "$py" -m uvicorn main:app --port "$port"
