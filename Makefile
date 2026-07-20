# PDI-AirFlow — one-stop install, run and lab control
#
#   make install     set everything up: venv + pdi2dag + provider + UI build
#   make run         launch the Migration Studio (http://localhost:5012)
#   make dev         Studio in hot-reload mode (Vite :5173 + API :5012)
#   make test        run the pdi2dag + provider test suites
#   make lab-up      Airflow + Marquez in Docker (:8088 / :3000)
#   make lab-down    stop the lab (data volumes survive)
#   make carte-up    add the containerized Carte (--profile carte, ~1.5 GB)
#   make status      lab container status
#   make deploy      deploy a runnable copy to C:\PDI-Airflow (DEPLOY_DIR=)
#   make clean       remove venv, UI build and caches
#
# Cross-platform: uses .venv/Scripts on Windows, .venv/bin elsewhere.

SHELL    := bash
VENV     := .venv
PY       := $(VENV)/bin/python
ifeq ($(OS),Windows_NT)
  PY     := $(VENV)/Scripts/python.exe
endif
PIP        := $(PY) -m pip
FRONTEND   := webapp/frontend
LAB        := lab/docker
DEPLOY_DIR ?= C:\PDI-Airflow

.DEFAULT_GOAL := help
.PHONY: help install venv provider frontend run dev test \
        lab-up lab-down carte-up status deploy clean

help:
	@sed -n 's/^#   /  /p' Makefile

install: venv provider frontend
	@echo ""
	@echo "  Ready. Start the Studio with:  make run"
	@echo "  Bring up Airflow + Marquez with: make lab-up"

venv:
	@test -x "$(PY)" || { echo "==> creating venv (preferring 64-bit Python)"; \
	  for v in 3.12 3.11 3.10; do \
	    if py -$$v -c "import struct;exit(0 if struct.calcsize('P')*8==64 else 1)" 2>/dev/null; \
	    then py -$$v -m venv $(VENV); break; fi; \
	  done; \
	  test -x "$(PY)" || py -3 -m venv $(VENV) 2>/dev/null \
	    || python3 -m venv $(VENV) || python -m venv $(VENV); }
	@$(PIP) install --quiet --upgrade pip
	@echo "==> installing pdi2dag (webapp + dev extras)"
	@$(PIP) install --quiet -e ".[webapp,dev]"

provider:
	@echo "==> installing airflow-pentaho-provider (pulls Apache Airflow)"
	@$(PIP) install --quiet -e airflow-pentaho-provider \
	  || echo "  (skipped — needs a 64-bit Python + Airflow; the Studio and lab don't require it)"

frontend:
	@echo "==> installing + building the UI"
	@cd $(FRONTEND) && npm install --no-audit --no-fund && npm run build

run:
	@echo "  Migration Studio -> http://localhost:5012   (API docs: /docs)"
	@$(PY) -m uvicorn main:app --app-dir webapp/backend --port 5012

dev:
	@bash run.sh --dev

test:
	@echo "==> pdi2dag tests"
	@$(PY) -m pytest tests -q
	@echo "==> provider tests"
	@$(PY) -m pytest airflow-pentaho-provider/tests -q

lab-up:
	@cd $(LAB) && docker compose up -d --build
	@echo "  Airflow: http://localhost:8088 (admin/admin)   Marquez: http://localhost:3000"

lab-down:
	@cd $(LAB) && docker compose down

carte-up:
	@cd $(LAB) && docker compose --profile carte up -d --build
	@echo "  Carte: http://localhost:8081/kettle/status (cluster/cluster)"

status:
	@docker ps --filter name=docker-airflow --filter name=docker-marquez \
	  --filter name=docker-carte --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' \
	  2>/dev/null || echo "Docker not running"

deploy:
	@echo "==> deploying a runnable copy to $(DEPLOY_DIR)"
	@pwsh -NoProfile -File scripts/deploy.ps1 -Dest "$(DEPLOY_DIR)" 2>/dev/null \
	  || powershell -NoProfile -File scripts/deploy.ps1 -Dest "$(DEPLOY_DIR)"

clean:
	@rm -rf $(VENV) $(FRONTEND)/dist $(FRONTEND)/node_modules \
	  .pytest_cache **/.pytest_cache *.egg-info **/*.egg-info webapp/settings.json
	@find . -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true
	@echo "  cleaned."
