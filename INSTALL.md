# Installation

## One-stop (recommended)

```bash
make install     # venv (64-bit) + pdi2dag + provider + build the UI
make run         # launch the Migration Studio -> http://localhost:5012
make lab-up      # Airflow + Marquez in Docker (:8088 / :3000)
make test        # run the pdi2dag + provider test suites
make help        # list all targets
```

No `make`? On Windows use `.\run.ps1` (creates the venv, builds the UI,
serves it); on Linux/macOS `./run.sh`. The manual steps are below.

> The venv must be **64-bit** Python — Apache Airflow's `msgspec`
> dependency has no 32-bit Windows build. `make`/`run.ps1` prefer a
> versioned 64-bit interpreter automatically.

---

Three installable pieces live here; install what you need.

## 1. pdi2dag (migration CLI)

```powershell
cd C:\Projects\PDI-AirFlow
py -3.10 -m venv .venv
.\.venv\Scripts\pip install -e .[dev]
.\.venv\Scripts\pdi2dag --version
```

Python 3.9+ and `requests` only — Airflow is *not* required on the
machine running pdi2dag (only on the target).

## 2. The Airflow provider

On any Airflow 2.7+/3.x deployment (all components):

```bash
pip install /path/to/PDI-AirFlow/airflow-pentaho-provider
```

On Astronomer: copy `airflow-pentaho-provider/` into the Astro
project and add `RUN pip install /usr/local/airflow/airflow-pentaho-provider`
to the Dockerfile (or publish the wheel to an internal index and use
`requirements.txt`). Details:
[airflow-pentaho-provider/INSTALL.md](airflow-pentaho-provider/INSTALL.md).

## 3. The lab stack (Airflow + Marquez in Docker)

Airflow always runs in **Linux containers** (never native Windows).
Two deployment options — see the table in
[lab/LAB-SETUP.md](lab/LAB-SETUP.md):

**A — Windows 11, Airflow 2.10** (Docker Desktop, dev):

```powershell
cd C:\PDI-Airflow\lab\docker
copy .env.example .env
docker compose -f docker-compose.win.yml up -d --build
```
- DAGs folder: `C:\PDI-Airflow\DAGS` (set `DAGS_DIR` in `.env`; the
  Studio's *Dags folder* setting must match). REST API v1 (basic auth).

**B — Ubuntu 24.04 VM, Airflow 3.3** (target — full guide:
[lab/UBUNTU-SETUP.md](lab/UBUNTU-SETUP.md)):

```bash
cd ~/PDI-Airflow && CARTE_HOST=<windows-ip> ./lab/install-ubuntu.sh
```
- DAGs folder on the VM; `CARTE_HOST` points at the Windows Carte.
  REST API v2 (JWT). The Studio (on Windows) connects over the LAN.

Both:
- Airflow: `:8088` (admin/admin) · Marquez: `:3000` UI, `:6001` API
- Carte reachable at `CARTE_HOST:8081` (cluster/cluster)
- The pdi2dag/Studio REST client **auto-detects v1 vs v2**, so the same
  Studio drives either lab.

## Running the tests

```powershell
# pdi2dag (uses the provider venv if you share it, or its own)
.\.venv\Scripts\python -m pytest

# provider
cd airflow-pentaho-provider
.\.venv\Scripts\python -m pytest
```
