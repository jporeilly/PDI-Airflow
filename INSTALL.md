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

Full walkthrough incl. PDI/Carte host setup:
[lab/LAB-SETUP.md](lab/LAB-SETUP.md). Short version:

```powershell
cd lab\docker
docker compose up -d
```

- Airflow: http://localhost:8088 (admin/admin)
- Marquez: http://localhost:3000 (UI), :6001 (API)
- Carte expected on the host at :8081 (cluster/cluster)

## Running the tests

```powershell
# pdi2dag (uses the provider venv if you share it, or its own)
.\.venv\Scripts\python -m pytest

# provider
cd airflow-pentaho-provider
.\.venv\Scripts\python -m pytest
```
