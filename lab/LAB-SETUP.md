# Lab Setup Guide — PDI + Apache Airflow + Marquez

## Deployment options

There are two supported ways to run the lab. Airflow always runs in
**Linux Docker containers** (never native Windows — Airflow's SDK is
POSIX-only); the difference is *where* those containers run and which
Airflow version.

| | **A — Windows 11 (dev)** | **B — Ubuntu 24.04 VM (target)** |
|---|---|---|
| Airflow | **2.10.5** | **3.3** |
| Runs on | Windows Docker Desktop | Ubuntu Docker Engine |
| Compose | `docker-compose.win.yml` | `docker-compose.yml` |
| REST API | v1 (basic auth) | v2 (JWT) — client auto-detects |
| DAGs folder | `C:\PDI-Airflow\DAGs` | a path on the VM |
| Carte | `host.docker.internal` (same box) | `CARTE_HOST` = the Windows IP |
| Studio + Carte | on the same Windows box | on Windows, connects over LAN |

- **Option A** — everything on one Windows machine; the Studio deploys
  DAGs into `C:\PDI-Airflow\DAGs\deploy-target`, which the Windows lab
  mounts (via its parent `DAGs` folder). Start:
  ```powershell
  cd lab\docker
  docker compose -f docker-compose.win.yml up -d --build
  # (set DAGS_DIR / CARTE_HOST in .env if not the defaults)
  ```
- **Option B** — the Airflow 3.3 + Marquez lab runs on the Ubuntu VM
  (alongside PDC); the Studio and Carte stay on Windows and connect
  over the LAN. See **[UBUNTU-SETUP.md](UBUNTU-SETUP.md)**.

Both read `lab/docker/.env` (`cp .env.example .env`). The Studio's
`dags_folder` setting must match the host side of the DAGs mount.

The rest of this guide covers Option A (Windows) in detail.

---

This guide builds a complete local lab on a Windows host (adaptable to
Linux/macOS) for running Pentaho Data Integration workloads from
Apache Airflow, with data lineage visible in Marquez:

```
┌────────────────────────── Windows host ──────────────────────────┐
│                                                                  │
│  PDI (Spoon + Carte :8081)      Docker Desktop                   │
│  File repository "Default"   ┌─────────────────────────────────┐ │
│  at C:\PDI-Repo              │ Airflow (standalone) :8088      │ │
│            ▲                 │   airflow-provider-pentaho      │ │
│            │ Carte REST      │   openlineage provider          │ │
│            └─────────────────│                                 │ │
│         host.docker.internal │ Marquez API :5000 / UI :3000    │ │
│                              └─────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

## Prerequisites

| Component        | Version                  | Notes                          |
|------------------|--------------------------|--------------------------------|
| Java             | 11 or 17 (64-bit)        | required by PDI                |
| PDI              | 9.x / 10.x (CE or EE)    | Spoon + Carte                  |
| Docker Desktop   | current                  | WSL2 backend recommended       |
| Python           | 3.9–3.12                 | only for pdi2dag CLI usage     |
| This repo        | C:\Projects\PDI-AirFlow  |                                |

## Part 1 — PDI and the file repository

1. **Install PDI** (unzip pdi-ce or run the EE installer), e.g. to
   `C:\Pentaho\data-integration`. Verify `Spoon.bat` starts.

2. **Create the repository folder**: `mkdir C:\PDI-Repo`.

3. **Register the repository**: copy
   [carte/repositories.xml](carte/repositories.xml) to
   `C:\Users\<you>\.kettle\repositories.xml` (create the `.kettle`
   folder if needed). Adjust `base_directory` if you used a different
   path. This defines a *file repository* named `Default` — no
   database server required.

4. **Connect Spoon to it**: start Spoon → Tools → Repository →
   Connect → `Default`. Any username/password works for a file
   repository (the lab uses `admin`/`password` to match the Airflow
   connection).

5. **Create the workshop content** in the repository (folder
   `/home/bi`):

   - `hello_world` (transformation): Generate Rows (limit 10, one
     String field `message` = `Hello from Airflow`) → Write to Log.
   - `extract_sales`, `extract_customers`, `load_warehouse`,
     `load_sales` (transformations): same pattern — Generate Rows →
     Write to Log is enough for the lab; add a named parameter `date`
     (Edit → Settings → Parameters) and log it.
   - `nightly_job`, `long_running_job` (jobs): Start → your
     transformation → Success. For `long_running_job` add a
     Wait For (e.g. 2 minutes) entry so deferrable mode is
     observable.
   - `/home/bi/reporting/publish_reports` (job) and `build_marts`
     (job): Start → Write To Log → Success.

   Save each into the repository under `/home/bi` (File → Save; pick
   the directory). On disk they appear under `C:\PDI-Repo\home\bi`.

> **Shortcut — skip Parts 1 and 2 entirely:** the lab can run Carte in
> Docker instead, pre-seeded with runnable repository content (cloned
> from the How-To course files):
>
> ```powershell
> cd C:\Projects\PDI-AirFlow\lab\docker
> docker compose --profile carte up -d --build   # first build ~1.5 GB
> ```
>
> The container publishes host port 8081, so the same `pdi_default`
> connection works unchanged. Use Parts 1–2 below when you want the
> full Spoon experience with your own repository.

## Part 2 — Carte (host install)

1. **Configuration**: use
   [carte/carte-config.xml](carte/carte-config.xml) (master on port
   8081, binds 0.0.0.0).

2. **Start Carte**:

   ```powershell
   cd C:\Pentaho\data-integration
   .\Carte.bat C:\Projects\PDI-AirFlow\lab\carte\carte-config.xml
   ```

   Keep this window open (or register it as a service later).

3. **Verify**: browse to
   `http://localhost:8081/kettle/status/` — log in with Carte's basic
   auth `cluster` / `cluster` (change in
   `data-integration\pwd\kettle.pwd` for anything non-lab). You
   should see the slave server status page.

4. **Firewall**: if Windows Defender prompts, allow Java on private
   networks — the Airflow container connects in via
   `host.docker.internal`.

## Part 3 — Airflow + Marquez (Docker)

1. **Start Docker Desktop** and wait until it reports "running".

2. **Bring up the stack** (Windows uses the 2.10 compose):

   ```powershell
   cd C:\PDI-Airflow\lab\docker
   copy .env.example .env                 # then set DAGS_DIR if needed
   docker compose -f docker-compose.win.yml up -d --build   # builds the 2.10 image
   docker compose -f docker-compose.win.yml logs -f airflow-webserver
   ```

   The compose file:
   - runs Airflow 2.10.5 as **separate webserver / scheduler /
     triggerer** services with Postgres, each `restart: unless-stopped`
     (a crashed process is auto-restarted — no more limping
     `standalone`);
   - **bakes** the **Pentaho provider** + **OpenLineage provider** into
     the image (`airflow/Dockerfile`), so boots are fast and never
     pip-install at runtime;
   - mounts `workshop/dags` as the dags folder;
   - pre-creates the `pdi_default` connection pointing at
     `host.docker.internal:8081` (env var `AIRFLOW_CONN_PDI_DEFAULT`);
   - configures OpenLineage to POST lineage events to Marquez;
   - runs Marquez (API :5000, UI :3000).

3. **Log in to Airflow**: http://localhost:8088 — `admin` / `admin`.
   Under Admin → Providers you should see `airflow-provider-pentaho`;
   the workshop DAGs appear on the DAGs page.

4. **Smoke test the Carte connection** from inside the container:

   ```powershell
   docker compose exec airflow python -c "from airflow_pentaho.hooks.carte import PentahoCarteHook; c = PentahoCarteHook().get_conn(); print(c.host, c.port)"
   ```

5. **Marquez**: http://localhost:3000 (UI) and
   http://localhost:6001/api/v1/namespaces (API — host port 6001; 5000-5002
   belongs to PDC-Glossary-Generator). The `pdi`
   namespace appears after the first DAG run emits lineage.

## Part 4 — pdi2dag (migration app)

On the host:

```powershell
cd C:\Projects\PDI-AirFlow
py -3.10 -m venv .venv
.\.venv\Scripts\pip install -e .[dev]
.\.venv\Scripts\pdi2dag --version
```

Migrate the bundled sample job and hand it to the running Airflow:

```powershell
.\.venv\Scripts\pdi2dag migrate samples\nightly_etl.kjb `
    --schedule "0 6 * * *" `
    --param "date={{ ds }}" `
    --dags-folder workshop\dags\deploy-target `
    --airflow-url http://localhost:8088 `
    --airflow-user admin --airflow-password admin `
    --trigger
```

## Alternative: Astronomer (astro CLI)

For an Astro-based lab instead of raw compose:

```powershell
winget install Astronomer.Astro
mkdir astro-pdi && cd astro-pdi
astro dev init
```

Add to `requirements.txt` (or copy the provider folder into the
project and reference it in the Dockerfile):

```
apache-airflow-providers-openlineage
airflow-provider-pentaho
```

Set the connection in `.env` (same `AIRFLOW_CONN_PDI_DEFAULT` value as
the compose file) and `astro dev start`. Marquez still runs from
`lab/docker` (comment out the airflow/airflow-db services or run
`docker compose up -d marquez-db marquez-api marquez-web`), with the
OpenLineage transport URL `http://host.docker.internal:5000`.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Airflow container restarts repeatedly | `docker compose logs airflow` — usually a pip failure in `_PIP_ADDITIONAL_REQUIREMENTS`; check the provider mount path. |
| Carte task fails `ConnectionError` | Carte not running, wrong port, or firewall blocking Docker → host. Test `http://localhost:8081/kettle/status/` on the host first. |
| `Unknown error` / `Unable to find job` from Carte | The repo path in the DAG doesn't exist in the `Default` repository — check spelling and the `/home/bi` folder. |
| Carte returns 401 | `carte_username`/`carte_password` in the connection extra don't match `pwd\kettle.pwd`. |
| DAG not appearing | It only parses if the provider import works: `docker compose exec airflow airflow dags list-import-errors`. |
| No lineage in Marquez | Check `docker compose logs airflow` for `openlineage` errors and that the transport URL uses `marquez-api:5000`. |
| Deferrable task stuck in `deferred` | Triggerer must be running — `standalone` includes it; on custom setups run `airflow triggerer`. |
