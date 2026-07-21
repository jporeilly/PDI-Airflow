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
│  in pipelines\ (install)     │ Airflow (3 services) :8088      │ │
│            ▲                 │   airflow-provider-pentaho      │ │
│            │ Carte REST      │   openlineage provider          │ │
│            └─────────────────│                                 │ │
│         host.docker.internal │ Marquez API :6001 / UI :3000    │ │
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

1. **Install PDI** and verify `Spoon.bat` starts. The Pentaho-suite
   installer puts it at `C:\Pentaho\design-tools\data-integration`; a CE
   zip unzips to wherever you extract it (e.g. `C:\Pentaho\data-integration`).
   Use *your* path in the commands below.

   > **EE only:** Carte needs a valid Pentaho license or transformations
   > fail to run (the Carte log prints `License validated.` when it's in
   > place). Apply it once with the license installer:
   >
   > ```powershell
   > cd C:\Pentaho\design-tools\data-integration
   > .\install_license.bat install C:\path\to\your-license.lic
   > ```

2. **Seed the pipelines folder** with the ready-made content so
   `/demo/hello_world` (Module 1) works immediately. The file-repository
   content lives under `pipelines\<scenario>\`:

   ```powershell
   mkdir C:\PDI-Airflow\pipelines
   xcopy /E /I C:\Projects\PDI-AirFlow\lab\carte\pipelines\* C:\PDI-Airflow\pipelines\
   ```

   That gives you `/demo/hello_world` and the `/CSCU/*` capstone
   pipelines. (Or just run `.\scripts\deploy.ps1`, which stages
   `pipelines\`, `repositories\` and `.kettle\` for you.)

3. **Register the repository**: copy
   [carte/repositories.xml](carte/repositories.xml) to
   `C:\Users\<you>\.kettle\repositories.xml` (create the `.kettle`
   folder if needed). Its `base_directory` points at
   `C:\PDI-Airflow\pipelines` — adjust if you used a different path. This
   defines a *file repository* named `Default` — no database server
   required. (The turnkey `run-carte.ps1` handles this via
   `KETTLE_HOME`, leaving your global `~/.kettle` untouched.)

4. **Connect Spoon to it**: start Spoon → Tools → Repository →
   Connect → `Default`. Any username/password works for a file
   repository (the lab uses `admin`/`password` to match the Airflow
   connection).

5. **Create the remaining workshop content** in the repository (folder
   `/demo`). `hello_world` is already seeded by step 2; add these as
   you reach the modules that need them:

   - `extract_sales`, `extract_customers`, `load_warehouse`,
     `load_sales` (transformations): same pattern — Generate Rows →
     Write to Log is enough for the lab; add a named parameter `date`
     (Edit → Settings → Parameters) and log it.
   - `nightly_job`, `long_running_job` (jobs): Start → your
     transformation → Success. For `long_running_job` add a
     Wait For (e.g. 2 minutes) entry so deferrable mode is
     observable.
   - `/demo/reporting/publish_reports` (job) and `build_marts`
     (job): Start → Write To Log → Success.

   Save each into the repository under `/demo` (File → Save; pick
   the directory). On disk they appear under `C:\PDI-Airflow\pipelines\demo`.

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

> **Turnkey (deployed install):** `scripts\deploy.ps1` stages a
> self-contained Carte setup under `C:\PDI-Airflow` —
> `carte\carte-config.xml`, the file repository at `repositories\`
> (seeded with `hello_world`), and a `.kettle\repositories.xml`. Start
> it with one command, which sets `KETTLE_HOME` to that folder so your
> global `~/.kettle` is untouched:
>
> ```powershell
> cd C:\PDI-Airflow
> .\run-carte.ps1           # single master :8081 (add -PdiHome <path> if PDI isn't auto-found)
> .\run-carte-cluster.ps1   # or a local cluster: master :8081 + slaves :8082/:8083
> ```
>
> The cluster launcher starts a master and two slaves (each in its own
> window); point the `pdi_cluster` connection at the master and run a
> transformation carrying a cluster schema to fan work across the slaves.
> The manual steps below are for running Carte straight from the source
> repo.

1. **Configuration**: use
   [carte/carte-config.xml](carte/carte-config.xml) (master on port
   8081, binds 0.0.0.0).

2. **Start Carte**:

   ```powershell
   cd C:\Pentaho\design-tools\data-integration   # or your PDI folder
   .\Carte.bat C:\Projects\PDI-AirFlow\lab\carte\carte-config.xml
   ```

   Keep this window open (or register it as a service later).

3. **Verify**: browse to
   `http://localhost:8081/kettle/status/` — log in with Carte's basic
   auth `cluster` / `cluster` (change in
   `data-integration\pwd\kettle.pwd` for anything non-lab). You
   should see the slave server status page.

4. **Firewall** — required for the **Ubuntu VM topology (Option B)**,
   where Airflow runs on another machine and reaches Carte over the LAN.
   Windows Firewall blocks inbound 8081 by default, and if your LAN is
   classified **Public** a *Private*-only rule won't apply — so open it
   for all profiles, in an **elevated** PowerShell:

   ```powershell
   New-NetFirewallRule -DisplayName "Carte 8081 (lab)" -Direction Inbound `
     -Protocol TCP -LocalPort 8081 -Action Allow -Profile Any
   ```

   Verify from the VM (replace with the Windows machine's LAN IP):

   ```bash
   curl -u cluster:cluster http://192.168.1.100:8081/kettle/status/?xml=Y
   ```

   A `<serverstatus>` back = the hop is open. For **Option A** (Docker
   Desktop on the same Windows box) the container reaches Carte via
   `host.docker.internal` and usually needs no rule.

### Configuring Carte with Airflow (single / custom port / cluster)

Airflow talks to Carte through an **Airflow connection** (type
`pentaho`). Every `CarteJobOperator` / `CarteTransOperator` POSTs
`executeJob` / `executeTrans` to that connection's `host:port` and polls
status there — so a task always targets **one** Carte endpoint, chosen
by its `pdi_conn_id` (default `pdi_default`). Clustering is a PDI-side
concern; Airflow just submits to the server you point it at.

Connection fields: **Host** (include `http://`/`https://`), **Port**,
**Login/Password** = PDI *repository* creds, **Extra** =
`{"rep","carte_username","carte_password","verify_ssl","timeout"}` where
`carte_*` is Carte's own basic auth (`cluster`/`cluster`).

**Single server, default or custom port (e.g. 9000).** Just set the
port. In the lab it's parameterised in `.env`:

```bash
CARTE_HOST=192.168.1.100
CARTE_PORT=9000          # -> env-var connection pdi_default
```

Outside the lab: Airflow UI → *Admin → Connections → pdi_default → Port*,
or set the `AIRFLOW_CONN_PDI_DEFAULT` env var.

**Cluster (master + slaves).** Point the connection at the **master**:

```bash
CLUSTER_HOST=carte-master.lan
CLUSTER_PORT=8080        # -> DB connection pdi_cluster (seeded by airflow-init)
```

The `.ktr` carries the cluster schema (master + slave definitions, steps
flagged to run clustered); when the master receives `executeTrans` it
distributes to the slaves. Airflow submits to and polls the **master**
only — no slave config on the Airflow side.

**Choosing per DAG (dev single vs prod cluster).** Define several
connections and pick one per DAG via `pdi_conn_id`. The lab ships two out
of the box:

| Connection | Topology | Defined as | Lists in picker? |
|---|---|---|---|
| `pdi_default` | single server | env var (`AIRFLOW_CONN_PDI_DEFAULT`) | offered as fallback |
| `pdi_cluster` | cluster master | DB (seeded by `airflow-init`) | yes |

In the Studio's **Configure** page the *Carte connection* field is a
picker over the `pentaho`-type connections; or with the CLI:

```powershell
pdi2dag convert build_member_mart.ktr --conn-id pdi_cluster
# -> CarteTransOperator(..., pdi_conn_id='pdi_cluster')
```

**Gotcha — env-var vs DB connections.** Airflow's REST API (and so the
picker) only enumerates connections stored in the **metadata DB**.
Env-var connections like `pdi_default` work at runtime but don't list —
hence `pdi_default` is always offered as a fallback. To make a new Carte
selectable in the picker, add it to the DB:

```bash
docker compose exec airflow-scheduler airflow connections add pdi_edge \
  --conn-type pentaho --conn-host http://carte-edge.lan --conn-port 8081 \
  --conn-login admin --conn-password '***' \
  --conn-extra '{"rep":"Default","carte_username":"cluster","carte_password":"cluster"}'
```

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
   - mounts the DAGs folder (`DAGS_DIR`) — the Ubuntu compose defaults
     to `workshop/dags`, the Windows compose to `C:\PDI-Airflow\DAGs`;
   - pre-creates the `pdi_default` connection pointing at
     `host.docker.internal:8081` (env var `AIRFLOW_CONN_PDI_DEFAULT`),
     plus a DB-seeded `pdi_cluster` connection for the cluster topology;
   - configures OpenLineage to POST lineage events to Marquez;
   - runs Marquez (API :6001, UI :3000).

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
OpenLineage transport URL `http://host.docker.internal:6001`.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Airflow container restarts repeatedly | `docker compose logs airflow` — usually a pip failure in `_PIP_ADDITIONAL_REQUIREMENTS`; check the provider mount path. |
| Carte task fails `ConnectionError` | Carte not running, wrong port, or firewall blocking Docker → host. Test `http://localhost:8081/kettle/status/` on the host first. |
| `Unknown error` / `Unable to find job` from Carte | The repo path in the DAG doesn't exist in the `Default` repository — check spelling and the `/demo` folder. |
| Carte returns 401 | `carte_username`/`carte_password` in the connection extra don't match `pwd\kettle.pwd`. |
| DAG not appearing | It only parses if the provider import works: `docker compose exec airflow airflow dags list-import-errors`. |
| No lineage in Marquez | Check `docker compose logs airflow` for `openlineage` errors and that the transport URL uses `marquez-api:5000`. |
| Scheduler/UI-triggered task fails fast (log stops at `Pre Execute`), scheduler shows `SIGKILL` / `Workload execution failed`, but `airflow tasks test <dag> <task>` succeeds | Airflow 3 split scheduler/api-server: task workers can't reach the **execution API** (default `localhost:8080` = the scheduler's own container), so they can't heartbeat and are SIGKILLed. Set `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-apiserver:8080/execution/` (in `docker-compose.yml`); `docker compose up -d` to apply. |
| Deferrable task stuck in `deferred` | Triggerer must be running — the lab's `airflow-triggerer` service provides it; on custom setups run `airflow triggerer`. |
| MinIO/S3 VFS connection: `IllegalArgumentException: unknown signer type: aws.endpoint` | Signature Version must be the AWS SDK **signer type** `AWSS3V4SignerType`, **not** the algorithm string `AWS4-HMAC-SHA256`. Also check **PathStyle Access** is on (MinIO requires it). |
| Carte step fails `!BaseDatabaseStep.Init.ConnectionMissing!` although the connection works in Spoon | **Shared** DB connections live in `.kettle\shared.xml`, which Spoon writes to your **global** `%USERPROFILE%\.kettle`. `run-carte.ps1` sets `KETTLE_HOME` to the install, so Carte reads `C:\PDI-Airflow\.kettle\` instead. The launchers now **sync `shared.xml`** from the global folder on startup - just restart Carte. (Embedded, non-shared connections aren't affected.) |
| Lineage appears in PDC but under no **PDI Server**, or under a made-up one | The event **namespace** becomes PDC's *PDI Server* node - the plugin uses the PDI **server hostname**. The Studio derives it (explicit `pdi_server` setting, else the Carte host, else this machine's hostname) rather than sending a placeholder. Changing it creates a *new* server node; events published under the old name stay put until deleted. |
| A wrong or renamed dataset stays in PDC's lineage graph after deleting the events | **Deleting events does not retract graph edges.** PDC materialises the graph when events arrive and stores it separately from the event log; the lineage service exposes only `/api/events` and `/api/status`, with no graph API, so an edge cannot be withdrawn from outside. Treat published lineage as **append-only**: a renamed output path leaves the old edge in place permanently, beside the new one. Get output paths right *before* publishing, and check the graph (not just the event store) when verifying a cleanup. |
| PDC shows the table but the pipeline never attaches to it | The dataset **namespace must match the catalog exactly**. PDC catalogues PostgreSQL as `postgres://host:port` - emitting `postgresql://` creates a *separate* node with the same name that is never linked. Check the namespace on the asset's Lineage panel in PDC and emit precisely that. |
| Lineage publishes to PDC (HTTP 200) but no pipeline appears | Two causes, both easy to mistake for a broken publish. (1) **Wrong view** - PDI lineage builds the **lineage graph on the data assets**; the **ETL hierarchy is a separate feature** and will never show it. (2) **Unsupported steps emit no dataset.** PDC carries lineage only for **Table input/output, Text file input/output, S3 CSV input, S3 file output, Excel input/writer**. A transformation ending in **Write to Log** emits an input with no output - half an edge, nothing to draw. Add a supported output step. Note PDC returns **200 for events it builds nothing from**, so the response code proves nothing. |
| PDC lineage publish returns **401** on every path, though Keycloak issues a valid token | **Use the hostname, not the IP.** PDC routes by **virtual host**: `https://pentaho.io` serves the app, `https://192.168.1.200` returns a bare 401 for *every* path including `/`. The tell is that the 401 is byte-identical with and without a token and carries no `WWW-Authenticate` - nothing is evaluating the token, the gateway rejects before routing. Needs a `hosts` entry (`192.168.1.200 pentaho.io`). |
| Carte step reading `s3://...` finds nothing, or reports **Finished with 0 rows** | **Use `pvfs://<connection>/<bucket>/<path>`, not plain `s3://`.** The *Default S3 Connection* flag routes bare `s3://` URLs in **Spoon** but **not on Carte**, so the path resolves to nothing. Two aggravating factors: VFS connections live in the Pentaho metastore, which follows `KETTLE_HOME` (the launchers now sync it - needs a Carte restart, the location is cached at JVM start); and with the step's **File required = N** an unresolvable path matches zero files *silently*. Set file-required **Y**. |
| Text file input: `DOS format was specified but only a single line feed character was found, not 2` | The file has Unix **LF** endings but the step's *Content -> Format* is `DOS` (CRLF). Set Format to **mixed**, which accepts either. |
| Carte step reading `s3://...` reports **Finished with 0 rows** and no error | Two things compound here. (1) **VFS connections live in the Pentaho metastore**, which also follows `KETTLE_HOME` - so Carte looked in `C:\PDI-Airflow\.pentaho\metastore` while Spoon saved to your global `%USERPROFILE%\.pentaho\metastore`. The launchers now sync it, but **Carte caches the metastore location at JVM start - you must restart Carte**. (2) The step's **"File required" = N** makes an unresolvable path match zero files *silently*. Set file-required to **Y** while debugging so it fails loudly. |
| `run-carte.ps1`: `'Spoon.bat' is not recognized` | `Carte.bat` calls `Spoon.bat` **relative to the working directory**. The launcher now runs it from the PDI folder (and sets the *process* cwd - `Push-Location` alone doesn't, since child processes inherit `[Environment]::CurrentDirectory`). |
