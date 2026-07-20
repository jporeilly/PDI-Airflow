# Changelog

## PDI-AirFlow v1.15.0 - 2026-07-20

- **Lab ships two Carte connections out of the box.** `airflow-init`
  (both `docker-compose.yml` and `docker-compose.win.yml`) now seeds a
  `pdi_cluster` connection into the metadata DB — the cluster topology
  (points at a master via new `CLUSTER_HOST`/`CLUSTER_PORT` in `.env`),
  alongside the env-var `pdi_default` (single server). Because DB
  connections enumerate over the REST API, `pdi_cluster` shows in the
  Studio's connection picker; delete-then-add keeps the seed idempotent
  across restarts. Validated with `docker compose config` on both.
- **LAB-SETUP.md → "Configuring Carte with Airflow (single / custom
  port / cluster)"**: the connection model, single/custom-port/cluster
  recipes, per-DAG selection via `pdi_conn_id`, the shipped
  `pdi_default` vs `pdi_cluster` table, and the env-var-vs-DB
  enumeration gotcha with the `airflow connections add` recipe.

## PDI-AirFlow v1.14.0 - 2026-07-20

- **Carte connection picker on Configure.** The *Carte connection*
  field (which sets the generated DAG's `pdi_conn_id`) is now a picker
  backed by `/api/airflow/connections` — it lists the `pentaho`-type
  connections defined in Airflow so you can target a specific Carte
  server or cluster master per DAG (dev single vs prod cluster). You can
  still type a name that will exist at deploy time. Because Airflow's
  REST API only enumerates metadata-DB connections, env-var connections
  like the lab's `AIRFLOW_CONN_PDI_DEFAULT` don't list — so `pdi_default`
  is always offered as a fallback, and the field note explains the
  distinction. Underlying support (`pdi2dag --conn-id`, generator
  `pdi_conn_id`, per-operator `pdi_conn_id`) was already there; this
  surfaces it in the UI.
  - **Carte topologies:** a single Carte on a custom port is just the
    connection's port; a cluster points the connection at the *master*
    (the `.ktr` carries the cluster schema — Airflow submits to the
    master and polls it); multiple named connections let each DAG pick
    its Carte via `pdi_conn_id`.

## PDI-AirFlow v1.13.0 - 2026-07-20

- **Workshop DAGs split into three folders** under `workshop/dags/`
  (scanned recursively by Airflow, so all still load):
  `workshop/` (the module curriculum — `00_lineage_demo`, `01`–`10`),
  `examples/` (standalone reference DAGs — `mini_job`), and
  `deploy-target/` (empty landing zone where the Studio / `pdi2dag`
  deploy generated DAGs in Module 11; generated `*.py` there are
  git-ignored). The Studio's default *DAGs folder* now points at
  `deploy-target/`, `scripts/deploy.ps1` mirrors the whole tree, and
  Module 11 deploys with `--dags-folder …\deploy-target`. Spelling
  normalised to **DAGs** throughout (folder `C:\PDI-Airflow\DAGs`).
- **Carte / PDI connection test.** New `/api/carte/status` probes the
  Carte server the deployed DAGs delegate to — `GET /kettle/status/?xml=Y`
  behind basic auth (default `cluster`/`cluster`). HTTP 200 = connected,
  401 = reachable but wrong credentials, else offline. Adds a **Carte /
  PDI** group to Settings (URL / user / password) with a **Test
  connection** button and a **Carte status dot** in the sidebar — so all
  four services the Studio touches (Airflow, Carte/PDI, Marquez, PDC)
  are now testable. Default `carte_url` is `http://localhost:8081`
  because the Studio and Carte run on the same Windows box; the LAN IP
  (`192.168.1.100`) is what Airflow on the VM uses to reach Carte.

## PDI-AirFlow v1.12.0 - 2026-07-20

Migrates the lab to **Apache Airflow 3.3** (run in Linux Docker; the
Windows machine only connects over REST) and fixes the Studio's Airflow
status probe against it. Studio webapp → 1.12.0.

**Fixes**

- **Studio showed "Airflow offline" against Airflow 3.3.** The
  `/api/airflow/status` sidebar/Settings probe hard-coded a
  `GET /api/v1/dags` basic-auth request, which **404s on Airflow 3.3**
  (v1 REST API removed) and read as offline — even though deploy
  already used the auto-detecting client. The probe now goes through
  `AirflowClient`: **v2 + JWT** on Airflow 3.x, **v1 + basic auth** on
  2.x. Verified against the VM (192.168.1.200:8088): `api=v2`, DAGs
  listed, sidebar reads *connected*. The response also reports the
  detected `api` version.

**Studio**

- **Test Marquez connection** button + `/api/marquez/status`
  (namespace-count probe), matching the Airflow/PDC test buttons.
- **Browse… button** on Settings → Deployment *Dags folder*: opens the
  OS folder picker on the machine running the Studio (`/api/browse/folder`)
  and fills in the absolute path.

**Airflow 3.3 migration**

- **Lab → Airflow 3.3**: image rebuilt on `apache/airflow:3.3.0` with
  the Pentaho + OpenLineage + standard + FAB providers baked in;
  compose runs `api-server` / `scheduler` / `triggerer` /
  `dag-processor` as separate `restart: unless-stopped` services with
  FAB auth (admin/admin) and JWT. Validated: health 200, JWT
  `/auth/token`, DAGs load with 0 import errors.
- **pdi2dag REST client** auto-detects **API v2 + JWT** (Airflow 3) and
  falls back to **v1 basic auth** (Airflow 2). Deploy/unpause/trigger/
  status work on both.
- **Provider** already used `airflow.sdk` (3.x) with a 2.x fallback —
  confirmed on 3.3 in the Linux container. `airflow.sdk` can't import
  on **native Windows** (Airflow's `os.register_at_fork` is POSIX-only),
  so provider unit tests run on Linux or against Airflow 2.10.5 locally.
- **Workshop DAGs** made dual-compatible (2.x + 3.x): `BashOperator`/
  `BranchPythonOperator` (providers.standard), `Dataset`→`Asset`,
  `airflow.decorators.task`→`airflow.sdk.task`.

**Deployment**

- **Two documented options** (LAB-SETUP.md + INSTALL.md):
  - **A — Windows 11 / Airflow 2.10.5** via Docker Desktop
    (`docker-compose.win.yml` + `airflow2/Dockerfile`), REST API v1,
    DAGs in `C:\PDI-Airflow\DAGs`.
  - **B — Ubuntu 24.04 VM / Airflow 3.3** (`docker-compose.yml`),
    REST API v2/JWT, DAGs on the VM. The Studio's REST client
    auto-detects v1 vs v2, so one Studio drives either.
- **Configurable DAGs folder**: `DAGS_DIR` in `lab/docker/.env`
  (default `../../workshop/dags`; Windows deploy uses
  `C:\PDI-Airflow\DAGs`). `scripts/deploy.ps1` creates the DAGs folder,
  seeds it with the workshop DAGs, and copies a runnable Studio to
  `C:\PDI-Airflow` (UI built, no venv/node_modules).
- **Ubuntu 24.04 target**: `lab/UBUNTU-SETUP.md` + `lab/docker/
  .env.example`; Carte connection parameterised (`CARTE_HOST` etc.) so
  Airflow on the VM reaches Carte on the Windows machine.
- GitHub: https://github.com/jporeilly/PDI-Airflow (private).

Note: the lab now targets Airflow 3.3 (option B); `pdi_default` uses
`.env` (`cp .env.example .env`).

## PDI-AirFlow v1.11.0 - 2026-07-19

- **File data sources** — the emitter now handles file-based steps
  (CSV, Text File In/Out, Excel, JSON, Fixed) in addition to DB tables.
  File paths become OpenLineage file datasets (object stores keep
  their scheme, e.g. `s3://bucket` + key; local/VFS files use the
  `file` namespace). New `import_transcripts_csv.ktr` sample shows
  CSV → DB lineage. Parser reads `<filename>` and `<file><name>` forms.
- **Settings** grouped into Deployment / Airflow / Marquez / PDC, each
  service with a **Test connection** button (PDC and Airflow probe
  `/api/{pdc,airflow}/status`) — PDC connect config is now explicit
  and verifiable. Added `pdi_server` (ETL Server node name).
- **Home diagram** de-cluttered — Pentaho Data Catalog moved **below
  Carte / PDI**, cleaner arrow routing.
- 2 new tests (41 total).

## PDI-AirFlow v1.10.0 - 2026-07-19

- **Stable lab Airflow** — replaced the flaky single `standalone`
  container with separate **webserver / scheduler / triggerer**
  services, each `restart: unless-stopped` (a crashed process now
  exits its container and Docker restarts it). The Pentaho provider +
  OpenLineage are **baked into a custom image** (`lab/docker/airflow/
  Dockerfile`) instead of pip-installed on every boot — fast, reliable
  starts. `.dockerignore` keeps the build context small.
- **PDC in the Studio** — sidebar now shows a **PDC** status dot
  (offline / reachable / connected) alongside Airflow, and a **PDC**
  link in the footer; new `GET /api/pdc/status` endpoint. The Home
  architecture diagram now includes **Marquez** (orchestration
  lineage) and **Pentaho Data Catalog** (ETL lineage) with the Carte
  row-count enrichment arrow.
- **`run.ps1` / `run.sh`** — one-command Studio launch (venv, deps,
  UI build, serve); prefer a 64-bit Python.
- **Makefile** — one-stop `make install / run / dev / test / lab-up /
  lab-down / carte-up / clean` in the house style.
- Note: Apache Airflow needs a **64-bit** Python locally (`msgspec`
  has no 32-bit Windows build); the scripts pick one automatically.

## PDI-AirFlow v1.9.0 - 2026-07-19

**Carte runtime enrichment — free runtime lineage (no paid plugin).**
The Carte `transStatus` we already poll carries a per-step
`stepstatuslist` with real row counts; we now use it.

- `parse_carte_step_metrics()` extracts per-step
  read/written/input/output/rejected/errors from a Carte transStatus.
- `trans_datasets(detail, step_metrics)` attaches real `rowCount` to
  each dataset (Table Input rows read, Table Output rows written).
- Emitted OpenLineage now carries **`outputStatistics` / input
  `dataQualityMetrics` rowCount facets** and a real **FAIL** event
  when a step reports errors — genuine runtime lineage, table-level.
- CLI: `pdi2dag lineage --carte-status <file|dir>` supplies a captured
  transStatus (single .xml, or `<transname>.xml` per job trans).
- README architecture flowchart now includes **PDC** and both lineage
  producers; documents the three lineage paths (Airflow OL → Marquez,
  pdi2dag → PDC/Marquez, paid PDI plugin → PDC runtime/column-level).
- 7 new tests (39 total).

Ceiling vs the paid plugin: this gives table lineage + real row counts
+ run state; column-level lineage still requires the plugin (or an
offline SQL+schema analyzer).

## PDI-AirFlow v1.8.0 - 2026-07-19

- **`pdi2dag provision`** — pre-create PDC data connections from a PDI
  file's `<connection>` definitions so lineage attaches to real,
  credentialed connections instead of credential-less stubs. Creates
  each connection with its identity (type/host/port/database),
  username and referenced schemas; **never handles the password** —
  the user completes credentials in PDC. `--dry-run` previews.
  (`collect_connections` / `build_connection_body` /
  `provision_connections` in lineage.py; parser now reads
  `<username>` — never `<password>`.)
- CSCU sample connections carry usernames (`cscu_etl`, `dw_loader`).

## PDI-AirFlow v1.7.1 - 2026-07-19

- **Dataset naming matched to the plugin exactly** (decompiled
  `SqlDataset`): dataset name is now `database.schema.table`
  (FORMAT_DB_SCHEMA_TABLE `%s.%s.%s`), not `schema.table`. Namespace
  stays `<protocol>://<host>:<port>` (`getNamespaceFormat` `%s://%s:%s`,
  no database). Fixes PDC recording `databaseName: POSTGRES` — it now
  captures the real database (e.g. `cscu`), so a lineage dataset keys
  to the correct connection.
- Documented PDC connection model (see README/notes): OpenLineage
  auto-creates a **credential-less connection stub** (host/port/type
  only, `users: []`); it matches connections by that **identity**
  (protocol/host/port + database.schema.table), NOT by the PDI
  connection name. Real credentials must be pre-created/enriched in
  PDC — lineage never carries secrets.

## PDI-AirFlow v1.7.0 - 2026-07-19

**Real table lineage into PDC — the CSCU deliverable, working.**
Confirmed PDC ingests OpenLineage events and auto-creates data
connections + TABLE entities + lineage from dataset inputs/outputs.
The empty ETL tree was because sample transformations had no table
steps.

- **Parser**: `parse_trans_detail` now extracts `<connection>`
  definitions and Table Input (connection + SQL) / Table Output
  (connection + schema + table) step details (`PdiConnection`;
  `PdiStep.connection/sql/schema/table`).
- **Emitter**: `trans_datasets()` resolves a transformation's
  input/output DB datasets (namespace `<scheme>://<host>:<port>`,
  name `schema.table`; source tables parsed from Table Input SQL).
  `build_pdc_etl_events` and new `build_pdc_trans_events` attach these
  datasets so PDC builds connections, tables and lineage.
- **CSCU sample pipelines** (`samples/cscu/`): `extract_enrollment`
  (registrar.enrollment → staging.enrollment_stg),
  `build_student_mart` (staging.enrollment_stg + dim.student →
  mart.student_enrollment_fact), and the `cscu_enrollment_load` job.
  Verified live: pushing this to PDC created
  `POSTGRES-cscu-db-registrar` / `POSTGRES-cscu-dw-staging` data
  sources and the enrollment→staging→mart table lineage.
- CLI/Studio pass `trans_details` through to the PDC emitter.

## PDI-AirFlow v1.6.0 - 2026-07-19

**PDC ETL Pipelines — matched to the real plugin.** Decompiled the
official PDI OpenLineage plugin JAR (pdi-openlineage-plugin-core
0.7.0-292) to learn the exact OpenLineage convention PDC's ETL tree is
built from, and rewrote `build_pdc_etl_events` to mirror it:

- **namespace = the PDI Server hostname** (was a flat `pdi`) — this is
  the node PDC groups jobs under (`TransHelper.getJobNamespace` →
  config localHostname / HostnameResolver; `file://` URI for
  file-based). Configurable via `--pdi-server` / the `pdi_server`
  setting (default `pdi2dag`).
- **job/transformation name = repository path with leading `/`**
  (`getPathAndName` + `prependIfMissing '/'`), e.g.
  `/home/bi/nightly_etl` — path segments become the tree's Folders.
- **jobType facet**: `integration=PDI` (was PENTAHO), lowercase
  `jobType=job|transformation` (was JOB/TRANS), `processingType=BATCH`.
- **producer = the plugin's GitHub URL**, so PDC treats our events
  identically to the plugin's.
- Job > Transformation nesting via **ParentRunFacet (run+job+root)**,
  not datasets; no synthetic `pdi://` datasets in PDC events.

The earlier empty tree was because our events used a flat namespace and
dotted names — PDC couldn't derive Server/Folder/Job. Marquez profile
(step-spliced graph) is unchanged.

## PDI-AirFlow v1.5.1 - 2026-07-19

- **PDC ETL Pipelines tree** work: events reach PDC's lineage store
  (verified via GET /lineage/api/events) but the ETL *tree* did not
  materialize. Added a PDC/plugin-shaped event profile
  (`build_pdc_etl_events`: repository-path job names, ParentRunFacet
  transformation→job linkage, processing_engine facet, no step jobs)
  and a **file-import path** that needs no API knowledge:
  `pdi2dag lineage --out-file pdi.openlineage.json` and a Studio
  **Download for PDC Import** button both emit newline-delimited
  OpenLineage (the PDI plugin file-consumer format) for PDC's
  ETL → Actions → **Import**.
- OPEN: exact ETL-tree ingestion contract still unconfirmed — blind
  probing of /etl-service/api/v1/* found only health-check; needs one
  captured request from the ETL page or a successful Import.

## PDI-AirFlow v1.5.0 - 2026-07-19

**PDC integration — the strategic lineage destination.**

- Discovered and verified PDC v11's OpenLineage ingestion endpoint
  (`POST /lineage/api/events`, Keycloak auth with `pdc-client` /
  `scope=openid`) — the same contract the official PDI OpenLineage
  plugin (PDI 10.2+) uses. Our emitter's events are accepted verbatim
  ("Event(s) created successfully") and readable back via
  `GET /lineage/api/events`.
- `pdi2dag lineage --pdc-url https://pentaho.io --pdc-user ...` sends
  the connected job+step graph straight into PDC (new `emit_pdc()` in
  pdi2dag/lineage.py).
- Migration Studio: **Publish to PDC** button beside Publish to
  Marquez (`/api/lineage/publish` gained a `target` field); PDC
  URL/user/password under Settings (stored in the gitignored
  webapp/settings.json).
- Verified live against the CSCU scenario on PDC-Demo
  (https://pentaho.io, vhost for 192.168.1.200): 20 events for
  nightly_etl + step graphs ingested via CLI, 12 more via the Studio.
- Recon notes: PDC app-shell config at `/api/config` maps all
  services (`/etl-service`, `alteryx-lineage-service:8181`,
  `/glossary-service`, ...); public v2 OpenAPI at
  `/api/public/v2/openapi.json`; the UI is vhost-routed — raw IP
  gets 401, use the hosts-mapped name.

## PDI-AirFlow v1.4.0 - 2026-07-18

- **PDI Graph page** — our own hierarchical lineage viewer, the view
  Marquez structurally can't render: Job → Transformations → Steps as
  one nested graph. Entry cards laid out by dependency layer with SVG
  edges; each transformation card folds/unfolds its step list
  (name + step type); missing .ktr files reported per card; latest
  run states overlaid from Marquez (best effort). Backed by
  `POST /api/pdi/graph`. Hand-rolled layered layout — no new
  frontend dependencies (suite rule).
- **API docs links** in the sidebar footer (`/docs` Swagger UI,
  `/redoc`); backend rewritten with Pydantic models so the OpenAPI
  schema documents every request/response (tags: pdi, jobs, lineage,
  services).
- Lineage page: **Type column** from the OpenLineage jobType facet
  (step / transformation / job entry / airflow dag / airflow task).
- Positioning: Marquez stays as the OpenLineage interop backend
  (Airflow runtime lineage lands there automatically); the PDI Graph
  page is the PDI-native presentation. Marquez remains structure-only
  — Carte owns runtime row metrics (decision recorded in workshop
  module 12).

## PDI-AirFlow v1.3.1 - 2026-07-18

- **Connected lineage graph**: job entries and transformation steps
  were emitted as separate islands — Marquez showed both but with no
  edges between them. Step graphs are now *spliced into* the job
  graph: the entry job writes a `.../start` dataset consumed by the
  transformation's first step(s), and the terminal step(s) produce the
  entry's result dataset that downstream entries read. One continuous
  graph: `Extract_Sales (entry) → Get_Variables → Write_to_log →
  Load_Warehouse (entry) → …`. Applied to both `pdi2dag lineage
  --ktr-dir` and the Studio's publish endpoint; 3 new tests (32).

## PDI-AirFlow v1.3.0 - 2026-07-18

Migration Studio batch upgrade:

- **Drag & drop loading** — the Load page is now a dropzone (click to
  browse still works); multiple .kjb/.ktr files at once.
- **Batch migration** — every job and standalone transformation in the
  batch becomes its own DAG (shared schedule/options); Deploy runs all
  migrate jobs in parallel so a batch costs one scheduler scan, not
  one per DAG; per-DAG progress and outcomes in a table.
- **Dependency awareness** — a loaded job lists the transformations
  its TRANS entries require and shows found ✓ / missing per file;
  dropped .ktr files referenced by a job are classified as
  dependencies (validation + step lineage), not extra DAGs. Missing
  ones don't block migration (tasks call Carte by repository path).
- **One-click lineage** — new `POST /api/lineage/publish` + Deploy-page
  button publishes the loaded batch's PDI structure (job entry graphs +
  step graphs) to Marquez.

## PDI-AirFlow v1.2.0 - 2026-07-18

- **PDI structure in Marquez**: new `pdi2dag lineage` command +
  `pdi2dag/lineage.py` — publishes .kjb entry graphs (hops as
  dataset edges) and .ktr step graphs as OpenLineage jobs, so Marquez
  shows the inside of a PDI job, not just the Airflow tasks. Parser
  gained `parse_trans_detail()` (steps + step hops). 5 new tests
  (29 total). Verified live: `nightly_etl` entry graph + step graphs
  for 3 transformations visible in the `pdi` namespace.
- **Final port map** (avoiding PDC apps and the Pentaho Server):
  Airflow 8090→**8088** (8090 is also Pentaho Server), Marquez API
  5002→**6001**, admin →**6002** (exactly 6000 is blocked by Chrome
  as an unsafe port). Studio defaults updated.
- Ecosystem notes (see README): the PyPI `airflow-pentaho-plugin` is
  the same damavis 1.2.3 package this provider modernizes;
  damavis' `airflow-hop-plugin` is its Apache Hop successor (Hop
  Server REST ≈ Carte REST); `apache-airflow-providers-kettle` is a
  third-party kitchen/pan-only wrapper (local execution, no Carte).

## PDI-AirFlow v1.1.0 - 2026-07-18

- **Migration Studio web app** (`webapp/`): React 18 + Vite frontend
  following the PDC suite conventions (shared stylesheet, themes,
  `.card` shell, `{"error"}` API contract, background-jobs pattern) +
  FastAPI backend on **:5012** wrapping the pdi2dag core. Pages: Home
  (architecture diagram), Load → Configure → Preview → Deploy stepper,
  Lineage (live Marquez data), Settings. Verified end-to-end against
  the running lab (convert → deploy → parse-wait → unpause).
- **Course integration** (`course/`): COURSE-UPDATE.md maps every
  technique from the 2023 "How-To: Apache Airflow" course to its
  modern replacement; modernized course DAGs included. Course masters
  on P: untouched.
- **Containerized Carte** (`lab/docker/carte/`, compose profile
  `carte`): modernized from the course's setup-pentaho image (Java 11,
  PDI 9.4), repository pre-seeded with course-derived content incl.
  `/home/bi` workshop clones — the lab no longer requires PDI on the
  host.
- **Port fixes**: Marquez API now on host **6001** (5000-5002 belong to
  PDC-Glossary-Generator); Marquez web UI fixed (missing `WEB_PORT`
  env left the container listening on nothing — localhost:3000 now
  serves).
- Mermaid diagrams added to README, WORKSHOP (lineage flow) and
  COURSE-UPDATE; port map documented in README.
- `pdi2dag` wait_for_dag default timeout raised to 360 s to cover
  Airflow's 5-minute new-file scan.

## PDI-AirFlow v1.0.0 - 2026-07-18

Initial release.

- **airflow-pentaho-provider 2.0.0** bundled (see its own
  CHANGELOG.md): Airflow 2.7+/3.x provider with Carte + Kitchen/Pan
  operators, deferrable mode, POST credentials, stopJob/stopTrans on
  kill.
- **Workshop**: 12 modules covering Carte + local execution, cron
  scheduling, templating, dependencies/trigger rules, deferrable
  mode, failure handling, Datasets, dynamic task mapping, migration
  and Marquez lineage; 10 ready-made workshop DAGs.
- **Lab**: setup guide (PDI + file repository + Carte on host),
  Docker Compose stack (Airflow 2.10 standalone + Marquez 0.50 +
  OpenLineage wiring), Carte and repositories.xml templates,
  Astronomer alternative.
- **pdi2dag 1.0.0**: .kjb/.ktr parser, DAG generator (wrap/explode
  modes, hop→dependency collapse, failure-hop trigger rules,
  parameter mapping, deferrable option), Airflow REST deployment
  (wait/unpause/trigger), CLI (`inspect`, `convert`, `migrate`,
  `deploy`), 24 unit tests incl. generated-DAG import test under real
  Airflow.
