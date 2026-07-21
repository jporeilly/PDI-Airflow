# Changelog

## PDI-AirFlow v1.22.4 - 2026-07-21

- **PDC status no longer overstates itself.** It probed only Keycloak, so
  the light went green on a successful token while every lineage publish
  returned 401 - the status said "connected" about something it had never
  tested. It now also probes `/lineage/api/events` (GET, so no junk event
  is created) and reports `lineage_ok` + `lineage_detail`. 401/403 means
  the token was refused; anything else means auth was passed.

  Current lab reading: `authenticated: true, lineage_ok: false, HTTP
  401` - which is the honest picture of the outstanding PDC issue.

## PDI-AirFlow v1.22.3 - 2026-07-21

- **Pinned a shared Airflow `secret_key`.** Nothing set it, so each of
  the five containers generated its **own random key** at startup and
  tokens minted by one failed validation in another - surfacing as
  *"please make sure that all your Airflow components have the same
  'secret_key' configured"* and broken log/UI fetches. Same family as the
  `execution_api_server_url` bug: a default that is harmless in
  `standalone` and wrong once the components are split across containers.
  Airflow 3 reads it from `[api]`, 2.x from `[webserver]`; both compose
  files set the right one, overridable via `AIRFLOW_SECRET_KEY` in
  `.env`.

## PDI-AirFlow v1.22.2 - 2026-07-21

- **Fixed the stale-UI-after-rebuild trap.** `index.html` was served with
  default caching, so after `npm run build` the browser kept loading the
  previous fingerprinted bundle and the UI silently ran old code - it
  cost two rounds of confusion during a walkthrough before being spotted.
  The entry point is now `no-cache`; the hashed `assets/*` are marked
  `immutable` with a one-year max-age, so this costs one small
  conditional request per load rather than re-downloading the bundle.
  (Windows note: the path arrives as `assets\index-<hash>.js`, so the
  match normalises separators - the first attempt checked for `/assets/`
  and silently never fired.)

## PDI-AirFlow v1.22.1 - 2026-07-21

**Every long-running lab service now has `restart: always`.** Only the
`airflow-common` anchor carried a policy, so the app containers came back
after a reboot while `airflow-db`, `marquez-db`, `marquez-api` and
`marquez-web` - which had **no policy at all** - stayed down. The result
looks healthy but is not: scheduler/triggerer/dag-processor "running"
with a dead metadata database behind them, the api-server stuck on
*starting*, and `marquez-api` dying 137.

- Added to `airflow-db`, `marquez-db`, `marquez-api`, `marquez-web` and
  the optional `carte` service, in both the Ubuntu and Windows compose
  files.
- Anchor moved `unless-stopped` -> `always`: `unless-stopped` does *not*
  bring a container back after a daemon restart if it had been stopped
  first, which is the exact case here.
- `airflow-init` deliberately stays `on-failure` - it is one-shot, and
  `always` would restart it forever.

## PDI-AirFlow v1.22.0 - 2026-07-21

**Repository path is now a first-class field in the Studio.** Files are
uploaded by *content*, so the folder an object lives in was never
knowable server-side - a transformation in `/CSCU` parsed as if it sat at
the repository root and the generated DAG emitted
`trans='/txn_report'`, which Carte cannot find. The `.ktr`'s own
`<directory>` is no help: Spoon leaves it `/` even for objects saved in a
subfolder.

- **Load page**: new editable **Repo path** column per file, seeded from
  the parsed document and correctable before generating.
- **API**: `PdiFile`/`ConvertRequest` accept `repo_path`; `/api/inspect`
  and `/api/convert` honour it.
- **CLI**: matching `--repo-path` on `convert` and `migrate`.

Verified end to end in the app: dropping `txn_report.ktr`, setting
`/CSCU/txn_report`, and generating produces
`trans='/CSCU/txn_report'` - the path proven to run on Carte.

> Deliberately **not** inferred by walking up the filesystem for a
> `.kettle` marker. That was tried and reverted: callers parse a temp
> copy, so the walk found the user's home `.kettle` and produced
> `/Local/Temp/txn_report`. An explicit path is the only honest source.

## PDI-AirFlow v1.21.2 - 2026-07-21

Got the MinIO ingestion actually working - `ingest_from_minio` now reads
**20 rows** from `cscu-documents/feeds/ach_payments_2026.csv`. Three
separate faults were stacked behind one silent green run.

- **Use `pvfs://<connection>/<bucket>/<path>`, not plain `s3://`.** The
  *Default S3 Connection* flag routes a bare `s3://` URL in **Spoon** but
  **not on Carte** - the path resolved to nothing. The explicit
  connection-scoped `pvfs://` form names the connection in the URL and
  behaves identically in both. `ingest_from_minio.ktr` and
  `import_ach_minio.ktr` updated; the capstone previously claimed the
  `s3://` form "works unchanged", which was wrong.
- **Content format `DOS` -> `mixed`.** The CSV has Unix LF endings, so
  the step failed with *"DOS format was specified but only a single line
  feed character was found, not 2"*. `mixed` accepts either.
- **`file_required` N -> Y.** With it off, an unresolvable path matches
  zero files and Carte reports **Finished, 0 rows, no error** - which is
  what masked all of the above.
- **New `scripts/carte_run.py`.** Identifies a run by *diffing* run-ids
  around the execute call, because `executeTrans` does not reliably
  return one and `transname` is not unique - "take the newest by name"
  silently reports on a previous run. Polls to completion, prints
  per-step row counts, and exits non-zero on a zero-row run.

## PDI-AirFlow v1.21.1 - 2026-07-21

Two launcher fixes found while getting the first live CSCU run working.

- **`run-carte.ps1` now syncs `shared.xml`.** Spoon writes *shared*
  database connections to your **global** `%USERPROFILE%\.kettle\shared.xml`,
  but the launcher sets `KETTLE_HOME` to the install - so Carte read
  `C:\PDI-Airflow\.kettle\` and found no connections at all. Every step
  using a shared connection failed `!BaseDatabaseStep.Init.ConnectionMissing!`
  even though the same connection tested fine in Spoon. The launcher now
  copies the global `shared.xml` in on startup (mirroring the existing
  `repositories.xml` sync) and prints which source it used.
  `run-carte-cluster.ps1` got the same fix - all nodes need it.
- **Fixed `'Spoon.bat' is not recognized`.** `Carte.bat` invokes
  `Spoon.bat` *relative to the working directory*, so calling it by full
  path from elsewhere failed. The launcher now runs it from the PDI
  folder - and sets `[Environment]::CurrentDirectory`, because
  `Push-Location` changes only PowerShell's location, not the **process**
  working directory that child processes inherit.
- **Launchers also sync the Pentaho metastore.** VFS connections
  (Amazon S3/MinIO/HCP) are stored in `%USERPROFILE%\.pentaho\metastore`, which
  follows `KETTLE_HOME` exactly like `shared.xml` - so `s3://` paths
  resolved to nothing under Carte. Worse, it failed *silently*: with the
  step's "File required" off, an unresolvable path matches zero files and
  the transformation reports **Finished, 0 rows, no error**. Note Carte
  caches the metastore location at JVM start, so this needs a restart.
- Added `.gitignore` entries for `.kettle/`, `.pentaho/` and `shared.xml`
  - both files carry credentials (DB passwords, VFS access keys) and the
  launcher writes them into its own folder.
- Documented all of it in LAB-SETUP troubleshooting, plus a capstone note to
  define the `cscu-core` shared connection in Spoon **before** starting
  Carte.

## PDI-AirFlow v1.21.0 - 2026-07-20

- **Profiling added to the capstone sequence + an end-to-end flow
  diagram.** The PDC prerequisite is now the full three-step catalog
  prep - **connect -> ingest/scan -> profile** - with the profiling step
  tied to CSCU courseware Workshop-04/05 (the six rules, the
  `opted_out_marketing` opt-out, the PCI `cvv_cd` triangulation).
- Documented *why* profiling matters to the lineage rather than being
  housekeeping: **row counts reconcile** (PDC profiles N rows; Carte
  reports N read), **sensitivity propagates** (once `ssn`/`cvv_cd` are
  flagged, lineage shows which pipeline carries them downstream), and the
  `opted_out_marketing` compliance trace. Notes the honest limit - this
  emitter is table/dataset-level, not column-level.
- New **mermaid flow diagram** at the top of the capstone: prepare the
  catalog (connect/scan/profile) -> build in Spoon -> migrate + run ->
  metadata lands (Marquez orchestration + PDC pipeline layer), with the
  dependency that profiled facts give the lineage its meaning. Framed as
  **three layers on one asset**: structure, profile, pipeline - only the
  third comes from PDI, and it needs the first two to land on.

## PDI-AirFlow v1.20.6 - 2026-07-20

- **Capstone: the enhanced-PDI-metadata framing made explicit.** The CSCU
  sources must be **registered and scanned in PDC first** (new
  prerequisite, with the pre-filled
  `PDC-Scenarios/data_sources/CSCU/cscu-datasources.csv` connections) -
  that is what lets PDI lineage *enrich governed assets* rather than
  making PDC auto-create bare stub data sources from incoming events.
  Module 5 now states the point directly: scan the **whole** bucket
  including `feeds/`, because the overlap is the demo - a catalogued
  object gains a **pipeline layer** (what consumes it, where the data
  goes, Carte row counts) that PDC cannot derive by scanning. Same on the
  DB side: PDC catalogs `cscu_core` tables, PDI adds the flow between
  them.

## PDI-AirFlow v1.20.5 - 2026-07-20

- **MinIO feed sample + folder ingestion.** Added
  `samples/cscu/data/ach_payments_2026.csv` (20 rows matching the real
  `cscu_core.ach_payments` schema, June dates) to upload to a dedicated
  **`feeds/`** prefix in the `cscu-documents` bucket - keeping PDI feeds
  out of the documents PDC scans. Blueprint + test repointed to
  `s3://cscu-documents/feeds/ach_payments_2026.csv`.
- **Capstone Module 5** now shows building the MinIO reader in Spoon and
  answers the folder question: **Text file input** takes a
  *File/Directory* + *Wildcard (RegExp)* (`s3://cscu-documents/feeds/` +
  `.*\.csv`) to ingest a whole folder - no per-file entry; **CSV file
  input** is the single-file (or filename-from-field) variant.

## PDI-AirFlow v1.20.4 - 2026-07-20

- **CSCU: default transaction date corrected to the sample data.** The
  `since_dt` filter (capstone Module 0 SQL, `extract_transactions.ktr` +
  `cscu_daily_load.kjb` param defaults) now uses `2026-06-01` - the
  `cscu_core.transactions` sample data is June, so `2026-07-01` returned
  no rows. Added `post_dt` to the sample SELECT to match.

## PDI-AirFlow v1.20.3 - 2026-07-20

- **Capstone reframed around the real workflow: build in PDI -> migrate
  -> run.** Clarified that the shipped `samples/cscu/*.ktr` are minimal
  **migration-input blueprints** (connection + SQL + target only) - they
  convert and emit lineage but do **not** render in Spoon or execute on
  Carte. Added **Module 0 - Build the pipeline in PDI (Spoon)** (author a
  real Table Input -> Write to Log `txn_report`), reworked Module 2 to
  migrate and run that executable transformation, and noted the full
  member-360 mart needs a writable `cscu-mart` DB. New `samples/cscu/README.md`
  explains the blueprint-vs-runnable distinction.

## PDI-AirFlow v1.20.2 - 2026-07-20

- **Docs: MinIO VFS connection signer type.** Capstone Module 5 and a
  LAB-SETUP troubleshooting row now record the correct PDI VFS field
  values for the shared MinIO store, including the fix for
  `IllegalArgumentException: unknown signer type: aws.endpoint` - the
  Signature Version must be the AWS SDK signer type `AWSS3V4SignerType`,
  not the algorithm string `AWS4-HMAC-SHA256`. With *Default S3
  Connection* checked, the `s3://cscu-documents/...` path works unchanged.

## PDI-AirFlow v1.20.1 - 2026-07-20

- **CSCU capstone: MinIO / object-store ingestion.** New
  `import_ach_minio.ktr` reads the ACH export from the shared MinIO bucket
  `s3://cscu-documents` (the same bucket PDC/Glossary catalogs) into
  `staging.ach_stg`, so a PDI pipeline consumes an object-store source and
  emits **S3 lineage**: `s3://cscu-documents/ach_payments_2026.csv ->
  cscu_mart.staging.ach_stg` (scheme + bucket kept as the dataset
  namespace, tying it to the same bucket in PDC). Added Capstone Module 5,
  the generated DAG, and a regression test. 46 tests pass.

## PDI-AirFlow v1.20.0 - 2026-07-20

- **Per-vertical scenario structure** mirroring PDC-Scenarios. Each
  scenario has `pipelines/<SCENARIO>/` (the ETL, repo paths
  /<SCENARIO>/...) and `workshop/<SCENARIO>/` (the capstone). Renamed
  `workshop/cscu` -> `workshop/CSCU` (and `workshop/dags/cscu` ->
  `workshop/dags/CSCU`) for consistency, and scaffolded the other three
  verticals with capstone templates wired to their PDC-Scenarios
  databases: **HEALTH** (Lakeshore Health Partners, `lhp_clinical`),
  **MFG** (Cascade Precision Components, `cpc_mfg`), **RETAIL** (Canyon
  Trail Outfitters, `cto_retail`). New `workshop/README.md` scenarios
  index. Adding a vertical = drop a `pipelines/<SCENARIO>/` folder.
- **Carte architecture toggle** in the Studio (Settings -> Carte / PDI ->
  Architecture: single | cluster). It sets the default Carte connection
  (`pdi_default` / `pdi_cluster`) for generated DAGs and shows which
  launcher to run (`run-carte.ps1` / `run-carte-cluster.ps1`). One
  obvious switch for the single-vs-cluster architecture; the Configure
  picker still overrides per DAG. Webapp -> 1.15.0.

## PDI-AirFlow v1.19.0 - 2026-07-20

- **Repository layout: content and definition split.** The Carte file
  repository now separates *content* from *definition*:
  `pipelines\<scenario>\` holds the `.ktr`/`.kjb` (e.g. `pipelines\demo\`,
  `pipelines\CSCU\`) and `repositories\repositories.xml` is just the
  definition (`base_directory` -> `C:\PDI-Airflow\pipelines`). Repo paths
  changed accordingly: `/home/bi/*` -> `/demo/*`, `/home/cscu/etl/*` ->
  `/CSCU/*`. `deploy.ps1` stages `pipelines\` + `repositories\` + `.kettle\`
  (protected from the /MIR purge); `run-carte.ps1` syncs
  `repositories\repositories.xml` into `.kettle\` so one file is
  authoritative and the global `~/.kettle` stays untouched. The old ad-hoc
  `C:\PDI-Repo` is retired. Samples, generated DAGs, workshop DAGs,
  tests, LAB-SETUP and the capstone updated. 45 tests pass.

## PDI-AirFlow v1.18.1 - 2026-07-20

- **CSCU capstone DAGs moved under the mounted DAGs folder**
  (`workshop/dags/CSCU/`) so they load on the VM's Airflow after a
  `git pull` (they were reference-only under `workshop/CSCU/dags/`).
  Capstone dry-run findings: scheduler green, `cscu_core` reachable
  (5433), Carte loads the CSCU transformations from the staged
  repository; live DB reads still need the `cscu_core` credential
  defined in Carte (the lineage-shaped `.ktr` carry no password).

## PDI-AirFlow v1.18.0 - 2026-07-20

- **CSCU capstone workshop** (`workshop/CSCU/CSCU-CAPSTONE.md`) - the
  bring-it-all-together track on the Copper State Credit Union banking
  pipeline: migrate `cscu_daily_load.kjb` in the Studio, run it on Carte
  **single and clustered** under the (now-verified) scheduler, and trace
  a member's data from `cscu_core` source tables into the mart in
  **Marquez + PDC**. Reuses the same `cscu_core` DB / PDC as the
  PDC-Scenarios lab. Ships the generated reference DAGs
  (`workshop/CSCU/dags/`) and stages the CSCU transformations in the
  Carte file repository (`lab/carte/repository/home/cscu/etl/`) so they
  run live. Linked from README + WORKSHOP.

## PDI-AirFlow v1.17.4 - 2026-07-20

- **Scheduler verified end-to-end** on the Airflow 3.3 VM after the
  `execution_api_server_url` fix (v1.15.5): a scheduler-triggered
  `m01_carte_trans_basic` ran green — VM Airflow -> Windows Carte ->
  transformation -> Marquez `COMPLETED` (was `FAIL`).
- **Fix: non-ASCII (em-dash) in generated output.** `pdi2dag` source
  used U+2014 em-dashes in CLI warnings and the generated-DAG docstring,
  which rendered as `?`/mojibake on a Windows (cp1252) console and inside
  generated `.py` files. All `pdi2dag` + backend strings normalised to
  ASCII; generated DAGs are now clean ASCII.

## PDI-AirFlow v1.17.3 - 2026-07-20

Remaining production-roadmap items + a UI refinement.

- **File logging**: the Studio backend now writes a rotating log to
  `webapp/logs/studio.log` (1 MB x 5) alongside the console, logs startup
  and full tracebacks for unhandled errors - a diagnosable trail for the
  service install.
- **API hardening**: conservative security headers (`X-Content-Type-Options`,
  `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`) on every
  response, and `run.ps1` binds uvicorn to `127.0.0.1` explicitly (the
  Studio is a local tool, not a shared service). No CORS middleware =
  same-origin only.
- **Versioned releases**: `.github/workflows/release.yml` - on a `v*`
  tag, builds the UI, packages a deployable zip, and creates a GitHub
  Release with generated notes.
- **Subtler Status tiles** on Home: the values are smaller, lighter and
  muted (was a shouty 1.7rem/650), labels now quiet uppercase captions.
  Studio webapp -> 1.14.3.

## PDI-AirFlow v1.17.2 - 2026-07-20

- **CSCU samples re-themed to Copper State Credit Union** (financial
  services), from the mistaken student-enrollment theme, using the real
  `cscu_core` schema from `PDC-Scenarios` (members, accounts,
  transactions, branches, loans). New `samples/cscu/`:
  `extract_transactions.ktr` (posted txns -> mart staging),
  `build_member_mart.ktr` (member 360: members x branches x accounts x
  transactions), `import_ach_csv.ktr` (ACH CSV -> staging), and
  `cscu_daily_load.kjb` (extract -> build, mail-on-failure). Connections
  target the shared lab (`192.168.1.200:5433`, `cscu_core`, `pdc_user`).
  Old enrollment samples removed; `test_carte_enrichment.py` /
  `test_generator.py` and the LAB-SETUP convert example updated. 45 tests
  pass. Groundwork for the CSCU capstone (Phase C).

## PDI-AirFlow v1.17.1 - 2026-07-20

- **Secrets encrypted at rest (Windows DPAPI)** — completes the
  production-hardening batch (installer + CI + service + secrets). The
  Studio's `airflow_password`, `carte_password` and `pdc_password` are
  now stored in `settings.json` as `dpapi:<base64>`, encrypted with
  `CryptProtectData` (scoped to the current user + machine) and decrypted
  only in-process. New `pdi2dag/dpapi.py` (`protect`/`unprotect`, ctypes,
  no extra dependency) with a plaintext pass-through fallback off
  Windows. Verified end-to-end: the API still returns plaintext to the
  local UI while `settings.json` holds only ciphertext. 4 new tests
  (45 pass, 1 skipped off-Windows). Studio webapp -> 1.14.2.

## PDI-AirFlow v1.17.0 - 2026-07-20

Production-hardening pass (part 1 of 4 requested: installer + CI +
secrets + service; this ships the **installer, service and CI**).

- **One-script installer** `install.ps1`: prereq checks (64-bit Python
  3.10-3.12, Node, Docker) -> venv + `pdi2dag`/provider -> build the UI
  -> deploy a **self-contained** copy to `C:\PDI-Airflow` (its own venv,
  so it runs with **no Node**). `-Service` also registers auto-start.
  `uninstall.ps1` (with `-KeepData`) reverses it.
- **Auto-start service** `scripts/install-service.ps1`: registers the
  Studio as a scheduled task (starts at logon, restarts on failure) via
  the deployed venv + `run.ps1 -NoBuild` - fixes the "console dies"
  flakiness for unattended running.
- **CI** (GitHub Actions): pdi2dag tests (py 3.10/3.12), provider tests,
  UI build, and a `scripts/check_versions.py` consistency gate. README
  gains a CI badge.
- **`run.ps1 -NoBuild`** no longer needs Node - serves the shipped
  `dist` directly (errors clearly if no build is present).
- **Non-destructive re-deploy**: `deploy.ps1` now protects the install
  venv and user data (`DAGs\`, `repositories\`, `.kettle\`) from the
  `/MIR` purge, so re-installing refreshes code without wiping
  deployments.
- All `.ps1` files sanitised to ASCII (em-dashes broke parsing under
  Windows PowerShell 5.1). Launchers parse-checked.

## PDI-AirFlow v1.16.0 - 2026-07-20

- **Local Carte cluster** (workshop capstone, Phase B). New
  `run-carte-cluster.ps1` starts a master (:8081) + two slaves
  (:8082/:8083), each in its own window; slaves register with the master
  so a transformation carrying a cluster schema fans out across them.
  Configs in `lab/carte/cluster/` (`master.xml`, `slave1.xml`,
  `slave2.xml`); `deploy.ps1` stages them under `C:\PDI-Airflow\carte\cluster`.
  `KETTLE_HOME` is set to the install folder (global `~/.kettle`
  untouched). The seeded `pdi_cluster` connection now defaults to the
  master port `:8081` (`CLUSTER_PORT` in `.env`). Point `pdi_cluster` at
  the master (Studio picker / `pdi2dag --conn-id pdi_cluster`) to run
  clustered. Both launchers validated (PS parse + XML well-formed).

## PDI-AirFlow v1.15.5 - 2026-07-20

- **Real fix for scheduler-triggered tasks being SIGKILLed on the
  Airflow 3.3 VM** (supersedes the v1.15.1 misdiagnosis). The cause was
  **not** the OpenLineage fork deadlock — `execute_tasks_new_python_interpreter`
  is a no-op in Airflow 3's Task-SDK model. With separate
  `airflow-scheduler` / `airflow-apiserver` containers, the LocalExecutor
  task workers reach the **execution API** at the default `localhost:8080`,
  which inside the scheduler container has no api-server — so every task
  fails with `httpx.ConnectError: Connection refused` and is reaped at
  ~8s (while `airflow tasks test`, running standalone, works). Fixed by
  `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-apiserver:8080/execution/`
  in `docker-compose.yml`; the earlier `EXECUTE_TASKS_NEW_PYTHON_INTERPRETER`
  setting was removed. Redeploy the VM (`git pull && docker compose up -d`)
  to apply. Troubleshooting row updated.

## PDI-AirFlow v1.15.4 - 2026-07-20

- **Self-contained Carte setup in the deployed install.**
  `scripts/deploy.ps1` now stages the Carte artifacts under the install
  root (`C:\PDI-Airflow`): `carte\carte-config.xml`, the file-repository
  `repositories\` (seeded with `home\bi\hello_world.ktr` + a copy of
  `repositories.xml`), and `.kettle\repositories.xml` — all with
  `base_directory` rewritten to `<Dest>\repositories`.
- **`run-carte.ps1`** (new, at the repo root) launches Carte against
  that config, auto-locating PDI and setting `KETTLE_HOME` to the
  install folder so Carte reads its own `.kettle` and **never touches
  your global `~/.kettle`** (which would otherwise wipe existing Pentaho
  repository definitions). Falls back to `lab\carte\` + the global
  `.kettle` when run from the source repo. LAB-SETUP documents the
  turnkey path.

## PDI-AirFlow v1.15.3 - 2026-07-20

- **Fix: "Open the graph in Marquez" ignored the Settings override.**
  The link used the *Marquez UI URL* setting, which stays `localhost:3000`
  by default — so pointing only the *Marquez API URL* at a remote host
  (e.g. the VM) left the graph link opening a dead local Marquez. Now,
  when the UI URL is still localhost but the API URL is remote, the link
  **follows the API host** (keeping the UI port/scheme) — verified the
  sidebar link and the Lineage button both resolve to the VM's Marquez.
  An explicitly-set remote UI URL is still respected.
- **Favicon** added (`favicon.svg`, a PDI ⇄ Airflow mark in the app
  accent) and referenced from `index.html`. Studio webapp → 1.14.1.

## PDI-AirFlow v1.15.2 - 2026-07-20

Documentation + lab-content pass so the workshop runs clean end-to-end
(no webapp code change — Migration Studio stays 1.14.0).

- **`hello_world` transformation now ships in the repo**
  (`lab/carte/repository/home/bi/hello_world.ktr`) — workshop Module 1
  works out of the box. LAB-SETUP / `repositories.xml` seed it with
  `xcopy … C:\PDI-Repo\` instead of hand-authoring in Spoon.
- **LAB-SETUP.md Carte setup rewritten** with what a real run needs:
  the EE **license** step (`install_license.bat`), the correct PDI path
  (`C:\Pentaho\design-tools\data-integration`), and the actual
  **firewall rule** for the VM topology (`New-NetFirewallRule …
  -Profile Any` — `-Profile Private` silently fails on Public LANs),
  with VM-side `curl` verification.
- **WORKSHOP.md**: fixed the 9 module DAG links to `dags/workshop/…`
  after the three-folder reorg.
- **Doc consistency sweep** (audit): Marquez **host** API port shown as
  `:6001` (not the internal `:5000`) in the topology diagram, the
  Astronomer transport URL, and the Part-3 summary; "Airflow
  (standalone)" → the three-service reality; "Airflow 2.10" → "2.10.5"
  precision in VERSION/INSTALL/README.

## PDI-AirFlow v1.15.1 - 2026-07-20

- **Fix: scheduler/UI-triggered Carte tasks were SIGKILLed on the
  Airflow 3.3 VM.** The LocalExecutor forks a multi-threaded parent to
  run each task; on Python 3.12+/3.13 the OpenLineage listener's
  START-event emission deadlocks in the forked child, so tasks hung at
  `Pre Execute` and the supervisor reaped them with SIGKILL — while
  `airflow tasks test` (in-process, no fork) worked fine
  ([apache/airflow#47160](https://github.com/apache/airflow/issues/47160)).
  Set `AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER=true` in
  `docker-compose.yml` so each task runs in a fresh interpreter instead
  of forking, keeping OpenLineage on. The Windows lab
  (`docker-compose.win.yml`, Airflow 2.10.5 / Python 3.10) is
  unaffected and left as-is. End-to-end verified: Airflow → Windows
  Carte → transformation runs → START/COMPLETE lineage in Marquez.

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
