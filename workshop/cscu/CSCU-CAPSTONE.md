# Capstone - Copper State Credit Union on Airflow

The capstone ties the whole toolkit together on a realistic **Copper
State Credit Union (CSCU)** banking pipeline: migrate PDI jobs in the
Studio, run them on Carte (single **and** clustered) under the Airflow
scheduler, and see the lineage in **Marquez** and **Pentaho Data
Catalog**.

It reuses the **same `cscu_core` database** as the PDC-Scenarios /
Glossary lab (`192.168.1.200:5433`), so the PDI table lineage lands in
the **same PDC** the catalog work already uses - one member's data,
traced from source tables through the mart.

## The pipeline

`samples/cscu/` (also staged in the Carte repository at
`/CSCU/`):

| PDI file | Does |
|---|---|
| `extract_transactions.ktr` | posted `cscu_core.transactions` -> `mart staging.txn_stg` |
| `build_member_mart.ktr` | **member 360**: members x branches x accounts x transactions -> `mart.member_360` |
| `import_ach_csv.ktr` | ACH payments CSV -> `staging.ach_stg` |
| `cscu_daily_load.kjb` | `extract_transactions` -> `build_member_mart` (mail-on-failure) |

Generated DAGs live in [`workshop/dags/cscu/`](../dags/cscu/) — under the
mounted DAGs folder, so they load on the VM's Airflow after a `git pull`.

## Prerequisites

1. **Lab up**: Airflow (`:8088`) + Marquez (`:3000`/`:6001`) on the VM,
   PDC at `https://pentaho.io`. The scheduler must be on the fixed compose
   (`execution_api_server_url`, v1.15.5+) - verified when a plain
   `m01_carte_trans_basic` run goes green.
2. **CSCU data** (only for live execution, Modules 2-4): load `cscu_core`
   into the shared lab with the PDC-Scenarios kit -
   `cd PDC-Scenarios/data_sources/lab && make load SCENARIO=CSCU`.
3. **Carte** running on Windows (`.\run-carte.ps1`), with a **`cscu-core`
   database connection** to `192.168.1.200:5433 / cscu_core / pdc_user`
   (read-only) and a writable **`cscu-mart`** target. Define these in
   Spoon (Tools -> Repository connected -> Database connections) so Carte
   resolves the connection names the `.ktr` files reference. Module 1
   (migration + structural lineage) needs neither the DB nor Carte.

---

## Module 1 - Migrate in the Studio (10 min)

No database or Carte needed - this is the migration itself.

1. Studio -> **Load** -> drop `samples/cscu/cscu_daily_load.kjb`. The
   Studio auto-pulls the two transformations it calls.
2. **Configure**: schedule `0 2 * * *`, pick the **Carte connection**
   `pdi_default` (or `pdi_cluster` for the clustered run later).
3. **Preview** the generated DAG: the job's two `TRANS` entries become
   `CarteTransOperator` tasks wired `Extract_Transactions >>
   Build_Member_Mart`; the `MAIL` entry surfaces as a migration
   **warning** (no Airflow equivalent - port to an `EmailOperator`).
4. **Deploy** to `deploy-target`. On the VM the scheduler parses it
   within ~30s.

**Lineage without running anything:** Studio -> **Lineage** publishes the
PDI *structure* to PDC - the `cscu_core.transactions -> staging.txn_stg`
and `members/accounts/transactions -> mart.member_360` table lineage,
built from the `.ktr` SQL. Open PDC and follow a member's data from the
core tables into the mart.

## Module 2 - Run on a single Carte (15 min)

*(needs `cscu_core` loaded + Carte with the DB connections)*

1. Start Carte: `.\run-carte.ps1` (serves `/CSCU/*` from the
   file repository).
2. Unpause `cscu_daily_load` in Airflow and **Trigger**.
3. Watch the Graph view: `Extract_Transactions` runs on Carte (reads
   `cscu_core.transactions`, writes `staging.txn_stg`), then
   `Build_Member_Mart` builds `mart.member_360`. The Carte step metrics
   (rows read/written) stream into the Airflow task log.

## Module 3 - Run clustered (20 min)

Contrast single-server with a **Carte cluster**.

1. Stop the single Carte; start the cluster: `.\run-carte-cluster.ps1`
   (master `:8081` + slaves `:8082`/`:8083`). Confirm registration at
   `http://localhost:8081/kettle/getSlaves/`.
2. In the Studio's **Configure** page, set the **Carte connection** to
   `pdi_cluster` (seeded in Airflow, pointing at the master) and
   redeploy - or `pdi2dag convert build_member_mart.ktr --conn-id
   pdi_cluster`.
3. Assign a **cluster schema** to `build_member_mart` in Spoon (the
   member-360 aggregation is the step that benefits) so the master fans
   the work across the slaves.
4. Trigger and watch: the same DAG, now executing clustered. Airflow
   orchestration is identical - only the Carte run configuration changed.

## Module 4 - Full lineage (15 min)

1. **Marquez** (`:3000`, namespace `pdi`): the `cscu_daily_load` job with
   `Extract_Transactions` / `Build_Member_Mart` runs, states and
   durations - the *orchestration* view.
2. **PDC**: the *table* lineage - `cscu_core.members`,
   `cscu_core.accounts`, `cscu_core.transactions` feeding
   `cscu_mart.mart.member_360`, with real row counts when Carte metrics
   are attached (`pdi2dag lineage ... --carte-status`). This is the same
   PDC that catalogs the CSCU sources in the Glossary scenario, so the
   PDI pipelines slot straight into the existing catalog.

**Division of responsibility:** Carte owns runtime row metrics, Airflow
owns orchestration/retries/schedule, Marquez owns run history, PDC owns
the governed table lineage. The capstone shows all four working on one
credit-union pipeline.

---

*All Copper State Credit Union data is fictional and generated for
training.*
