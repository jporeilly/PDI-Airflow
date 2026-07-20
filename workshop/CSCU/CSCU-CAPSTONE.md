# Capstone - Copper State Credit Union on Airflow

The capstone ties the whole toolkit together on a realistic **Copper
State Credit Union (CSCU)** banking pipeline, following the real
migration workflow: **build the pipeline in PDI (Spoon) -> migrate it in
the Studio -> run it on Carte (single and clustered) under the Airflow
scheduler -> trace the lineage in Marquez and Pentaho Data Catalog.**

It reuses the **same `cscu_core` database** as the PDC-Scenarios /
Glossary lab (`192.168.1.200:5433`), so the PDI table lineage lands in
the **same PDC** the catalog work already uses.

## Two kinds of `.ktr` here

- **Shipped blueprints** — `samples/cscu/*.ktr` (also staged at
  `/CSCU/` in the Carte repository). These are **minimal
  migration-input examples**: just the connection, SQL and target, enough
  for `pdi2dag` to convert them and emit **structural lineage**. They do
  **not** render in Spoon and are **not executable** on Carte - they show
  the *shape* of the pipeline, not a runnable one.

  | Blueprint | Shape |
  |---|---|
  | `extract_transactions.ktr` | `cscu_core.transactions` -> `staging.txn_stg` |
  | `build_member_mart.ktr` | members x branches x accounts x transactions -> `mart.member_360` |
  | `import_ach_csv.ktr` | ACH CSV (local) -> `staging.ach_stg` |
  | `import_ach_minio.ktr` | ACH from MinIO `s3://cscu-documents` -> `staging.ach_stg` |
  | `cscu_daily_load.kjb` | `extract_transactions` -> `build_member_mart` |

- **What you run live** — a transformation **you build in Spoon** (below).
  Spoon writes a complete, executable `.ktr`; that's what actually runs on
  Carte and what a real migration starts from.

Generated reference DAGs live in [`workshop/dags/CSCU/`](../dags/CSCU/).

## Prerequisites

1. **Lab up**: Airflow (`:8088`) + Marquez (`:3000`/`:6001`) on the VM,
   PDC at `https://pentaho.io`, scheduler on the fixed compose
   (`execution_api_server_url`, v1.15.5+).
2. **CSCU data** (for live execution): `cd PDC-Scenarios/data_sources/lab
   && make load SCENARIO=CSCU` loads the read-only `cscu_core`.
3. **Carte** from the install (`cd C:\PDI-Airflow ; .\run-carte.ps1`),
   with a **`cscu-core`** database connection to
   `192.168.1.200:5433 / cscu_core / pdc_user / catalog123!` defined as a
   **shared** connection in Spoon (so every transformation resolves it).

The *migration + structural-lineage* part (Module 1) needs none of the DB
or Carte - it works off the shipped blueprints.

---

## Module 0 - Build the pipeline in PDI (Spoon)

The authentic starting point: author the transformation in Spoon, which
produces a real, executable, migratable `.ktr`.

1. Connect Spoon to the **`Default`** repository (`C:\PDI-Airflow\pipelines`).
2. New transformation. Drag a **Table Input** (connection `cscu-core`):
   ```sql
   SELECT txn_id, acct_id, txn_dt, post_dt, txn_amt, merch_nm, mcc_cd
   FROM cscu_core.transactions
   WHERE post_dt >= '2026-06-01'
   ```
3. Drag a **Write to Log** (log the fields) - a read-only demo needs no
   writable mart. *(For the full member-360 mart, add a `Table Output` to
   a writable `cscu-mart` target instead.)*
4. Hop Table Input -> Write to Log; **Save as** `/CSCU/txn_report`.

You now have `/CSCU/txn_report` - a genuine transformation that reads real
credit-union data and runs on Carte.

---

## Module 1 - Migrate in the Studio (10 min)

No database or Carte needed - this is the migration itself, run on the
shipped **blueprint** (`cscu_daily_load.kjb` + its two transformations) to
show conversion and structural lineage.

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

## Module 2 - Migrate and run it on a single Carte (15 min)

Now the real thing: migrate the **`txn_report`** you built in Module 0
and run it live.

*(needs `cscu_core` loaded + Carte running + `/CSCU/txn_report`)*

1. **Migrate** it: Studio -> **Load** ->
   `C:\PDI-Airflow\pipelines\CSCU\txn_report.ktr` -> **Configure**
   (`pdi_default`) -> **Deploy**. (Or `pdi2dag migrate
   ...\txn_report.ktr --dags-folder ...\deploy-target ...`.)
2. **Carte**: `cd C:\PDI-Airflow ; .\run-carte.ps1` (serves `/CSCU/*`).
3. **Trigger** the `txn_report` DAG. It runs on Carte, reads
   `cscu_core.transactions`, and streams the rows into the Airflow task
   log; the input lineage `cscu_core.cscu_core.transactions` shows in
   Marquez/PDC.

> **The full mart pipeline** (`cscu_daily_load`: extract -> member-360
> mart) is the advanced version - build both transformations in Spoon
> with a `Table Output` to a **writable `cscu-mart`** database (the
> PDC-Scenarios `cscu_core` is read-only), then migrate the `.kjb`.

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

## Module 5 - Object-store ingestion from MinIO (15 min)

The same `cscu-documents` MinIO bucket that PDC/Glossary catalogs for
*unstructured* documents can also be a PDI **source**. `import_ach_minio.ktr`
reads the ACH export from `s3://cscu-documents/ach_payments_2026.csv`
instead of a local file - so the PDI pipeline and the document catalog
share one bucket.

1. **Create a VFS connection** in Spoon (New VFS Connection ->
   Amazon S3/Minio/HCP -> S3 Connection Type `Minio/HCP`):

   | Field | Value |
   |---|---|
   | Connection Name | `cscu-minio` |
   | Access Key | `cscu_minio_user` |
   | Secret Key | `minio_secret_123!` |
   | Endpoint | `http://192.168.1.200:9000` |
   | **Signature Version** | **`AWSS3V4SignerType`** |
   | PathStyle Access | ✅ (MinIO requires path-style) |
   | Default S3 Connection | ✅ (so plain `s3://…` URLs route through this endpoint) |
   | Root Folder Path | `/` |

   > **Gotcha:** the Signature Version field wants the AWS SDK **signer
   > type** `AWSS3V4SignerType` - **not** the algorithm string
   > `AWS4-HMAC-SHA256`, which fails with
   > `IllegalArgumentException: unknown signer type`. With *Default S3
   > Connection* checked, the `s3://cscu-documents/…` path in
   > `import_ach_minio.ktr` works unchanged and the lineage stays clean.

2. **Run** `import_ach_minio` on Carte (single or clustered) via its DAG.
3. **Lineage**: the input dataset is `s3://cscu-documents/ach_payments_2026.csv`
   (object-store scheme + bucket kept as the namespace), feeding
   `cscu_mart.staging.ach_stg`. In Marquez/PDC the PDI job now traces
   from the **MinIO object** into the mart - the object-store half of the
   catalog, produced by PDI rather than a document scan.

**Structured vs unstructured:** PDC/Glossary scans `cscu-documents` for
document structure and PII; this module shows PDI *consuming* an object
from the same bucket and emitting S3 lineage. Two lenses on one store.

---

*All Copper State Credit Union data is fictional and generated for
training.*
