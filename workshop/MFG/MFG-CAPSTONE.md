# Capstone - Cascade Precision Components on Airflow

The manufacturing capstone: migrate PDI pipelines in the Studio, run them on
Carte (single **and** clustered) under the scheduler, and trace the
lineage in **Marquez** and **Pentaho Data Catalog**.

Reuses the **same lab** as the PDC-Scenarios / Glossary work: the `cpc_mfg`
database on `192.168.1.200:5433`, lineage into the **same PDC**.

> **Status: scaffold.** The pipelines for this vertical are not built
> yet. Add them under `pipelines/MFG/` (repo paths `/MFG/...`),
> extracting from the `cpc_mfg` schema, then follow the same four-module arc
> as the CSCU capstone
> ([../CSCU/CSCU-CAPSTONE.md](../CSCU/CSCU-CAPSTONE.md)):
> migrate -> single Carte -> cluster -> lineage.

## Source (from PDC-Scenarios)

| | Value |
|---|---|
| Database | `cpc_mfg` (schema `cpc_mfg`, 11 tables) on `192.168.1.200:5433` |
| Read-only user | `pdc_user` / `catalog123!` |
| PDC | `https://pentaho.io` |

## To build this capstone

1. Load the scenario on the VM: `make load SCENARIO=MFG` (in
   `PDC-Scenarios/data_sources/lab`).
2. Author pipelines under `pipelines/MFG/` (Table Input from
   `cpc_mfg.<table>` -> a mart), modelled on `samples/cscu/*`.
3. Define the `cpc-core` connection in Carte/Spoon ->
   `192.168.1.200:5433 / cpc_mfg / pdc_user / catalog123!`.
4. Migrate in the Studio, deploy, run on Carte (single or `pdi_cluster`),
   and trace the lineage - exactly as the CSCU capstone documents.

*All Cascade Precision Components data is fictional and generated for training.*
