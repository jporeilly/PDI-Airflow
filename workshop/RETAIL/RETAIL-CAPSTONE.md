# Capstone - Canyon Trail Outfitters on Airflow

The retail capstone: migrate PDI pipelines in the Studio, run them on
Carte (single **and** clustered) under the scheduler, and trace the
lineage in **Marquez** and **Pentaho Data Catalog**.

Reuses the **same lab** as the PDC-Scenarios / Glossary work: the `cto_retail`
database on `192.168.1.200:5433`, lineage into the **same PDC**.

> **Status: scaffold.** The pipelines for this vertical are not built
> yet. Add them under `pipelines/RETAIL/` (repo paths `/RETAIL/...`),
> extracting from the `cto_retail` schema, then follow the same four-module arc
> as the CSCU capstone
> ([../CSCU/CSCU-CAPSTONE.md](../CSCU/CSCU-CAPSTONE.md)):
> migrate -> single Carte -> cluster -> lineage.

## Source (from PDC-Scenarios)

| | Value |
|---|---|
| Database | `cto_retail` (schema `cto_retail`, 11 tables) on `192.168.1.200:5433` |
| Read-only user | `pdc_user` / `catalog123!` |
| PDC | `https://pentaho.io` |

## To build this capstone

1. Load the scenario on the VM: `make load SCENARIO=RETAIL` (in
   `PDC-Scenarios/data_sources/lab`).
2. Author pipelines under `pipelines/RETAIL/` (Table Input from
   `cto_retail.<table>` -> a mart), modelled on `samples/cscu/*`.
3. Define the `cto-core` connection in Carte/Spoon ->
   `192.168.1.200:5433 / cto_retail / pdc_user / catalog123!`.
4. Migrate in the Studio, deploy, run on Carte (single or `pdi_cluster`),
   and trace the lineage - exactly as the CSCU capstone documents.

*All Canyon Trail Outfitters data is fictional and generated for training.*
