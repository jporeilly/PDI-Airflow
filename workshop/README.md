# Workshops

The general **[WORKSHOP.md](WORKSHOP.md)** (12 modules) teaches every PDI
scheduling scenario on Airflow using the `demo` pipelines. The
**scenario capstones** then apply the whole toolkit to a realistic
business vertical, each reusing its own PDC-Scenarios database and
landing lineage in the same PDC.

| Scenario | Vertical | Database | Capstone | Pipelines |
|---|---|---|---|---|
| **CSCU** | Copper State Credit Union (financial) | `cscu_core` | [CSCU/CSCU-CAPSTONE.md](CSCU/CSCU-CAPSTONE.md) | ✅ built (`pipelines/CSCU/`) |
| **HEALTH** | Lakeshore Health Partners (healthcare) | `lhp_clinical` | [HEALTH/HEALTH-CAPSTONE.md](HEALTH/HEALTH-CAPSTONE.md) | scaffold |
| **MFG** | Cascade Precision Components (manufacturing) | `cpc_mfg` | [MFG/MFG-CAPSTONE.md](MFG/MFG-CAPSTONE.md) | scaffold |
| **RETAIL** | Canyon Trail Outfitters (retail) | `cto_retail` | [RETAIL/RETAIL-CAPSTONE.md](RETAIL/RETAIL-CAPSTONE.md) | scaffold |

## Structure

Each scenario is self-contained and mirrors the PDC-Scenarios layout:

```
lab/carte/pipelines/<SCENARIO>/   the .ktr/.kjb (repo paths /<SCENARIO>/...)
workshop/<SCENARIO>/              the capstone guide
workshop/dags/<SCENARIO>/         the generated reference DAGs (mounted -> VM)
```

Adding a vertical = drop a `pipelines/<SCENARIO>/` folder (the Carte
repository `base_directory` is `pipelines\`, so any subfolder is a repo
path with zero config) and follow the CSCU capstone as the template.
