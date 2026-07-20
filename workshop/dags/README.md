# Workshop DAGs

Airflow mounts **this folder** as its DAGs directory and scans it
recursively, so every DAG under the three subfolders below is loaded.

| Folder | Contents | Committed? |
|---|---|---|
| `workshop/` | The guided curriculum — one DAG per module in [WORKSHOP.md](../WORKSHOP.md) (`00_lineage_demo`, `01`–`10`). | yes |
| `examples/` | Standalone reference DAGs not tied to a module (`mini_job`). | yes |
| `deploy-target/` | Empty landing zone where the Studio / `pdi2dag` deploy the DAGs you generate (Module 11). Generated `*.py` here are git-ignored. | `.gitkeep` only |

**Studio setting:** point *Settings → Deployment → DAGs folder* at
`…/workshop/dags/deploy-target` so your generated DAGs land in
`deploy-target/`, cleanly separated from the pre-built examples.

**Lab mount:** `DAGS_DIR` (in `lab/docker/.env`) points at this parent
`workshop/dags` folder, so `workshop/`, `examples/` and `deploy-target/`
all load in Airflow.
