# Course companion — updating "How-To: Apache Airflow" (2023)

The self-paced course at `P:\Self-paced Courses\How-To--Apache-AirFlow`
(recorded March 2023, Airflow 2.0 + PDI 9.1) pioneered the dockerized
Airflow-orchestrates-Carte pattern this project industrializes. This
document maps every course technique to its modern replacement, and
[dags/](dags/) contains modernized versions of the course DAGs that run
on the current lab. The course masters on P: are untouched — apply
these updates when re-recording.

```mermaid
flowchart LR
    subgraph course["Course (2023)"]
        A[BashOperator + curl] --> B[execute-carte.sh<br/>poll loop]
        B --> C[Carte REST API]
    end
    subgraph now["This project (2026)"]
        D[CarteJobOperator /<br/>CarteTransOperator] --> E[airflow-provider-pentaho<br/>hook + trigger]
        E --> C2[Carte REST API]
    end
    course -. "same API,<br/>now first-class operators" .-> now
```

## What carries forward unchanged

| Course asset | Status |
|---|---|
| Dockerized PDI/Carte image (`setup-pentaho`) | **Adopted** — modernized copy at [../lab/docker/carte/](../lab/docker/carte/) (Java 8→11, PDI 9.1→9.4, master-only entrypoint, repo renamed `test-repo`→`Default`). Run with `docker compose --profile carte up -d --build`. |
| File-repository pattern (`repositories.xml` + mounted ktrs) | **Adopted** — the lab mounts [../lab/docker/carte/repository/](../lab/docker/carte/repository/), seeded with the course's helloworld/process1 files plus `/home/bi` clones for the workshop. |
| Separate-containers architecture (Airflow ⇄ Carte) | **Adopted** — same topology in [../lab/docker/docker-compose.yml](../lab/docker/docker-compose.yml). |
| Course ktr/kjb samples | **Reused** as runnable lab content. |

## What each course technique becomes

| Course (2023) | Now | Why |
|---|---|---|
| `BashOperator` + `curl` to `executeTrans`/`executeJob` | `CarteTransOperator` / `CarteJobOperator` | Status polling, log streaming into the task log, error detection, `stopJob` on kill — all built in. No shell escaping of URLs. |
| Credentials in `PDI_CONN_STR` env URL (`http://user:pass@host`) | Airflow **Connection** `pdi_default` (+ POST body credentials) | Secrets out of env/URLs and access logs; per-deployment config in the Airflow UI. |
| `execute-carte.sh` polling loop (webserver-side bash) | Operator poll loop, or **deferrable mode** (`deferrable=True`) | The shell loop held a worker slot and only fetched job logs. Deferrable operators release the slot; logs stream for both jobs and transformations. |
| "async trigger" DAG (fire-and-forget curl) | `deferrable=True` | Fire-and-forget lost the outcome — a failed transformation looked green in Airflow. Deferrable waits without occupying a worker. |
| `airflow.operators.bash_operator`, `DummyOperator`, `days_ago()`, `schedule_interval` | `airflow.operators.bash`, `EmptyOperator`, `pendulum.datetime`, `schedule` | Removed/renamed in Airflow 2.4+ / 3.x. |
| CeleryExecutor + Redis (3 Airflow containers) | `standalone` LocalExecutor for the lab | One container, includes the triggerer (needed for deferrable). Celery remains the right call for real multi-worker deployments — keep that slide, flag it as production topology. |
| Airflow 2.0 image | 2.10 (Astro Runtime / 3.x ready) | Datasets, dynamic task mapping, deferrable operators, Params UI all post-2.0. |
| Manual DAG authoring per PDI job | **pdi2dag** / Migration Studio | Entries→tasks, hops→dependencies, parameters mapped automatically. |
| (not covered) | OpenLineage → **Marquez** | New module: every run visible as lineage. |

## Suggested new course outline (delta)

1. Keep: intro, why-orchestrate, containers module, Carte concepts.
2. Replace Method 1/Method 2 (curl / execute-carte.sh) with the
   provider operators — demo `airflow-provider-pentaho` install and the
   `pdi_default` connection.
3. New lessons: deferrable mode (show the freed worker slot),
   Datasets, dynamic task mapping, pdi2dag migration (live demo with
   the Migration Studio web app), lineage in Marquez.
4. The workshop in [../workshop/WORKSHOP.md](../workshop/WORKSHOP.md)
   is the hands-on script for the new recording — one module per video.

## Modernized course DAGs ([dags/](dags/))

| Course file | Modernized | Notes |
|---|---|---|
| `sync-trigger.py` | `sync_trigger.py` | Two sequential `CarteTransOperator`s (task1 → task2 from process1). "Sync" is simply how the operator works. |
| `async-trigger.py` | `async_trigger.py` | Same pipeline with `deferrable=True` — the honest version of async: doesn't hold a worker, but still reports the real outcome. |
| `load-testing.py` | `load_testing.py` | Parallel fan-out of 3 jobs + 1 transformation through the provider. |
| `hello-world.py` | (module 1 of the workshop covers this) | `m01_carte_trans_basic` is the equivalent. |

Drop the modernized DAGs into the lab's dags folder (or copy them to
`workshop/dags/`) to run them; they use the course-derived repository
content mounted into the containerized Carte.
