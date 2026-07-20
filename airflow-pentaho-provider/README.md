# Pentaho Airflow Provider

A Pentaho Data Integration (PDI) provider for Apache Airflow and
Astronomer. Runs PDI **Jobs** and **Transformations** through **Carte**
servers, or locally through **Kitchen** and **Pan**, and orchestrates the
dependencies between them.

This is a modernized, provider-package rework of the original
[airflow-pentaho-plugin](https://github.com/damavis/airflow-pentaho-plugin)
(Apache 2.0, Aneior Studio SL / Damavis). Import paths are unchanged
(`airflow_pentaho.*`), so existing DAGs keep working.

## Highlights over the 1.x plugin

- **Airflow 2.7+ and Airflow 3.x** support (Astro Runtime compatible).
  No more version shims for Airflow 1.10.
- **Provider package**: registers a `Pentaho` connection type with a
  proper connection form — no plugin mechanism involved.
- **Deferrable Carte operators** (`deferrable=True`): the worker slot is
  released while the job/transformation runs on Carte; polling happens
  on the triggerer. Recommended on Astronomer.
- **Credentials out of URLs**: Carte calls now POST the repository
  user/password in the request body instead of the query string, so
  they no longer land in Carte access logs.
- **Task kill stops the remote work**: killing an Airflow task now calls
  Carte `stopJob`/`stopTrans` instead of leaving it running.
- **Configurable polling** (`poll_interval`), request timeouts and
  TLS verification (`verify_ssl` in the connection extra).

## Requirements

1. Apache Airflow >= 2.7 (or Astro Runtime with Airflow 2.7+/3.x).
2. For Carte operators: one or more running Carte servers.
3. For Kitchen/Pan operators: a PDI installation on the Airflow
   worker(s) that run those tasks (see
   [Queues](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html)).

## Installation

```bash
pip install airflow-provider-pentaho
```

On Astronomer, add it to your project's `requirements.txt`.
See [INSTALL.md](INSTALL.md) for full setup, including the Airflow
connection.

## Connection

Create a connection of type **Pentaho** (default id: `pdi_default`):

| Field    | Value                                          |
|----------|------------------------------------------------|
| Host     | Carte host, including `http://` or `https://`  |
| Port     | Carte port                                     |
| Login    | PDI repository username                        |
| Password | PDI repository password                        |
| Extra    | JSON, see below                                |

```json
{
    "rep": "Default",
    "carte_username": "cluster",
    "carte_password": "cluster",
    "pentaho_home": "/opt/pentaho",
    "verify_ssl": true
}
```

`pentaho_home` is only needed for Kitchen/Pan. `carte_username` /
`carte_password` are only needed for Carte.

## Usage

### CarteJobOperator / CarteTransOperator

```python
from airflow_pentaho.operators.carte import CarteJobOperator
from airflow_pentaho.operators.carte import CarteTransOperator

avg_spent = CarteJobOperator(
    task_id="average_spent",
    job="/home/bi/average_spent",
    params={"date": "{{ ds }}"},
    deferrable=True,      # release the worker slot while the job runs
    poll_interval=30)

enrich = CarteTransOperator(
    task_id="enrich_customer_data",
    trans="/home/bi/enrich_customer_data",
    params={"date": "{{ ds }}"})
```

### KitchenOperator / PanOperator (local PDI)

```python
from airflow_pentaho.operators.kettle import KitchenOperator
from airflow_pentaho.operators.kettle import PanOperator

avg_spent = KitchenOperator(
    task_id="average_spent",
    queue="pdi",
    directory="/home/bi",
    job="average_spent",
    params={"date": "{{ ds }}"})

clean_input = PanOperator(
    task_id="cleanup",
    queue="pdi",
    directory="/home/bi",
    trans="clean_somedata",
    params={"file": "/tmp/input_data/{{ ds }}/sells.csv"})
```

### XCom

Every operator returns the last log line as its XCom `return_value` and
pushes an `err_count` key with the number of log lines containing
"error". The old `xcom_push=True` argument is deprecated and no longer
needed.

See [sample_dags/](sample_dags/) for complete examples, including a
deferrable flow.

## Development

```bash
python -m venv .venv
.venv/Scripts/pip install -e .[dev]     # Windows
.venv/bin/pip install -e .[dev]         # Linux/macOS
pytest
```

## License

Apache License 2.0. Contains code derived from
airflow-pentaho-plugin, Copyright 2020 Aneior Studio, SL.
