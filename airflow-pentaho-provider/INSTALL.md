# Installation

## Standard Airflow (2.7+ or 3.x)

Install the package on every component that parses or runs DAGs
(webserver/api-server, scheduler, triggerer and workers):

```bash
pip install airflow-provider-pentaho
```

For development installs from source:

```bash
pip install /path/to/airflow-pentaho-provider
```

## Astronomer (Astro Runtime)

Add to your Astro project's `requirements.txt`:

```
airflow-provider-pentaho
```

Then `astro dev restart` (local) or deploy as usual. To use the
deferrable Carte operators, make sure the triggerer is enabled (it is by
default on Astro Runtime).

## Airflow connection

Create a connection in **Admin -> Connections**:

- **Connection Id**: `pdi_default` (the operators' default)
- **Connection Type**: `Pentaho`
- **Host**: Carte hostname, including scheme, e.g. `https://carte.acme.com`
- **Port**: Carte port, e.g. `8081`
- **Login / Password**: PDI repository credentials
- **Extra**:

```json
{
    "rep": "Default",
    "carte_username": "cluster",
    "carte_password": "cluster",
    "pentaho_home": "/opt/pentaho",
    "verify_ssl": true,
    "timeout": 60
}
```

Notes:

- `rep` is the PDI repository name.
- `carte_username` / `carte_password` are Carte's own HTTP basic auth
  credentials (defaults are `cluster`/`cluster` on a stock Carte).
- `pentaho_home` is only required for `KitchenOperator`/`PanOperator`
  and must point to the PDI installation on the worker.
- `verify_ssl: false` disables TLS certificate verification for
  self-signed Carte certificates.
- `timeout` is the per-request timeout in seconds for Carte calls.

Or via environment variable:

```bash
export AIRFLOW_CONN_PDI_DEFAULT='{
  "conn_type": "pentaho",
  "host": "https://carte.acme.com",
  "port": 8081,
  "login": "repo_user",
  "password": "repo_pass",
  "extra": {"rep": "Default", "carte_username": "cluster", "carte_password": "cluster"}
}'
```

## Local PDI workers (Kitchen/Pan only)

`KitchenOperator` and `PanOperator` shell out to `kitchen.sh`/`pan.sh`
(`.bat` on Windows) on the worker itself, so those workers need a PDI
installation at `pentaho_home`. Consider routing these tasks to
dedicated workers with an Airflow queue (`queue="pdi"`).

## Running the tests

```bash
python -m venv .venv
.venv/Scripts/pip install -e .[dev]     # Windows
.venv/bin/pip install -e .[dev]         # Linux/macOS
.venv/Scripts/python -m pytest          # Windows
.venv/bin/python -m pytest              # Linux/macOS
```
