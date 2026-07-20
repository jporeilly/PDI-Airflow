# Changelog

## airflow-provider-pentaho v2.0.0 - 2026-07-18

Modernized rework of airflow-pentaho-plugin 1.2.3 as an Airflow
provider package. Import paths (`airflow_pentaho.*`) are unchanged.

### Added

- Airflow provider package metadata (`apache_airflow_provider` entry
  point): registers the `Pentaho` connection type with a customized
  connection form.
- Deferrable mode for `CarteJobOperator` and `CarteTransOperator`
  (`deferrable=True`): polling moves to the triggerer via new
  `CarteJobTrigger` / `CarteTransTrigger`, releasing the worker slot
  while the job/transformation runs on Carte.
- `poll_interval` parameter on Carte operators (was hardcoded to 5s).
- `on_kill` on Carte operators now calls Carte `stopJob` / `stopTrans`,
  so killing an Airflow task stops the remote execution.
- Request `timeout` and `verify_ssl` connection extras for Carte calls.
- Windows support for Kitchen/Pan command execution (`cmd /c` + `.bat`).
- `pyproject.toml` packaging (PEP 621); Python 3.9–3.12.

### Changed

- **Security**: Carte `executeJob` / `executeTrans` are now called with
  POST (form body) instead of GET, keeping repository credentials out
  of URLs and Carte access logs.
- Operators always return the last log line as XCom `return_value` and
  always push `err_count`; the `xcom_push` argument is deprecated
  (a no-op that emits a `DeprecationWarning`).
- `run_trans` now returns the parsed Carte response.
- CDATA log parsing fixed for log payloads containing `]` characters;
  undecodable log payloads no longer crash the operator.
- Non-XML Carte error responses (e.g. a proxy's 502 page) now raise a
  readable error instead of an XML parse failure.
- Unknown Kitchen/Pan exit codes no longer raise `KeyError`.

### Removed

- Airflow 1.10.x support and all `airflow.__version__` runtime shims.
  Requires Apache Airflow >= 2.7 (Airflow 3.x supported).
- The legacy `AirflowPlugin` registration (`airflow_pentaho.plugin`)
  and the deprecated top-level operator modules
  (`CarteJobOperator.py`, `KitchenOperator.py`, ...). Import from
  `airflow_pentaho.operators.carte` / `.kettle` instead.

## Previous releases

See the original project's changelog:
https://github.com/damavis/airflow-pentaho-plugin
