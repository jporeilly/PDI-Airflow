# Version

**Current version: 1.23.0** (2026-07-21)

Components:

| Component | Version |
|---|---|
| PDI-AirFlow (umbrella / pdi2dag) | 1.23.0 |
| Migration Studio webapp | 1.15.0 |
| airflow-pentaho-provider (bundled) | 2.0.0 |
| Targets | **Airflow 2.10.5 (Windows lab) / 3.3 (Ubuntu VM)**, Marquez 0.50, Pentaho Data Catalog |

Version sources of truth: `pyproject.toml` + `pdi2dag/__init__.py`
(pdi2dag), and the provider's own `pyproject.toml` /
`airflow_pentaho/__init__.py`. Update CHANGELOG.md on every release.
