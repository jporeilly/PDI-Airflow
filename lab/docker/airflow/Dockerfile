# Lab Airflow 3.3 image with the Pentaho provider + the providers the
# workshop/lab needs baked in, so containers boot fast and never
# pip-install at runtime. Airflow runs in Linux here; Windows (the
# Studio / pdi2dag) connects over the REST API v2 (JWT).
#
# Build context is the repo root (see docker-compose.yml).
FROM apache/airflow:3.3.0

COPY --chown=airflow:root airflow-pentaho-provider /opt/pentaho-provider

# providers-standard: BashOperator/EmptyOperator/PythonOperator moved here
# providers-fab: FAB auth manager (admin/admin login + JWT for the API)
RUN pip install --no-cache-dir \
        "apache-airflow-providers-openlineage" \
        "apache-airflow-providers-standard" \
        "apache-airflow-providers-fab" \
        /opt/pentaho-provider
