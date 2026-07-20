# Lab Airflow 2.10 image (Windows / Docker Desktop deployment).
# Airflow 2.10 keeps BashOperator/EmptyOperator/PythonOperator in core
# and uses basic-auth REST API v1, so only the OpenLineage + Pentaho
# providers are baked in. See docker-compose.win.yml.
FROM apache/airflow:2.10.5-python3.10

COPY --chown=airflow:root airflow-pentaho-provider /opt/pentaho-provider

RUN pip install --no-cache-dir \
        "apache-airflow-providers-openlineage>=1.11,<2" \
        /opt/pentaho-provider
