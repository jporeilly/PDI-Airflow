# -*- coding: utf-8 -*-
"""Module 5: a multi-transformation pipeline with fan-out/fan-in.

Mirrors a typical PDI job graph in Airflow: two extracts run in
parallel, the load waits for both, reporting runs last, and an alert
task fires only when the load fails (PDI 'failure hop' equivalent).
"""

import pendulum

from airflow import DAG
try:
    from airflow.providers.standard.operators.bash import BashOperator  # Airflow 3
except ImportError:
    from airflow.operators.bash import BashOperator  # Airflow 2
from airflow_pentaho.operators.carte import CarteJobOperator
from airflow_pentaho.operators.carte import CarteTransOperator

with DAG(
    dag_id='m05_pipeline_dependencies',
    description='Workshop module 5: dependencies and trigger rules',
    schedule='0 5 * * 1-5',        # weekdays at 05:00
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    tags=['workshop', 'pdi'],
) as dag:

    extract_sales = CarteTransOperator(
        task_id='extract_sales',
        trans='/demo/extract_sales',
        params={'date': '{{ ds }}'})

    extract_customers = CarteTransOperator(
        task_id='extract_customers',
        trans='/demo/extract_customers',
        params={'date': '{{ ds }}'})

    load_warehouse = CarteTransOperator(
        task_id='load_warehouse',
        trans='/demo/load_warehouse',
        params={'date': '{{ ds }}'})

    publish_reports = CarteJobOperator(
        task_id='publish_reports',
        job='/demo/reporting/publish_reports',
        params={'date': '{{ ds }}'})

    # PDI failure-hop equivalent: runs only if an upstream task failed
    alert_on_failure = BashOperator(
        task_id='alert_on_failure',
        bash_command='echo "load failed — alerting" ',
        trigger_rule='one_failed')

    [extract_sales, extract_customers] >> load_warehouse
    load_warehouse >> publish_reports
    load_warehouse >> alert_on_failure
