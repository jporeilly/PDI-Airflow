# -*- coding: utf-8 -*-
"""Module 12 warm-up: a pure-Airflow pipeline to verify Marquez lineage.

Runs without PDI/Carte, so it can be triggered immediately after the
lab stack is up to confirm OpenLineage events are flowing into Marquez.
"""

from datetime import timedelta

import pendulum

from airflow import DAG
try:
    from airflow.providers.standard.operators.bash import BashOperator  # Airflow 3
except ImportError:
    from airflow.operators.bash import BashOperator  # Airflow 2

with DAG(
    dag_id='lineage_demo',
    description='Pure-Airflow pipeline to verify Marquez wiring',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=['workshop', 'lineage'],
) as dag:

    extract = BashOperator(
        task_id='extract',
        bash_command='echo "extracting source rows" && sleep 2')

    transform = BashOperator(
        task_id='transform',
        bash_command='echo "transforming rows" && sleep 2')

    load = BashOperator(
        task_id='load',
        bash_command='echo "loading warehouse" && sleep 2')

    extract >> transform >> load
