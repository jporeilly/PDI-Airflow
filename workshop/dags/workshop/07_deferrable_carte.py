# -*- coding: utf-8 -*-
"""Module 7: deferrable Carte execution.

With deferrable=True the task releases its worker slot while the
job/transformation runs on Carte; a trigger polls status from the
triggerer. Compare Browse -> Task Instances while this runs against
the non-deferrable module 2 DAG: state shows 'deferred' instead of
occupying a worker.
"""

import pendulum

from airflow import DAG
from airflow_pentaho.operators.carte import CarteJobOperator

with DAG(
    dag_id='m07_deferrable_carte',
    description='Workshop module 7: deferrable mode',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    tags=['workshop', 'pdi', 'deferrable'],
) as dag:

    long_running = CarteJobOperator(
        task_id='long_running_job',
        job='/demo/long_running_job',
        params={'date': '{{ ds }}'},
        deferrable=True,
        poll_interval=15)
