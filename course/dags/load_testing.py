# -*- coding: utf-8 -*-
"""Modernized course DAG: load-testing.py (2023) -> provider operators.

Parallel fan-out against Carte, exactly as the course demonstrated —
but each task now reports the true PDI outcome and can be retried or
stopped individually.
"""

import pendulum

from airflow import DAG
from airflow_pentaho.operators.carte import CarteJobOperator
from airflow_pentaho.operators.carte import CarteTransOperator

with DAG(
    dag_id='load_testing',
    description='Course DAG modernized: parallel Carte load test',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    tags=['course', 'pdi'],
) as dag:

    jobs = [
        CarteJobOperator(
            task_id='Trigger_Job{}'.format(i),
            job='/helloworld/helloworld-job')
        for i in (1, 2, 3)
    ]

    trans = CarteTransOperator(
        task_id='Trigger_Transformation',
        trans='/helloworld/helloworld-trans')

    # All four run in parallel; add pools/queues to shape the load.
