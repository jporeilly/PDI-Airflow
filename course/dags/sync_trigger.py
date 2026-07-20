# -*- coding: utf-8 -*-
"""Modernized course DAG: sync-trigger.py (2023) -> provider operators.

The 2023 version used BashOperator + curl + execute-carte.sh to poll
Carte. The provider operator does all of that natively: submit, poll,
stream the PDI log into the task log, fail on PDI errors.
"""

import pendulum

from airflow import DAG
from airflow_pentaho.operators.carte import CarteTransOperator

with DAG(
    dag_id='sync_trigger',
    description='Course DAG modernized: sequential Carte transformations',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    tags=['course', 'pdi'],
) as dag:

    task1 = CarteTransOperator(
        task_id='Task_1',
        trans='/process1/task1')

    task2 = CarteTransOperator(
        task_id='Task_2',
        trans='/process1/task2')

    # Start/Stop DummyOperators from the course are unnecessary —
    # the DAG boundary is implicit.
    task1 >> task2
