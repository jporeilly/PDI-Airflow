# -*- coding: utf-8 -*-
"""Modernized course DAG: async-trigger.py (2023) -> deferrable mode.

The 2023 "async" fired curl and moved on — a failed transformation
still showed green in Airflow. Deferrable mode is the honest
equivalent: the worker slot is released while Carte runs (poll happens
on the triggerer), but the task still reflects the real outcome.
"""

import pendulum

from airflow import DAG
from airflow_pentaho.operators.carte import CarteTransOperator

with DAG(
    dag_id='async_trigger',
    description='Course DAG modernized: deferrable Carte transformations',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    tags=['course', 'pdi', 'deferrable'],
) as dag:

    task1 = CarteTransOperator(
        task_id='Task_1',
        trans='/process1/task1',
        deferrable=True,
        poll_interval=10)

    task2 = CarteTransOperator(
        task_id='Task_2',
        trans='/process1/task2',
        deferrable=True,
        poll_interval=10)

    task1 >> task2
