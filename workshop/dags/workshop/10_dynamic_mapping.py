# -*- coding: utf-8 -*-
"""Module 10: dynamic task mapping — one Carte run per partition.

A Python task decides the partitions at runtime (here: regions; in
real life often files, table shards or tenant lists) and Airflow
expands one CarteTransOperator task per element.
"""

import pendulum

from airflow import DAG
try:
    from airflow.sdk import task  # Airflow 3
except ImportError:
    from airflow.decorators import task  # Airflow 2
from airflow_pentaho.operators.carte import CarteTransOperator

with DAG(
    dag_id='m10_dynamic_mapping',
    description='Workshop module 10: dynamic task mapping over regions',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    tags=['workshop', 'pdi'],
) as dag:

    @task
    def list_partitions():
        # Runtime discovery: query a control table, list files, etc.
        return [
            {'region': 'EMEA', 'date': '{{ ds }}'},
            {'region': 'AMER', 'date': '{{ ds }}'},
            {'region': 'APAC', 'date': '{{ ds }}'},
        ]

    load_region = CarteTransOperator.partial(
        task_id='load_region',
        trans='/home/bi/load_sales',
    ).expand(params=list_partitions())
