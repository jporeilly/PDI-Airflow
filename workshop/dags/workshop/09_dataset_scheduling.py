# -*- coding: utf-8 -*-
"""Module 9: data-aware scheduling with Datasets.

The producer DAG runs a PDI transformation that refreshes the sales
staging table and declares a Dataset outlet. The consumer DAG has no
cron schedule at all — it runs whenever the dataset is updated.
"""

import pendulum

from airflow import DAG
try:
    from airflow.sdk import Asset as Dataset  # Airflow 3 (Datasets -> Assets)
except ImportError:
    from airflow.datasets import Dataset  # Airflow 2
from airflow_pentaho.operators.carte import CarteJobOperator
from airflow_pentaho.operators.carte import CarteTransOperator

SALES_STAGING = Dataset('warehouse://staging/sales')

with DAG(
    dag_id='m09a_dataset_producer',
    description='Workshop module 9: produces the sales staging dataset',
    schedule='0 4 * * *',
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    tags=['workshop', 'pdi', 'datasets'],
) as producer:

    stage_sales = CarteTransOperator(
        task_id='stage_sales',
        trans='/demo/extract_sales',
        params={'date': '{{ ds }}'},
        outlets=[SALES_STAGING])

with DAG(
    dag_id='m09b_dataset_consumer',
    description='Workshop module 9: runs when sales staging updates',
    schedule=[SALES_STAGING],      # dataset-driven, no cron
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    tags=['workshop', 'pdi', 'datasets'],
) as consumer:

    build_marts = CarteJobOperator(
        task_id='build_marts',
        job='/demo/build_marts')
