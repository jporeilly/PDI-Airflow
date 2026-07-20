# -*- coding: utf-8 -*-
"""Module 6: run PDI locally on the worker with Kitchen and Pan.

Requires PDI installed on the worker at the connection's
`pentaho_home` (see extra field), so in the Docker lab this DAG is for
reading; run it on a worker VM with PDI installed. Route such tasks to
dedicated workers with `queue='pdi'`.
"""

import pendulum

from airflow import DAG
from airflow_pentaho.operators.kettle import KitchenOperator
from airflow_pentaho.operators.kettle import PanOperator

with DAG(
    dag_id='m06_kitchen_pan_local',
    description='Workshop module 6: local Kitchen/Pan execution',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    tags=['workshop', 'pdi', 'local'],
) as dag:

    clean_input = PanOperator(
        task_id='clean_input',
        # queue='pdi',             # uncomment with dedicated PDI workers
        directory='/home/bi',
        trans='clean_somedata',
        params={'file': '/tmp/input/{{ ds }}/sales.csv'})

    run_job_from_file = KitchenOperator(
        task_id='run_job_from_file',
        # queue='pdi',
        file='/opt/etl/local_job.kjb',   # file-based, no repository
        params={'date': '{{ ds }}'})

    clean_input >> run_job_from_file
