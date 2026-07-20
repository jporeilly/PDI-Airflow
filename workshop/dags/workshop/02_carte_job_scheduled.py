# -*- coding: utf-8 -*-
"""Module 2 & 3: run a PDI job on Carte on a daily cron schedule."""

from datetime import timedelta

import pendulum

from airflow import DAG
from airflow_pentaho.operators.carte import CarteJobOperator

with DAG(
    dag_id='m02_carte_job_scheduled',
    description='Workshop module 2: scheduled Carte job',
    schedule='30 6 * * *',         # every day at 06:30
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,                 # do not backfill missed runs
    dagrun_timeout=timedelta(hours=1),
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['workshop', 'pdi'],
) as dag:

    nightly = CarteJobOperator(
        task_id='nightly_job',
        job='/demo/nightly_job',
        params={'date': '{{ ds }}'})
