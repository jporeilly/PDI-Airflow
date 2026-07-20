# -*- coding: utf-8 -*-
"""Module 1: run a single PDI transformation on Carte.

Requires the `pdi_default` connection and a transformation saved at
/demo/hello_world in the PDI repository (see LAB-SETUP.md).
"""

import pendulum

from airflow import DAG
from airflow_pentaho.operators.carte import CarteTransOperator

with DAG(
    dag_id='m01_carte_trans_basic',
    description='Workshop module 1: first Carte transformation',
    schedule=None,                 # manual trigger for the workshop
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    tags=['workshop', 'pdi'],
) as dag:

    hello = CarteTransOperator(
        task_id='hello_world',
        trans='/demo/hello_world')
