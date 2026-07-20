# -*- coding: utf-8 -*-
"""Module 4: pass Airflow context into PDI named parameters.

Demonstrates Jinja templating ({{ ds }}, {{ data_interval_start }})
and runtime-configurable DAG params (Trigger DAG w/ config).
"""

import pendulum

from airflow import DAG
from airflow.models.param import Param
from airflow_pentaho.operators.carte import CarteTransOperator

with DAG(
    dag_id='m04_params_templating',
    description='Workshop module 4: parameters and templating',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    params={
        # NOTE: no enum here — the operator's PDI params merge into the
        # DAG params at parse time, and the Jinja string
        # '{{ params.region }}' must validate as a plain string.
        'region': Param('EMEA', type='string',
                        description='Sales region: EMEA, AMER or APAC'),
    },
    tags=['workshop', 'pdi'],
) as dag:

    load_sales = CarteTransOperator(
        task_id='load_sales',
        trans='/home/bi/load_sales',
        params={
            # Airflow template macros become PDI named parameters
            'date': '{{ ds }}',
            'window_start': '{{ data_interval_start.isoformat() }}',
            'window_end': '{{ data_interval_end.isoformat() }}',
            # UI-configurable value (Trigger DAG w/ config)
            'region': '{{ params.region }}',
        })
