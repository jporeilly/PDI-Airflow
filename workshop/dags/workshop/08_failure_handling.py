# -*- coding: utf-8 -*-
"""Module 8: failure handling — retries, err_count XCom, callbacks.

The Carte operators always push an `err_count` XCom (number of log
lines containing 'error') and return the last log line as
return_value. This DAG branches on err_count after the load.
"""

from datetime import timedelta

import pendulum

from airflow import DAG
try:
    from airflow.providers.standard.operators.python import BranchPythonOperator  # Airflow 3
except ImportError:
    from airflow.operators.python import BranchPythonOperator  # Airflow 2
try:
    from airflow.providers.standard.operators.bash import BashOperator  # Airflow 3
except ImportError:
    from airflow.operators.bash import BashOperator  # Airflow 2
from airflow_pentaho.operators.carte import CarteTransOperator


def _route_on_errors(ti=None, **_):
    err_count = ti.xcom_pull(task_ids='load_with_soft_errors',
                             key='err_count') or 0
    return 'notify_data_team' if int(err_count) > 0 else 'all_clean'


def _fail_alert(context):
    print('ALERT: task {} failed'.format(
        context['task_instance'].task_id))


with DAG(
    dag_id='m08_failure_handling',
    description='Workshop module 8: retries, err_count, callbacks',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
        'retry_exponential_backoff': True,
        'on_failure_callback': _fail_alert,
    },
    tags=['workshop', 'pdi'],
) as dag:

    load = CarteTransOperator(
        task_id='load_with_soft_errors',
        trans='/home/bi/load_warehouse',
        params={'date': '{{ ds }}'},
        execution_timeout=timedelta(minutes=30))

    route = BranchPythonOperator(
        task_id='route_on_errors',
        python_callable=_route_on_errors)

    notify = BashOperator(
        task_id='notify_data_team',
        bash_command='echo "soft errors in load — see task logs"')

    all_clean = BashOperator(
        task_id='all_clean',
        bash_command='echo "load clean"')

    load >> route >> [notify, all_clean]
