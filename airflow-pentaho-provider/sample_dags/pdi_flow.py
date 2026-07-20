# -*- coding: utf-8 -*-
# Copyright 2020 Aneior Studio, SL
# Modifications Copyright 2026 Pentaho
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Example DAG mixing local (Kitchen/Pan) and remote (Carte) PDI tasks."""

from datetime import timedelta

import pendulum

from airflow import DAG

from airflow_pentaho.operators.kettle import KitchenOperator
from airflow_pentaho.operators.kettle import PanOperator
from airflow_pentaho.operators.carte import CarteJobOperator
from airflow_pentaho.operators.carte import CarteTransOperator

DAG_NAME = 'pdi_flow'
DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

with DAG(dag_id=DAG_NAME,
         default_args=DEFAULT_ARGS,
         start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
         dagrun_timeout=timedelta(hours=2),
         schedule='30 0 * * *',
         catchup=False) as dag:

    job1 = KitchenOperator(
        task_id='job1',
        directory='/home/bi',
        job='test_job',
        params={'date': '{{ ds }}'})

    trans1 = PanOperator(
        task_id='trans1',
        directory='/home/bi',
        trans='test_trans',
        params={'date': '{{ ds }}'})

    trans2 = CarteTransOperator(
        task_id='trans2',
        trans='/home/bi/test_trans',
        params={'date': '{{ ds }}'})

    job3 = CarteJobOperator(
        task_id='job3',
        job='/home/bi/test_job',
        params={'date': '{{ ds }}'})

    job1 >> trans1 >> trans2 >> job3
