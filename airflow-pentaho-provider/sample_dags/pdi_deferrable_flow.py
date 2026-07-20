# -*- coding: utf-8 -*-
# Copyright 2026 Pentaho
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
"""Example DAG running Carte jobs in deferrable mode.

While the job runs on the Carte server, the Airflow worker slot is
released and status polling happens on the triggerer. This is the
recommended mode on Astronomer, where worker slots are a metered
resource.
"""

from datetime import timedelta

import pendulum

from airflow import DAG

from airflow_pentaho.operators.carte import CarteJobOperator
from airflow_pentaho.operators.carte import CarteTransOperator

DAG_NAME = 'pdi_deferrable_flow'
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

    enrich = CarteTransOperator(
        task_id='enrich_customer_data',
        trans='/home/bi/enrich_customer_data',
        params={'date': '{{ ds }}'},
        deferrable=True,
        poll_interval=30)

    aggregate = CarteJobOperator(
        task_id='average_spent',
        job='/home/bi/average_spent',
        params={'date': '{{ ds }}'},
        deferrable=True,
        poll_interval=30)

    enrich >> aggregate
