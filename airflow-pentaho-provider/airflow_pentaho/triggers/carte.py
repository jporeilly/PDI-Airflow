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
"""Triggers that poll Carte job/transformation status asynchronously.

These run on the Airflow triggerer, freeing the worker slot while a
Carte job or transformation executes remotely. Polling uses the
synchronous hook wrapped in ``sync_to_async`` so no extra HTTP client
dependency is needed.
"""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

from asgiref.sync import sync_to_async

from airflow.triggers.base import BaseTrigger, TriggerEvent

FINISHED_STATUSES = ['Finished']
ERRORS_STATUSES = [
    'Stopped',
    'Finished (with errors)',
    'Stopped (with errors)',
]
END_STATUSES = FINISHED_STATUSES + ERRORS_STATUSES


class CarteJobTrigger(BaseTrigger):
    """Polls Carte until a job reaches an end status."""

    def __init__(self, conn_id, level, job_name, job_id, poll_interval=10):
        super().__init__()
        self.conn_id = conn_id
        self.level = level
        self.job_name = job_name
        self.job_id = job_id
        self.poll_interval = poll_interval

    def serialize(self):
        return (
            'airflow_pentaho.triggers.carte.CarteJobTrigger',
            {
                'conn_id': self.conn_id,
                'level': self.level,
                'job_name': self.job_name,
                'job_id': self.job_id,
                'poll_interval': self.poll_interval,
            },
        )

    def _get_client(self):
        from airflow_pentaho.hooks.carte import PentahoCarteHook
        return PentahoCarteHook(
            conn_id=self.conn_id, level=self.level).get_conn()

    def _poll_once(self, client, previous_response):
        return client.job_status(self.job_name, self.job_id,
                                 previous_response)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            client = await sync_to_async(self._get_client)()
            previous_response = None
            while True:
                status_rs = await sync_to_async(self._poll_once)(
                    client, previous_response)
                if not status_rs or 'jobstatus' not in status_rs:
                    yield TriggerEvent({
                        'status': 'error',
                        'message': 'Unexpected Carte response while polling '
                                   'job status',
                    })
                    return
                previous_response = status_rs
                status_desc = status_rs['jobstatus']['status_desc']
                self.log.info('Job %s (id %s): %s',
                              self.job_name, self.job_id, status_desc)
                if status_desc in END_STATUSES:
                    yield TriggerEvent({
                        'status': 'success'
                        if status_desc in FINISHED_STATUSES else 'error',
                        'status_desc': status_desc,
                        'job_id': self.job_id,
                    })
                    return
                await asyncio.sleep(self.poll_interval)
        except Exception as e:  # noqa: BLE001 - report failure to the task
            yield TriggerEvent({'status': 'error', 'message': str(e)})


class CarteTransTrigger(BaseTrigger):
    """Polls Carte until a transformation reaches an end status."""

    def __init__(self, conn_id, level, trans_name, trans_id=None,
                 poll_interval=10):
        super().__init__()
        self.conn_id = conn_id
        self.level = level
        self.trans_name = trans_name
        self.trans_id = trans_id
        self.poll_interval = poll_interval

    def serialize(self):
        return (
            'airflow_pentaho.triggers.carte.CarteTransTrigger',
            {
                'conn_id': self.conn_id,
                'level': self.level,
                'trans_name': self.trans_name,
                'trans_id': self.trans_id,
                'poll_interval': self.poll_interval,
            },
        )

    def _get_client(self):
        from airflow_pentaho.hooks.carte import PentahoCarteHook
        return PentahoCarteHook(
            conn_id=self.conn_id, level=self.level).get_conn()

    def _poll_once(self, client, previous_response):
        return client.trans_status(self.trans_name, self.trans_id,
                                   previous_response)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            client = await sync_to_async(self._get_client)()
            previous_response = None
            while True:
                status_rs = await sync_to_async(self._poll_once)(
                    client, previous_response)
                if not status_rs or 'transstatus' not in status_rs:
                    yield TriggerEvent({
                        'status': 'error',
                        'message': 'Unexpected Carte response while polling '
                                   'transformation status',
                    })
                    return
                previous_response = status_rs
                status = status_rs['transstatus']
                if 'id' in status:
                    self.trans_id = status['id']
                status_desc = status['status_desc']
                self.log.info('Transformation %s (id %s): %s',
                              self.trans_name, self.trans_id, status_desc)
                if status_desc in END_STATUSES:
                    yield TriggerEvent({
                        'status': 'success'
                        if status_desc in FINISHED_STATUSES else 'error',
                        'status_desc': status_desc,
                        'trans_id': self.trans_id,
                    })
                    return
                await asyncio.sleep(self.poll_interval)
        except Exception as e:  # noqa: BLE001 - report failure to the task
            yield TriggerEvent({'status': 'error', 'message': str(e)})
