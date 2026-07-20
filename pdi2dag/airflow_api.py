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
"""Minimal Airflow REST API client, supporting Airflow 2.x and 3.x.

- **Airflow 3.x**: stable REST **API v2** with **JWT** auth - a token
  is obtained from ``POST /auth/token`` and sent as a Bearer header.
- **Airflow 2.x**: stable **API v1** with **basic auth**
  (``AIRFLOW__API__AUTH_BACKENDS=...backend.basic_auth``).

The API version is auto-detected on first use (a JWT token request
that succeeds selects v2; otherwise v1). Used to wait for, activate
(unpause) and trigger DAGs that pdi2dag deployed.
"""

from __future__ import annotations

import time

import requests


class AirflowApiError(RuntimeError):
    pass


class AirflowClient:
    """Talks to the Airflow stable REST API (v2 JWT or v1 basic auth)."""

    def __init__(self, base_url, username, password, timeout=30):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.timeout = timeout
        self._api = None      # 'v2' or 'v1', detected lazily
        self._token = None

    # ---- auth / version detection ----------------------------------

    def _fetch_token(self):
        """Airflow 3 JWT: POST /auth/token -> {access_token}."""
        try:
            rs = requests.post(
                self.base_url + '/auth/token',
                json={'username': self.username, 'password': self.password},
                timeout=self.timeout)
        except requests.RequestException:
            return None
        if rs.status_code < 400:
            tok = (rs.json() or {}).get('access_token') if rs.content \
                else None
            return tok
        return None

    def _detect(self):
        if self._api:
            return
        token = self._fetch_token()
        if token:
            self._token = token
            self._api = 'v2'
        else:
            self._api = 'v1'

    def _request(self, method, path, ok404=False, **kwargs):
        self._detect()
        url = '{}/api/{}{}'.format(self.base_url, self._api, path)
        headers = {}
        auth = None
        if self._api == 'v2':
            headers['Authorization'] = 'Bearer ' + self._token
        else:
            auth = (self.username, self.password)
        rs = requests.request(method, url, headers=headers, auth=auth,
                              timeout=self.timeout, **kwargs)
        # Refresh an expired v2 token once.
        if rs.status_code == 401 and self._api == 'v2':
            self._token = self._fetch_token()
            if self._token:
                headers['Authorization'] = 'Bearer ' + self._token
                rs = requests.request(method, url, headers=headers,
                                      timeout=self.timeout, **kwargs)
        if rs.status_code == 404 and ok404:
            return None
        if rs.status_code >= 400:
            raise AirflowApiError(
                'Airflow API ({}) returned HTTP {} on {} {}: {}'.format(
                    self._api, rs.status_code, method, path,
                    rs.text[:400]))
        return rs.json() if rs.content else None

    # ---- operations ------------------------------------------------

    def get_dag(self, dag_id):
        return self._request('GET', '/dags/{}'.format(dag_id), ok404=True)

    def wait_for_dag(self, dag_id, timeout=360, poll_interval=5):
        """Wait until the scheduler has parsed the deployed DAG file."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.get_dag(dag_id) is not None:
                return True
            time.sleep(poll_interval)
        raise AirflowApiError(
            "DAG '{}' did not appear within {}s - check the scheduler "
            'logs and the dags folder.'.format(dag_id, timeout))

    def set_paused(self, dag_id, paused):
        # Same shape on v1 and v2.
        return self._request('PATCH', '/dags/{}'.format(dag_id),
                             json={'is_paused': paused})

    def trigger_dag_run(self, dag_id, conf=None):
        self._detect()
        body = {'conf': conf or {}}
        if self._api == 'v2':
            # v2 accepts a nullable logical_date for a manual run.
            body['logical_date'] = None
        return self._request('POST', '/dags/{}/dagRuns'.format(dag_id),
                             json=body)

    def get_dag_run(self, dag_id, run_id):
        return self._request(
            'GET', '/dags/{}/dagRuns/{}'.format(dag_id, run_id))
