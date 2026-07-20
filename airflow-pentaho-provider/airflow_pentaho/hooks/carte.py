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
"""Carte hook module."""

from __future__ import annotations

import json

import requests
import xmltodict
from requests.auth import HTTPBasicAuth

from airflow.exceptions import AirflowException

try:  # Airflow 3
    from airflow.sdk import BaseHook
except ImportError:  # Airflow 2
    from airflow.hooks.base import BaseHook


def _param_value(val):
    """Unwrap ``airflow.models.Param``-like objects to their plain value."""
    return getattr(val, 'value', val)


class PentahoCarteHook(BaseHook):
    """Hook to interact with the Pentaho Carte REST API."""

    conn_name_attr = 'conn_id'
    default_conn_name = 'pdi_default'
    conn_type = 'pentaho'
    hook_name = 'Pentaho'

    class PentahoCarteClient:
        """Client for Carte REST calls."""

        RUN_JOB_ENDPOINT = '/kettle/executeJob/'
        JOB_STATUS_ENDPOINT = '/kettle/jobStatus/'
        STOP_JOB_ENDPOINT = '/kettle/stopJob/'
        RUN_TRANS_ENDPOINT = '/kettle/executeTrans/'
        TRANS_STATUS_ENDPOINT = '/kettle/transStatus/'
        STOP_TRANS_ENDPOINT = '/kettle/stopTrans/'

        def __init__(
                self,
                host,
                port,
                rep,
                username,
                password,
                carte_username,
                carte_password,
                level='Basic',
                verify_ssl=True,
                timeout=60):
            self.host = host or ''
            if not self.host.startswith('http'):
                self.host = 'http://{}'.format(self.host)
            self.port = port
            self.rep = rep
            self.username = username
            self.password = password
            self.carte_username = carte_username
            self.carte_password = carte_password
            self.level = level
            self.verify_ssl = verify_ssl
            self.timeout = timeout

        def __get_url(self, endpoint):
            return '{}:{}{}'.format(self.host, self.port, endpoint)

        def __get_auth(self):
            return HTTPBasicAuth(self.carte_username, self.carte_password)

        def _post(self, endpoint, payload):
            """POST form-encoded data to a Carte endpoint.

            Carte's ``BaseHttpServlet`` delegates POST to GET handling, so
            form fields are read exactly like query parameters — but they
            stay out of URLs and access logs (credentials travel in the
            request body instead of the query string).
            """
            rs = requests.post(
                url=self.__get_url(endpoint),
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded'},
                data=payload,
                auth=self.__get_auth(),
                verify=self.verify_ssl,
                timeout=self.timeout)
            return self._parse_response(rs)

        @staticmethod
        def _parse_response(rs):
            if rs.status_code >= 400:
                try:
                    result = xmltodict.parse(rs.content)
                    raise AirflowException('{}: {}'.format(
                        result['webresult']['result'],
                        result['webresult']['message']))
                except AirflowException:
                    raise
                except Exception:  # noqa: BLE001 - non-XML error body
                    raise AirflowException(
                        'Carte returned HTTP {}: {}'.format(
                            rs.status_code, rs.content))
            if rs.content:
                return xmltodict.parse(rs.content)
            return None

        def _connection_args(self):
            return {
                'user': self.username,
                'pass': self.password,
                'rep': self.rep,
                'level': self.level,
            }

        def run_job(self, job_path, params=None):
            payload = self._connection_args()
            payload['job'] = job_path
            if params:
                for k, val in params.items():
                    payload[k] = _param_value(val)
            return self._post(self.RUN_JOB_ENDPOINT, payload)

        def job_status(self, job_name, job_id, previous_response=None):
            from_line = previous_response['jobstatus']['last_log_line_nr'] \
                if previous_response else 0
            payload = {
                'name': job_name,
                'id': job_id,
                'xml': 'Y',
                'from': from_line,
            }
            return self._post(self.JOB_STATUS_ENDPOINT, payload)

        def stop_job(self, job_name, job_id=None):
            payload = {'name': job_name, 'xml': 'Y'}
            if job_id:
                payload['id'] = job_id
            return self._post(self.STOP_JOB_ENDPOINT, payload)

        def run_trans(self, trans_path, params=None):
            payload = self._connection_args()
            payload['trans'] = trans_path
            if params:
                for k, val in params.items():
                    payload[k] = _param_value(val)
            return self._post(self.RUN_TRANS_ENDPOINT, payload)

        def trans_status(self, trans_name, trans_id=None,
                         previous_response=None):
            from_line = previous_response['transstatus']['last_log_line_nr'] \
                if previous_response else 0
            payload = {
                'name': trans_name,
                'xml': 'Y',
                'from': from_line,
            }
            if trans_id:
                payload['id'] = trans_id
            return self._post(self.TRANS_STATUS_ENDPOINT, payload)

        def stop_trans(self, trans_name, trans_id=None):
            payload = {'name': trans_name, 'xml': 'Y'}
            if trans_id:
                payload['id'] = trans_id
            return self._post(self.STOP_TRANS_ENDPOINT, payload)

    @classmethod
    def get_ui_field_behaviour(cls):
        """Customize the connection form in the Airflow UI."""
        return {
            'hidden_fields': ['schema'],
            'relabeling': {
                'host': 'Carte host (include http:// or https://)',
                'port': 'Carte port',
                'login': 'PDI repository username',
                'password': 'PDI repository password',
            },
            'placeholders': {
                'extra': json.dumps(
                    {
                        'rep': 'Default',
                        'carte_username': 'cluster',
                        'carte_password': 'cluster',
                        'pentaho_home': '/opt/pentaho',
                        'verify_ssl': True,
                    },
                    indent=4),
            },
        }

    def __init__(self, conn_id=default_conn_name, level='Basic'):
        super().__init__()
        self.conn_id = conn_id
        self.level = level
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson
        self.pentaho_cli = None

    def get_conn(self):
        """Provide the client required to run jobs and transformations
        on Carte.
        """
        if self.pentaho_cli:
            return self.pentaho_cli

        self.pentaho_cli = self.PentahoCarteClient(
            host=self.connection.host,
            port=self.connection.port,
            rep=self.extras.get('rep'),
            username=self.connection.login,
            password=self.connection.password,
            carte_username=self.extras.get('carte_username'),
            carte_password=self.extras.get('carte_password'),
            level=self.level,
            verify_ssl=self.extras.get('verify_ssl', True),
            timeout=self.extras.get('timeout', 60))

        return self.pentaho_cli
