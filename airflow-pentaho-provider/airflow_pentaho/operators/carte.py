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
"""Carte operator module."""

from __future__ import annotations

import base64
import json
import re
import time
import warnings
import zlib

from airflow.configuration import conf
from airflow.exceptions import AirflowException

try:  # Airflow 3
    from airflow.sdk import BaseOperator
except ImportError:  # Airflow 2
    from airflow.models import BaseOperator

from airflow_pentaho.hooks.carte import PentahoCarteHook
from airflow_pentaho.triggers.carte import CarteJobTrigger
from airflow_pentaho.triggers.carte import CarteTransTrigger


class CarteBaseOperator(BaseOperator):
    """Carte Base Operator."""

    FINISHED_STATUSES = ['Finished']
    ERRORS_STATUSES = [
        'Stopped',
        'Finished (with errors)',
        'Stopped (with errors)',
    ]
    END_STATUSES = FINISHED_STATUSES + ERRORS_STATUSES

    DEFAULT_CONN_ID = 'pdi_default'

    template_fields = ('task_params',)

    def __init__(self,
                 *args,
                 pdi_conn_id=None,
                 level='Basic',
                 poll_interval=5,
                 deferrable=conf.getboolean(
                     'operators', 'default_deferrable', fallback=False),
                 xcom_push=None,
                 **kwargs):
        super().__init__(*args, **kwargs)
        if xcom_push is not None:
            warnings.warn(
                "The 'xcom_push' argument is deprecated; the last log line "
                'is now always returned (XCom return_value) and err_count '
                'is always pushed.',
                DeprecationWarning, stacklevel=3)
        self.pdi_conn_id = pdi_conn_id or self.DEFAULT_CONN_ID
        self.level = level
        self.poll_interval = poll_interval
        self.deferrable = deferrable

    def _get_pentaho_carte_client(self):
        return PentahoCarteHook(conn_id=self.pdi_conn_id,
                                level=self.level).get_conn()

    def _log_logging_string(self, raw_logging_string):
        """Decode and print Carte's gzip+base64 logging payload.

        Returns the last non-empty line and the error line count.
        """
        if not raw_logging_string:
            return '', 0
        cdata = re.match(r'<!\[CDATA\[(.*)\]\]>', raw_logging_string,
                         re.DOTALL)
        cdata = cdata.group(1) if cdata else raw_logging_string
        try:
            decoded_lines = zlib.decompress(base64.b64decode(cdata),
                                            16 + zlib.MAX_WBITS)
        except (ValueError, zlib.error):
            self.log.info(raw_logging_string)
            return raw_logging_string, 0
        err_count = 0
        output_line = ''
        if decoded_lines:
            for line in re.compile(r'\r\n|\n|\r').split(
                    decoded_lines.decode('utf-8', errors='replace')):
                if 'error' in line.lower():
                    err_count += 1
                self.log.info(line)
                if len(line) > 0:
                    output_line = line
        if err_count:
            self.log.info('Errors: %s', err_count)
        return output_line, err_count

    @staticmethod
    def _push_err_count(context, err_count):
        ti = context.get('ti') if context else None
        if ti is not None:
            ti.xcom_push(key='err_count', value=err_count)


class CarteJobOperator(CarteBaseOperator):
    """Carte Job operator. Runs a job on a Carte service."""

    LOG_TEMPLATE = '%s: %s, with id %s'

    def __init__(self,
                 *args,
                 job=None,
                 params=None,
                 **kwargs):
        """
        Execute a Job in a remote Carte server from a PDI repository.

        :param job: The full repository path of the job.
        :param params: Named input parameters, as a dict.
        :param pdi_conn_id: Pentaho connection ID (default ``pdi_default``).
        :param level: Logging level (Basic, Detailed, Debug, Rowlevel,
            Error, Nothing); default is Basic.
        :param poll_interval: Seconds between status polls (default 5).
        :param deferrable: Release the worker slot while the job runs and
            poll from the triggerer instead (Airflow deferrable mode).
        """
        super().__init__(*args, **kwargs)
        self.job = job
        self.task_params = params
        self.job_id = None

    def _get_job_name(self):
        return self.job.split('/').pop().replace('.kjb', '')

    def execute(self, context):
        conn = self._get_pentaho_carte_client()

        exec_job_rs = conn.run_job(self.job, self.task_params)
        message = exec_job_rs['webresult']['message']
        self.job_id = exec_job_rs['webresult']['id']
        self.log.info(self.LOG_TEMPLATE, message, self.job, self.job_id)

        if self.deferrable:
            self.defer(
                trigger=CarteJobTrigger(
                    conn_id=self.pdi_conn_id,
                    level=self.level,
                    job_name=self._get_job_name(),
                    job_id=self.job_id,
                    poll_interval=self.poll_interval),
                method_name='execute_complete')

        return self._wait_for_completion(conn, context)

    def _wait_for_completion(self, conn, context):
        status_job_rs = None
        status = None
        status_desc = None
        output = ''
        err_count = 0
        while not status_job_rs or status_desc not in self.END_STATUSES:
            status_job_rs = conn.job_status(self._get_job_name(),
                                            self.job_id, status_job_rs)
            if not status_job_rs or 'jobstatus' not in status_job_rs:
                raise AirflowException(
                    'Unexpected server response: '
                    + json.dumps(status_job_rs))

            status = status_job_rs['jobstatus']
            status_desc = status['status_desc']
            self.log.info(self.LOG_TEMPLATE, status_desc, self.job,
                          self.job_id)
            line, errs = self._log_logging_string(
                status.get('logging_string'))
            if line:
                output = line
            err_count += errs

            if status_desc not in self.END_STATUSES:
                self.log.debug('Sleeping %s seconds before asking again',
                               self.poll_interval)
                time.sleep(self.poll_interval)

        self._push_err_count(context, err_count)

        if status.get('error_desc'):
            self.log.error(self.LOG_TEMPLATE, status['error_desc'],
                           self.job, self.job_id)
            raise AirflowException(status['error_desc'])

        if status_desc in self.ERRORS_STATUSES:
            self.log.error(self.LOG_TEMPLATE, status_desc, self.job,
                           self.job_id)
            raise AirflowException(status_desc)

        return output

    def execute_complete(self, context, event):
        """Resumed after the trigger reports an end status.

        Fetches the final status (with the full log, from line 0) so the
        complete Carte log lands in the task log.
        """
        if event.get('status') == 'error' and 'message' in event:
            raise AirflowException(event['message'])

        self.job_id = event.get('job_id', self.job_id)
        conn = self._get_pentaho_carte_client()
        status_job_rs = conn.job_status(self._get_job_name(), self.job_id)
        if not status_job_rs or 'jobstatus' not in status_job_rs:
            raise AirflowException(
                'Unexpected server response: ' + json.dumps(status_job_rs))

        status = status_job_rs['jobstatus']
        status_desc = status['status_desc']
        output, err_count = self._log_logging_string(
            status.get('logging_string'))
        self._push_err_count(context, err_count)

        if status.get('error_desc'):
            self.log.error(self.LOG_TEMPLATE, status['error_desc'],
                           self.job, self.job_id)
            raise AirflowException(status['error_desc'])

        if status_desc in self.ERRORS_STATUSES:
            self.log.error(self.LOG_TEMPLATE, status_desc, self.job,
                           self.job_id)
            raise AirflowException(status_desc)

        return output

    def on_kill(self):
        if self.job_id:
            self.log.info('Stopping Carte job %s (id %s)', self.job,
                          self.job_id)
            try:
                conn = self._get_pentaho_carte_client()
                conn.stop_job(self._get_job_name(), self.job_id)
            except Exception as e:  # noqa: BLE001 - best effort on kill
                self.log.warning('Could not stop Carte job: %s', e)


class CarteTransOperator(CarteBaseOperator):
    """Carte Transformation operator. Runs a transformation on a Carte
    service."""

    LOG_TEMPLATE = '%s: %s'

    def __init__(self,
                 *args,
                 trans=None,
                 params=None,
                 **kwargs):
        """
        Execute a Transformation in a remote Carte server from a PDI
        repository.

        :param trans: The full repository path of the transformation.
        :param params: Named input parameters, as a dict.
        :param pdi_conn_id: Pentaho connection ID (default ``pdi_default``).
        :param level: Logging level (Basic, Detailed, Debug, Rowlevel,
            Error, Nothing); default is Basic.
        :param poll_interval: Seconds between status polls (default 5).
        :param deferrable: Release the worker slot while the transformation
            runs and poll from the triggerer instead.
        """
        super().__init__(*args, **kwargs)
        self.trans = trans
        self.task_params = params
        self.trans_id = None

    def _get_trans_name(self):
        return self.trans.split('/').pop().replace('.ktr', '')

    def execute(self, context):
        conn = self._get_pentaho_carte_client()

        conn.run_trans(self.trans, self.task_params)
        self.log.info('Executing %s', self.trans)

        if self.deferrable:
            self.defer(
                trigger=CarteTransTrigger(
                    conn_id=self.pdi_conn_id,
                    level=self.level,
                    trans_name=self._get_trans_name(),
                    trans_id=None,
                    poll_interval=self.poll_interval),
                method_name='execute_complete')

        return self._wait_for_completion(conn, context)

    def _wait_for_completion(self, conn, context):
        status_trans_rs = None
        status = None
        status_desc = None
        output = ''
        err_count = 0
        while not status_trans_rs or status_desc not in self.END_STATUSES:
            status_trans_rs = conn.trans_status(self._get_trans_name(),
                                                self.trans_id,
                                                status_trans_rs)
            if not status_trans_rs or 'transstatus' not in status_trans_rs:
                raise AirflowException(
                    'Unexpected server response: '
                    + json.dumps(status_trans_rs))

            status = status_trans_rs['transstatus']
            if 'id' in status:
                self.trans_id = status['id']
            status_desc = status['status_desc']
            self.log.info(self.LOG_TEMPLATE, status_desc, self.trans)
            line, errs = self._log_logging_string(
                status.get('logging_string'))
            if line:
                output = line
            err_count += errs

            if status_desc not in self.END_STATUSES:
                self.log.debug('Sleeping %s seconds before asking again',
                               self.poll_interval)
                time.sleep(self.poll_interval)

        self._push_err_count(context, err_count)

        if status.get('error_desc'):
            self.log.error(self.LOG_TEMPLATE, status['error_desc'],
                           self.trans)
            raise AirflowException(status['error_desc'])

        if status_desc in self.ERRORS_STATUSES:
            self.log.error(self.LOG_TEMPLATE, status_desc, self.trans)
            raise AirflowException(status_desc)

        return output

    def execute_complete(self, context, event):
        """Resumed after the trigger reports an end status."""
        if event.get('status') == 'error' and 'message' in event:
            raise AirflowException(event['message'])

        self.trans_id = event.get('trans_id', self.trans_id)
        conn = self._get_pentaho_carte_client()
        status_trans_rs = conn.trans_status(self._get_trans_name(),
                                            self.trans_id)
        if not status_trans_rs or 'transstatus' not in status_trans_rs:
            raise AirflowException(
                'Unexpected server response: '
                + json.dumps(status_trans_rs))

        status = status_trans_rs['transstatus']
        status_desc = status['status_desc']
        output, err_count = self._log_logging_string(
            status.get('logging_string'))
        self._push_err_count(context, err_count)

        if status.get('error_desc'):
            self.log.error(self.LOG_TEMPLATE, status['error_desc'],
                           self.trans)
            raise AirflowException(status['error_desc'])

        if status_desc in self.ERRORS_STATUSES:
            self.log.error(self.LOG_TEMPLATE, status_desc, self.trans)
            raise AirflowException(status_desc)

        return output

    def on_kill(self):
        self.log.info('Stopping Carte transformation %s', self.trans)
        try:
            conn = self._get_pentaho_carte_client()
            conn.stop_trans(self._get_trans_name(), self.trans_id)
        except Exception as e:  # noqa: BLE001 - best effort on kill
            self.log.warning('Could not stop Carte transformation: %s', e)
