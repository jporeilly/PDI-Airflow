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
"""Kettle operator module."""

from __future__ import annotations

import os
import platform
import re
import warnings
from subprocess import PIPE, STDOUT, Popen
from tempfile import TemporaryDirectory

import psutil

from airflow.exceptions import AirflowException

try:  # Airflow 3
    from airflow.sdk import BaseOperator
except ImportError:  # Airflow 2
    from airflow.models import BaseOperator

from airflow_pentaho.hooks.kettle import PentahoHook


class PDIBaseOperator(BaseOperator):
    """Runs Kettle commands and tracks their logging."""

    DEFAULT_CONN_ID = 'pdi_default'

    def __init__(
            self,
            task_id=None,
            xcom_push=None,
            **kwargs):
        super().__init__(task_id=task_id, **kwargs)
        if xcom_push is not None:
            warnings.warn(
                "The 'xcom_push' argument is deprecated; the last log line "
                'is now always returned (XCom return_value) and err_count '
                'is always pushed.',
                DeprecationWarning, stacklevel=3)
        self.sub_process = None
        self.command_line = None
        self.codes_map: dict = {}

    def _run_command(self):
        is_windows = platform.system() == 'Windows'

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            suffix = '.bat' if is_windows else '.sh'
            fname = os.path.join(tmp_dir, self.task_id + suffix)
            with open(fname, 'w', encoding='utf-8') as f:
                f.write(self.command_line)
            self.log.info('Temporary script location: %s', fname)

            command_line_log = PDIBaseOperator._hide_sensitive_data(
                self.command_line)
            self.log.info('Running PDI: %s', command_line_log)

            argv = ['cmd', '/c', fname] if is_windows else ['bash', fname]
            self.sub_process = Popen(  # pylint: disable=W1509
                argv,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=tmp_dir)

            self.log.info('Output:')
            err_count = 0
            line = ''
            for raw_line in iter(self.sub_process.stdout.readline, b''):
                line = raw_line.decode('utf-8', errors='replace').rstrip()
                if 'error' in line.lower():
                    err_count += 1
                    self.log.info('Errors: %s', err_count)
                self.log.info(line)
            self.sub_process.wait()

            message = self.codes_map.get(
                self.sub_process.returncode,
                'Unknown status code: {}'.format(
                    self.sub_process.returncode))
            self.log.info(
                'Status Code %s: ' + message,
                self.sub_process.returncode
            )

            if self.sub_process.returncode:
                raise AirflowException(message)

        return line, err_count

    @staticmethod
    def _hide_sensitive_data(text):
        return re.sub(r'(-|/)pass(=|:)([^\s]+)', '', text)

    @staticmethod
    def _push_err_count(context, err_count):
        ti = context.get('ti') if context else None
        if ti is not None:
            ti.xcom_push(key='err_count', value=err_count)

    def on_kill(self):
        if self.sub_process and hasattr(self.sub_process, 'pid'):
            self.log.info('Sending SIGTERM signal to PDI process %s',
                          self.sub_process.pid)

            # Get process
            parent = psutil.Process(self.sub_process.pid)

            # Terminate
            child_processes = parent.children(recursive=True)
            for child in child_processes:
                child.terminate()
            parent.terminate()

            _, alive = psutil.wait_procs(child_processes, timeout=10)

            # Kill
            for p in alive:
                p.kill()
            parent.kill()


class PanOperator(PDIBaseOperator):
    """PanOperator runs pan (transformations) and tracks logging."""

    STATUS_CODES = {
        0: 'The transformation ran without a problem.',
        1: 'Errors occurred during processing',
        2: 'An unexpected error occurred during loading / running of the'
           ' transformation',
        3: 'Unable to prepare and initialize this transformation',
        7: "The transformation couldn't be loaded from XML or the Repository",
        8: 'Error loading steps or plugins (error in loading one of the'
           ' plugins mostly)',
        9: 'Command line usage printing'
    }

    template_fields = ('task_params',)

    def __init__(self,
                 task_id=None,
                 trans=None,
                 params=None,
                 directory=None,
                 file=None,
                 pdi_conn_id=None,
                 level='Basic',
                 logfile='/dev/stdout',
                 safemode=False,
                 maxloglines=0,
                 maxlogtimeout=0,
                 **kwargs):
        """
        Execute a Pan command (Pentaho Transformation). Pan runs
        transformations, either from a PDI repository (database or
        enterprise), or from a local file.

        :param trans: The name of the transformation (as it appears in the
            repository) to launch.
        :param params: Named input parameters, as a dict.
        :param directory: The repository directory that contains the
            transformation, including the leading slash.
        :param file: If you are calling a local KTR file, this is the
            filename, including the absolute path.
        :param pdi_conn_id: Pentaho connection ID (default ``pdi_default``).
        :param level: Logging level (Basic, Detailed, Debug, Rowlevel,
            Error, Nothing); default is Basic.
        :param logfile: A local filename to write log output to.
        :param safemode: Runs in safe mode, which enables extra checking.
        :param maxloglines: The maximum number of log lines that are kept
            internally by PDI. Set to 0 to keep all rows (default).
        :param maxlogtimeout: The maximum age (in minutes) of a log line
            while being kept internally by PDI. Set to 0 to keep all rows
            indefinitely (default).
        """
        super().__init__(task_id=task_id, **kwargs)

        self.pdi_conn_id = pdi_conn_id or self.DEFAULT_CONN_ID
        self.dir = directory
        self.file = file
        self.trans = trans
        self.level = level
        self.logfile = logfile
        self.safemode = safemode
        self.task_params = params
        self.maxloglines = maxloglines
        self.maxlogtimeout = maxlogtimeout
        self.codes_map = self.STATUS_CODES

    def _get_pentaho_client(self):
        return PentahoHook(self.pdi_conn_id).get_conn()

    def execute(self, context):
        conn = self._get_pentaho_client()

        arguments = {
            'dir': self.dir,
            'trans': self.trans,
            'level': self.level,
            'logfile': self.logfile,
            'safemode': 'true' if self.safemode else 'false',
            'maxloglines': str(self.maxloglines),
            'maxlogtimeout': str(self.maxlogtimeout)
        }
        if self.file:
            arguments.update({'file': self.file})
            arguments.update({'norep': 'true'})

        self.command_line = conn.build_command('pan', arguments,
                                               self.task_params)
        output, err_count = self._run_command()

        self._push_err_count(context, err_count)
        return output


class KitchenOperator(PDIBaseOperator):
    """KitchenOperator runs kitchen (jobs) and tracks logging."""

    STATUS_CODES = {
        0: 'The job ran without a problem.',
        1: 'Errors occurred during processing',
        2: 'An unexpected error occurred during loading or running of the'
           ' job',
        7: "The job couldn't be loaded from XML or the Repository",
        8: 'Error loading steps or plugins (error in loading one of the'
           ' plugins mostly)',
        9: 'Command line usage printing'
    }

    template_fields = ('task_params',)

    def __init__(self,
                 task_id=None,
                 job=None,
                 params=None,
                 directory=None,
                 file=None,
                 pdi_conn_id=None,
                 level='Basic',
                 logfile='/dev/stdout',
                 safemode=False,
                 maxloglines=0,
                 maxlogtimeout=0,
                 **kwargs):
        """
        Execute a Kitchen command (Pentaho Job). Kitchen runs jobs, either
        from a PDI repository (database or enterprise), or from a local
        file.

        :param job: The name of the job (as it appears in the repository)
            to launch.
        :param params: Named input parameters, as a dict.
        :param directory: The repository directory that contains the job,
            including the leading slash.
        :param file: If you are calling a local KJB file, this is the
            filename, including the absolute path.
        :param pdi_conn_id: Pentaho connection ID (default ``pdi_default``).
        :param level: Logging level (Basic, Detailed, Debug, Rowlevel,
            Error, Nothing); default is Basic.
        :param logfile: A local filename to write log output to.
        :param safemode: Runs in safe mode, which enables extra checking.
        :param maxloglines: The maximum number of log lines that are kept
            internally by PDI. Set to 0 to keep all rows (default).
        :param maxlogtimeout: The maximum age (in minutes) of a log line
            while being kept internally by PDI. Set to 0 to keep all rows
            indefinitely (default).
        """
        super().__init__(task_id=task_id, **kwargs)

        self.pdi_conn_id = pdi_conn_id or self.DEFAULT_CONN_ID
        self.dir = directory
        self.file = file
        self.job = job
        self.level = level
        self.logfile = logfile
        self.safemode = safemode
        self.task_params = params
        self.maxloglines = maxloglines
        self.maxlogtimeout = maxlogtimeout
        self.codes_map = self.STATUS_CODES

    def _get_pentaho_client(self):
        return PentahoHook(self.pdi_conn_id).get_conn()

    def execute(self, context):
        conn = self._get_pentaho_client()

        arguments = {
            'dir': self.dir,
            'job': self.job,
            'level': self.level,
            'logfile': self.logfile,
            'safemode': 'true' if self.safemode else 'false',
            'maxloglines': str(self.maxloglines),
            'maxlogtimeout': str(self.maxlogtimeout)
        }
        if self.file:
            arguments.update({'file': self.file})
            arguments.update({'norep': 'true'})

        self.command_line = conn.build_command('kitchen', arguments,
                                               self.task_params)
        output, err_count = self._run_command()

        self._push_err_count(context, err_count)
        return output
