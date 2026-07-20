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
"""Kettle hook module."""

from __future__ import annotations

import platform

from airflow.exceptions import AirflowException

try:  # Airflow 3
    from airflow.sdk import BaseHook
except ImportError:  # Airflow 2
    from airflow.hooks.base import BaseHook

from airflow_pentaho.hooks.carte import _param_value


class PentahoHook(BaseHook):
    """Hook to build Kettle (kitchen/pan) command lines."""

    conn_name_attr = 'conn_id'
    default_conn_name = 'pdi_default'

    class PentahoClient:
        """Builds command lines for kitchen/pan calls."""

        def __init__(
                self,
                pentaho_home,
                rep,
                username,
                password,
                system):
            self.pentaho_home = pentaho_home
            self.rep = rep
            self.username = username
            self.password = password
            self.system = system

        def _get_tool_command_template(self):
            if self.system == 'Windows':
                return '{}\\{}.bat'
            if self.system in ('Linux', 'Darwin'):
                return '{}/{}.sh'
            raise AirflowException(
                "Unsupported platform for airflow_pentaho: '{}'"
                .format(self.system))

        def _build_tool_command(self, command):
            return self._get_tool_command_template().format(
                self.pentaho_home, command)

        def _get_argument_template(self):
            if self.system == 'Windows':
                return '/{}:{}'
            if self.system in ('Linux', 'Darwin'):
                return '-{}={}'
            raise AirflowException(
                "Unsupported platform for airflow_pentaho: '{}'"
                .format(self.system))

        def _build_argument(self, key, val):
            return self._get_argument_template().format(key, val)

        def _build_connection_arguments(self):
            line = [
                self._build_argument('rep', self.rep),
                self._build_argument('user', self.username),
                self._build_argument('pass', self.password),
            ]
            return ' '.join(line)

        def build_command(self, command, arguments, params):
            line = [self._build_tool_command(command),
                    self._build_connection_arguments()]
            for k, val in arguments.items():
                line.append(self._build_argument(k, val))
            if params is not None:
                for k, val in params.items():
                    line.append(self._build_argument(
                        'param:{}'.format(k), _param_value(val)))
            return ' '.join(line)

    def __init__(self, conn_id=default_conn_name):
        super().__init__()
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson
        self.pentaho_cli = None

    def get_conn(self):
        """Provide the client required to build kitchen/pan commands."""
        if self.pentaho_cli:
            return self.pentaho_cli

        self.pentaho_cli = self.PentahoClient(
            self.extras.get('pentaho_home'),
            self.extras.get('rep'),
            self.connection.login,
            self.connection.password,
            platform.system())

        return self.pentaho_cli
