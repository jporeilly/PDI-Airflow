# -*- coding: utf-8 -*-
"""Unit tests for the Kettle hook."""

import pytest

from airflow.exceptions import AirflowException

from airflow_pentaho.hooks.kettle import PentahoHook


def _client(system):
    return PentahoHook.PentahoClient(
        pentaho_home='/opt/pentaho',
        rep='Default',
        username='repo_user',
        password='repo_pass',
        system=system)


class TestPentahoClient:

    def test_linux_command(self):
        client = _client('Linux')
        cmd = client.build_command(
            'pan',
            {'dir': '/home/bi', 'trans': 'clean'},
            {'date': '2026-07-18'})
        assert cmd.startswith('/opt/pentaho/pan.sh ')
        assert '-rep=Default' in cmd
        assert '-user=repo_user' in cmd
        assert '-pass=repo_pass' in cmd
        assert '-dir=/home/bi' in cmd
        assert '-trans=clean' in cmd
        assert '-param:date=2026-07-18' in cmd

    def test_windows_command(self):
        client = _client('Windows')
        cmd = client.build_command(
            'kitchen',
            {'dir': '/home/bi', 'job': 'nightly'},
            {'date': '2026-07-18'})
        assert cmd.startswith('/opt/pentaho\\kitchen.bat ')
        assert '/rep:Default' in cmd
        assert '/job:nightly' in cmd
        assert '/param:date:2026-07-18' in cmd

    def test_unsupported_platform_raises(self):
        client = _client('Solaris')
        with pytest.raises(AirflowException, match='Unsupported platform'):
            client.build_command('pan', {}, None)

    def test_param_object_unwrapped(self):
        class FakeParam:
            value = 'v1'

        client = _client('Linux')
        cmd = client.build_command('pan', {}, {'p': FakeParam()})
        assert '-param:p=v1' in cmd


class TestPentahoHook:

    def test_get_conn(self, mock_kettle_get_connection):
        hook = PentahoHook('pdi_default')
        client = hook.get_conn()
        assert client.pentaho_home == '/opt/pentaho'
        assert client.rep == 'Default'
        assert client.username == 'repo_user'
        # get_conn caches the client
        assert hook.get_conn() is client
