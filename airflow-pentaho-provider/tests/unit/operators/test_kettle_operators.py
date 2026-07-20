# -*- coding: utf-8 -*-
"""Unit tests for KitchenOperator and PanOperator."""

from unittest import mock

import pytest

from airflow_pentaho.hooks.kettle import PentahoHook
from airflow_pentaho.operators.kettle import KitchenOperator, PanOperator


@pytest.fixture
def linux_client():
    return PentahoHook.PentahoClient(
        pentaho_home='/opt/pentaho',
        rep='Default',
        username='repo_user',
        password='repo_pass',
        system='Linux')


@pytest.fixture
def patched_hook(linux_client):
    with mock.patch(
            'airflow_pentaho.operators.kettle.PentahoHook') as hook_cls:
        hook_cls.return_value.get_conn.return_value = linux_client
        yield hook_cls


@pytest.fixture
def context():
    return {'ti': mock.Mock()}


class TestKitchenOperator:

    def test_command_line_built(self, patched_hook, context):
        op = KitchenOperator(
            task_id='t',
            directory='/home/bi',
            job='nightly_job',
            params={'date': '2026-07-18'})

        with mock.patch.object(op, '_run_command',
                               return_value=('done', 0)) as run:
            output = op.execute(context)

        assert output == 'done'
        run.assert_called_once()
        cmd = op.command_line
        assert cmd.startswith('/opt/pentaho/kitchen.sh ')
        assert '-dir=/home/bi' in cmd
        assert '-job=nightly_job' in cmd
        assert '-param:date=2026-07-18' in cmd
        context['ti'].xcom_push.assert_called_with(key='err_count', value=0)

    def test_local_file_adds_norep(self, patched_hook, context):
        op = KitchenOperator(
            task_id='t',
            file='/opt/etl/local_job.kjb')

        with mock.patch.object(op, '_run_command', return_value=('', 0)):
            op.execute(context)

        assert '-file=/opt/etl/local_job.kjb' in op.command_line
        assert '-norep=true' in op.command_line

    def test_hide_sensitive_data(self):
        cleaned = KitchenOperator._hide_sensitive_data(
            '/opt/pentaho/kitchen.sh -rep=Default -user=u -pass=secret '
            '-job=j')
        assert 'secret' not in cleaned


class TestPanOperator:

    def test_command_line_built(self, patched_hook, context):
        op = PanOperator(
            task_id='t',
            directory='/home/bi',
            trans='clean_data',
            safemode=True,
            params={'file': '/tmp/in.csv'})

        with mock.patch.object(op, '_run_command',
                               return_value=('ok', 2)) as run:
            output = op.execute(context)

        assert output == 'ok'
        run.assert_called_once()
        cmd = op.command_line
        assert cmd.startswith('/opt/pentaho/pan.sh ')
        assert '-trans=clean_data' in cmd
        assert '-safemode=true' in cmd
        assert '-param:file=/tmp/in.csv' in cmd
        context['ti'].xcom_push.assert_called_with(key='err_count', value=2)

    def test_xcom_push_arg_deprecated(self, patched_hook):
        with pytest.warns(DeprecationWarning):
            PanOperator(task_id='t', trans='x', xcom_push=True)
