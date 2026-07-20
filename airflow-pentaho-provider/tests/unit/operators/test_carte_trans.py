# -*- coding: utf-8 -*-
"""Unit tests for CarteTransOperator."""

import base64
import gzip
from unittest import mock

import pytest

from airflow.exceptions import AirflowException, TaskDeferred

from airflow_pentaho.operators.carte import CarteTransOperator
from airflow_pentaho.triggers.carte import CarteTransTrigger


def _gzip_log(text):
    return base64.b64encode(gzip.compress(text.encode('utf-8'))).decode()


def _transstatus(status_desc, log_text='line one\nlast line',
                 error_desc=None, trans_id='t-456'):
    return {'transstatus': {
        'transname': 'test_trans',
        'id': trans_id,
        'status_desc': status_desc,
        'error_desc': error_desc,
        'logging_string': '<![CDATA[{}]]>'.format(_gzip_log(log_text)),
        'first_log_line_nr': 0,
        'last_log_line_nr': 10,
    }}


@pytest.fixture
def fake_client():
    client = mock.MagicMock()
    client.run_trans.return_value = None
    return client


@pytest.fixture
def patched_hook(fake_client):
    with mock.patch(
            'airflow_pentaho.operators.carte.PentahoCarteHook') as hook_cls:
        hook_cls.return_value.get_conn.return_value = fake_client
        yield hook_cls


@pytest.fixture
def context():
    return {'ti': mock.Mock()}


class TestCarteTransOperator:

    def test_execute_success(self, patched_hook, fake_client, context):
        fake_client.trans_status.side_effect = [
            _transstatus('Running'),
            _transstatus('Finished'),
        ]
        op = CarteTransOperator(task_id='t', trans='/home/bi/test_trans',
                                poll_interval=0)

        output = op.execute(context)

        assert output == 'last line'
        fake_client.run_trans.assert_called_once_with(
            '/home/bi/test_trans', None)
        # The second poll should reuse the id discovered in the first
        assert fake_client.trans_status.call_args_list[1][0][1] == 't-456'

    def test_execute_error_status_raises(self, patched_hook, fake_client,
                                         context):
        fake_client.trans_status.return_value = _transstatus(
            'Stopped (with errors)')
        op = CarteTransOperator(task_id='t', trans='/home/bi/test_trans',
                                poll_interval=0)

        with pytest.raises(AirflowException,
                           match=r'Stopped \(with errors\)'):
            op.execute(context)

    def test_deferrable_defers_with_trigger(self, patched_hook,
                                            fake_client, context):
        op = CarteTransOperator(task_id='t', trans='/home/bi/test_trans.ktr',
                                deferrable=True, poll_interval=15)

        with pytest.raises(TaskDeferred) as deferred:
            op.execute(context)

        trigger = deferred.value.trigger
        assert isinstance(trigger, CarteTransTrigger)
        assert trigger.trans_name == 'test_trans'
        assert trigger.poll_interval == 15
        assert deferred.value.method_name == 'execute_complete'

    def test_execute_complete_success(self, patched_hook, fake_client,
                                      context):
        fake_client.trans_status.return_value = _transstatus('Finished')
        op = CarteTransOperator(task_id='t', trans='/home/bi/test_trans')

        output = op.execute_complete(
            context, {'status': 'success', 'status_desc': 'Finished',
                      'trans_id': 't-456'})

        assert output == 'last line'
        fake_client.trans_status.assert_called_once_with(
            'test_trans', 't-456')

    def test_execute_complete_trigger_error(self, patched_hook, context):
        op = CarteTransOperator(task_id='t', trans='/home/bi/test_trans')

        with pytest.raises(AirflowException, match='conn refused'):
            op.execute_complete(
                context, {'status': 'error', 'message': 'conn refused'})

    def test_on_kill_stops_trans(self, patched_hook, fake_client, context):
        fake_client.trans_status.return_value = _transstatus('Finished')
        op = CarteTransOperator(task_id='t', trans='/home/bi/test_trans',
                                poll_interval=0)
        op.execute(context)

        op.on_kill()

        fake_client.stop_trans.assert_called_once_with('test_trans', 't-456')
