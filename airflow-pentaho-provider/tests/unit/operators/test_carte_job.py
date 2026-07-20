# -*- coding: utf-8 -*-
"""Unit tests for CarteJobOperator."""

import base64
import gzip
from unittest import mock

import pytest

from airflow.exceptions import AirflowException, TaskDeferred

from airflow_pentaho.operators.carte import CarteJobOperator
from airflow_pentaho.triggers.carte import CarteJobTrigger


def _gzip_log(text):
    return base64.b64encode(gzip.compress(text.encode('utf-8'))).decode()


def _webresult(job_id='abc-123'):
    return {'webresult': {'result': 'OK', 'message': 'Job started',
                          'id': job_id}}


def _jobstatus(status_desc, log_text='line one\nlast line',
               error_desc=None, last_line=10):
    return {'jobstatus': {
        'jobname': 'test_job',
        'id': 'abc-123',
        'status_desc': status_desc,
        'error_desc': error_desc,
        'logging_string': '<![CDATA[{}]]>'.format(_gzip_log(log_text)),
        'first_log_line_nr': 0,
        'last_log_line_nr': last_line,
    }}


@pytest.fixture
def fake_client():
    client = mock.MagicMock()
    client.run_job.return_value = _webresult()
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


class TestCarteJobOperator:

    def test_execute_success(self, patched_hook, fake_client, context):
        fake_client.job_status.side_effect = [
            _jobstatus('Running'),
            _jobstatus('Finished'),
        ]
        op = CarteJobOperator(task_id='t', job='/home/bi/test_job',
                              poll_interval=0)

        output = op.execute(context)

        assert output == 'last line'
        fake_client.run_job.assert_called_once_with('/home/bi/test_job',
                                                    None)
        assert fake_client.job_status.call_count == 2
        context['ti'].xcom_push.assert_called_with(key='err_count', value=0)

    def test_execute_error_status_raises(self, patched_hook, fake_client,
                                         context):
        fake_client.job_status.return_value = _jobstatus(
            'Finished (with errors)')
        op = CarteJobOperator(task_id='t', job='/home/bi/test_job',
                              poll_interval=0)

        with pytest.raises(AirflowException,
                           match=r'Finished \(with errors\)'):
            op.execute(context)

    def test_execute_error_desc_raises(self, patched_hook, fake_client,
                                       context):
        fake_client.job_status.return_value = _jobstatus(
            'Finished', error_desc='Something broke')
        op = CarteJobOperator(task_id='t', job='/home/bi/test_job',
                              poll_interval=0)

        with pytest.raises(AirflowException, match='Something broke'):
            op.execute(context)

    def test_unexpected_response_raises(self, patched_hook, fake_client,
                                        context):
        fake_client.job_status.return_value = {'unexpected': {}}
        op = CarteJobOperator(task_id='t', job='/home/bi/test_job',
                              poll_interval=0)

        with pytest.raises(AirflowException, match='Unexpected server'):
            op.execute(context)

    def test_error_lines_counted(self, patched_hook, fake_client, context):
        fake_client.job_status.return_value = _jobstatus(
            'Finished', log_text='ok line\nERROR: kaboom\ndone')
        op = CarteJobOperator(task_id='t', job='/home/bi/test_job',
                              poll_interval=0)

        op.execute(context)

        context['ti'].xcom_push.assert_called_with(key='err_count', value=1)

    def test_deferrable_defers_with_trigger(self, patched_hook,
                                            fake_client, context):
        op = CarteJobOperator(task_id='t', job='/home/bi/test_job.kjb',
                              deferrable=True, poll_interval=30)

        with pytest.raises(TaskDeferred) as deferred:
            op.execute(context)

        trigger = deferred.value.trigger
        assert isinstance(trigger, CarteJobTrigger)
        assert trigger.job_name == 'test_job'
        assert trigger.job_id == 'abc-123'
        assert trigger.poll_interval == 30
        assert deferred.value.method_name == 'execute_complete'

    def test_execute_complete_success(self, patched_hook, fake_client,
                                      context):
        fake_client.job_status.return_value = _jobstatus('Finished')
        op = CarteJobOperator(task_id='t', job='/home/bi/test_job')

        output = op.execute_complete(
            context, {'status': 'success', 'status_desc': 'Finished',
                      'job_id': 'abc-123'})

        assert output == 'last line'
        fake_client.job_status.assert_called_once_with('test_job', 'abc-123')

    def test_execute_complete_trigger_error(self, patched_hook, context):
        op = CarteJobOperator(task_id='t', job='/home/bi/test_job')

        with pytest.raises(AirflowException, match='poll failed'):
            op.execute_complete(
                context, {'status': 'error', 'message': 'poll failed'})

    def test_execute_complete_error_status(self, patched_hook, fake_client,
                                           context):
        fake_client.job_status.return_value = _jobstatus('Stopped')
        op = CarteJobOperator(task_id='t', job='/home/bi/test_job')

        with pytest.raises(AirflowException, match='Stopped'):
            op.execute_complete(
                context, {'status': 'error', 'status_desc': 'Stopped',
                          'job_id': 'abc-123'})

    def test_on_kill_stops_job(self, patched_hook, fake_client, context):
        fake_client.job_status.return_value = _jobstatus('Finished')
        op = CarteJobOperator(task_id='t', job='/home/bi/test_job',
                              poll_interval=0)
        op.execute(context)

        op.on_kill()

        fake_client.stop_job.assert_called_once_with('test_job', 'abc-123')

    def test_xcom_push_arg_deprecated(self, patched_hook):
        with pytest.warns(DeprecationWarning):
            CarteJobOperator(task_id='t', job='/j', xcom_push=True)
