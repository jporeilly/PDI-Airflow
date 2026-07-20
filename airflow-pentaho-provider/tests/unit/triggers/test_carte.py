# -*- coding: utf-8 -*-
"""Unit tests for Carte triggers."""

import asyncio
from unittest import mock

import pytest

from airflow_pentaho.triggers.carte import CarteJobTrigger
from airflow_pentaho.triggers.carte import CarteTransTrigger


def _jobstatus(status_desc):
    return {'jobstatus': {
        'status_desc': status_desc,
        'last_log_line_nr': 5,
    }}


def _transstatus(status_desc, trans_id='t-456'):
    return {'transstatus': {
        'id': trans_id,
        'status_desc': status_desc,
        'last_log_line_nr': 5,
    }}


async def _first_event(trigger):
    async for event in trigger.run():
        return event
    return None


class TestCarteJobTrigger:

    def test_serialize_roundtrip(self):
        trigger = CarteJobTrigger(conn_id='pdi_default', level='Basic',
                                  job_name='test_job', job_id='abc-123',
                                  poll_interval=30)
        classpath, kwargs = trigger.serialize()
        assert classpath == 'airflow_pentaho.triggers.carte.CarteJobTrigger'
        rebuilt = CarteJobTrigger(**kwargs)
        assert rebuilt.job_name == 'test_job'
        assert rebuilt.job_id == 'abc-123'
        assert rebuilt.poll_interval == 30

    def test_yields_success_on_finished(self):
        trigger = CarteJobTrigger(conn_id='pdi_default', level='Basic',
                                  job_name='test_job', job_id='abc-123',
                                  poll_interval=0)
        client = mock.MagicMock()
        client.job_status.side_effect = [
            _jobstatus('Running'),
            _jobstatus('Finished'),
        ]
        with mock.patch.object(trigger, '_get_client', return_value=client):
            event = asyncio.run(_first_event(trigger))

        assert event.payload['status'] == 'success'
        assert event.payload['job_id'] == 'abc-123'
        assert client.job_status.call_count == 2

    def test_yields_error_on_error_status(self):
        trigger = CarteJobTrigger(conn_id='pdi_default', level='Basic',
                                  job_name='test_job', job_id='abc-123',
                                  poll_interval=0)
        client = mock.MagicMock()
        client.job_status.return_value = _jobstatus('Finished (with errors)')
        with mock.patch.object(trigger, '_get_client', return_value=client):
            event = asyncio.run(_first_event(trigger))

        assert event.payload['status'] == 'error'

    def test_yields_error_on_exception(self):
        trigger = CarteJobTrigger(conn_id='pdi_default', level='Basic',
                                  job_name='test_job', job_id='abc-123')
        with mock.patch.object(trigger, '_get_client',
                               side_effect=RuntimeError('boom')):
            event = asyncio.run(_first_event(trigger))

        assert event.payload['status'] == 'error'
        assert 'boom' in event.payload['message']


class TestCarteTransTrigger:

    def test_serialize_roundtrip(self):
        trigger = CarteTransTrigger(conn_id='pdi_default', level='Basic',
                                    trans_name='test_trans',
                                    poll_interval=15)
        classpath, kwargs = trigger.serialize()
        assert classpath == \
            'airflow_pentaho.triggers.carte.CarteTransTrigger'
        rebuilt = CarteTransTrigger(**kwargs)
        assert rebuilt.trans_name == 'test_trans'
        assert rebuilt.trans_id is None

    def test_yields_success_and_resolves_id(self):
        trigger = CarteTransTrigger(conn_id='pdi_default', level='Basic',
                                    trans_name='test_trans',
                                    poll_interval=0)
        client = mock.MagicMock()
        client.trans_status.side_effect = [
            _transstatus('Running'),
            _transstatus('Finished'),
        ]
        with mock.patch.object(trigger, '_get_client', return_value=client):
            event = asyncio.run(_first_event(trigger))

        assert event.payload['status'] == 'success'
        assert event.payload['trans_id'] == 't-456'
